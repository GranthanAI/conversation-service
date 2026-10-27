"""Real-Cassandra check of the additive `citations` column, in a disposable keyspace (never the app's keyspace).

Simulates the rollout: the table exists WITHOUT the column and holds an old row; `ALTER TABLE ... ADD citations text`
is applied; old rows must read as "no citations" and new answers must round-trip through the real repository.
Skipped when no Cassandra is reachable on CASSANDRA_CONTACT_POINTS:CASSANDRA_PORT.
"""

import uuid
from types import SimpleNamespace

import pytest

from app.core.config import settings
from app.repositories.message_repository import CassandraMessageRepository

KEYSPACE = f"citations_mig_{uuid.uuid4().hex[:8]}"
DOC = {"file_id": "11111111-1111-1111-1111-111111111111", "source_name": "spec.pdf", "page_number": 3, "section": "Intro"}
VIDEO = {"file_id": "22222222-2222-2222-2222-222222222222", "source_name": "Talk", "page_number": None,
         "section": "t=75-110", "start_seconds": 75, "end_seconds": 110}

OLD_TABLE = """
CREATE TABLE messages_by_conversation (
    conversation_id uuid, message_id uuid, sender text, content text, created_at timestamp, status text,
    file_ids list<uuid>, PRIMARY KEY (conversation_id, message_id)
) WITH CLUSTERING ORDER BY (message_id DESC)
"""


@pytest.fixture(scope="module")
def session():
    cassandra = pytest.importorskip("cassandra.cluster")
    try:
        cluster = cassandra.Cluster([settings.CASSANDRA_CONTACT_POINTS], port=settings.CASSANDRA_PORT, connect_timeout=3)
        sess = cluster.connect()
    except Exception as exc:  # no database available in this environment
        pytest.skip(f"Cassandra not reachable: {exc}")
    sess.execute(f"CREATE KEYSPACE {KEYSPACE} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}")
    sess.set_keyspace(KEYSPACE)
    sess.execute(OLD_TABLE)
    yield sess
    sess.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
    cluster.shutdown()


def test_old_rows_survive_the_alter_and_new_answers_round_trip(session):
    conv, old_id = uuid.uuid4(), uuid.uuid1()
    session.execute(
        "INSERT INTO messages_by_conversation (conversation_id, message_id, sender, content, created_at, status) "
        "VALUES (%s, %s, 'assistant', 'old answer', toTimestamp(now()), 'sent')", (conv, old_id))

    session.execute("ALTER TABLE messages_by_conversation ADD citations text")      # the documented rollout step

    repo = CassandraMessageRepository()
    repo.manager = SimpleNamespace(session=session)

    # historical message: reads as no citations
    assert repo.history(conv)[0].citations == []

    # new answer: content + citations written together and returned by every read path
    new_id = uuid.uuid4()
    repo.create_message_direct(conv, new_id, "assistant", "answer [1][2]", "sent", citations=[DOC, VIDEO, {"junk": 1}])
    by_id = {m.message_id: m for m in repo.history(conv)}
    assert by_id[new_id].citations == [DOC, VIDEO]
    assert by_id[new_id].content == "answer [1][2]"
    assert by_id[old_id].citations == []
    assert repo.get_message(conv, new_id).citations == [DOC, VIDEO]
    assert repo.history_upto(conv, new_id)[0].citations == [DOC, VIDEO]

    # redelivered finalization with the same data is idempotent (same primary key, same value)
    repo.create_message_direct(conv, new_id, "assistant", "answer [1][2]", "sent", citations=[DOC, VIDEO])
    assert repo.get_message(conv, new_id).citations == [DOC, VIDEO]

    # a later write without citations (e.g. a failure marker) must not clobber stored sources
    repo.create_message_direct(conv, new_id, "assistant", "answer [1][2]", "sent")
    assert repo.get_message(conv, new_id).citations == [DOC, VIDEO]
