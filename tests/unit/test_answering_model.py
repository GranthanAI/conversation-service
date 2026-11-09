"""P6e: the answering model is stored and exposed, duplicate terminal events (even with a NEW event_id) are ignored, and
usage thresholds reach an open SSE stream as `notice` events."""

import asyncio
import json
import re
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.events.consumers import (
    _derive_event_id, _handle_chat_response_cancelled, _handle_chat_response_generated, _handle_usage_threshold,
)
from app.models.message import Message, MessageStatus
from app.repositories.message_repository import CassandraMessageRepository
from app.schemas.message import MessageResponse
from app.services.message_service import MessageService
from app.services.stream_service import StreamService

pytestmark = pytest.mark.anyio

CONV = uuid.UUID(int=11)
REQ = "0194a1b2-0000-7000-8000-0000000000b1"
RESP = "0194a1b2-0000-7000-8000-0000000000b2"


class FakeInbox:
    def __init__(self):
        self.seen = set()

    def exists(self, eid):
        return eid in self.seen

    def save(self, eid):
        self.seen.add(eid)


class FakeRepo:
    """In-memory messages_by_conversation: just enough for the consumers + MessageService."""

    def __init__(self):
        self.rows = {}
        self.direct_calls = []

    def get_message(self, conversation_id, message_id):
        return self.rows.get((conversation_id, message_id))

    def create_message_direct(self, conversation_id, message_id, sender, content, status, citations=None,
                              model=None, model_group=None):
        self.direct_calls.append(dict(message_id=message_id, content=content, status=status, model=model,
                                      model_group=model_group))
        msg = Message(conversation_id=conversation_id, message_id=message_id, sender=sender, content=content,
                      created_at=datetime.now(timezone.utc), status=MessageStatus(status), citations=citations or [],
                      model=model, model_group=model_group)
        self.rows[(conversation_id, message_id)] = msg
        return msg

    def set_answering_model(self, conversation_id, message_id, model, model_group):
        row = self.rows.get((conversation_id, message_id))
        if row is None or row.sender != "assistant" or not (model or model_group):
            return False
        row.model, row.model_group = model or row.model, model_group or row.model_group
        return True

    def add_pending(self):
        self.rows[(CONV, uuid.UUID(RESP))] = Message(conversation_id=CONV, message_id=uuid.UUID(RESP), sender="assistant",
                                                     content="", created_at=datetime.now(timezone.utc),
                                                     status=MessageStatus.PENDING)


def deps():
    repo, inbox = FakeRepo(), FakeInbox()
    repo.add_pending()
    stream = MagicMock()
    stream.publish_token = AsyncMock()
    return repo, inbox, MessageService(repo=repo), stream


def generated(**over):
    event = {"event_type": "chat.response.generated", "schema_version": "2.2", "event_id": str(uuid.uuid4()),
             "response_id": "r1", "conversation_id": str(CONV), "user_id": str(uuid.UUID(int=3)),
             "request_message_id": REQ, "response_message_id": RESP, "full_content": "Hello world", "status": "success"}
    event.update(over)
    return event


# ---- stored and exposed ----------------------------------------------------------------------------------------

async def test_model_and_group_are_stored_and_in_the_final_sse_event():
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_generated(generated(model="claude-sonnet-5-5", model_group="best"), inbox, svc, stream)
    row = repo.rows[(CONV, uuid.UUID(RESP))]
    assert (row.model, row.model_group, row.content, row.status) == ("claude-sonnet-5-5", "best", "Hello world", MessageStatus.SENT)
    final = stream.publish_token.call_args.args[1]
    assert final["status"] == "completed" and final["model"] == "claude-sonnet-5-5" and final["model_group"] == "best"


async def test_history_response_exposes_model_and_group():
    msg = Message(conversation_id=CONV, message_id=uuid.UUID(RESP), sender="assistant", content="x",
                  created_at=datetime.now(timezone.utc), status=MessageStatus.SENT, model="gpt-6-luna", model_group="smart")
    out = MessageResponse.model_validate(msg)
    assert (out.model, out.model_group) == ("gpt-6-luna", "smart")
    assert MessageResponse.model_validate(Message(conversation_id=CONV, message_id=uuid.UUID(RESP), sender="user", content="q",
                                                  created_at=datetime.now(timezone.utc))).model is None


async def test_old_2_1_events_without_model_fields_still_work():
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_generated(generated(schema_version="2.1"), inbox, svc, stream)
    row = repo.rows[(CONV, uuid.UUID(RESP))]
    assert row.content == "Hello world" and row.model is None and row.model_group is None
    assert repo.direct_calls[0]["model"] is None
    final = stream.publish_token.call_args.args[1]
    assert "model" not in final and "model_group" not in final


# ---- dedupe on request_message_id ---------------------------------------------------------------------------------

async def test_duplicate_terminal_event_with_a_new_event_id_is_ignored():
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_generated(generated(model="m1", model_group="fast"), inbox, svc, stream)
    # llm-service restarted and delivered the same request again: new event_id, new response_id, different text
    again = generated(response_id="r2", full_content="A different answer", model="m2", model_group="smart")
    await _handle_chat_response_generated(again, inbox, svc, stream)
    assert len(repo.direct_calls) == 1 and stream.publish_token.await_count == 1
    row = repo.rows[(CONV, uuid.UUID(RESP))]
    assert row.content == "Hello world" and row.model == "m1"       # the completed answer was not overwritten


async def test_duplicate_with_a_different_response_message_id_creates_no_second_message():
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_generated(generated(), inbox, svc, stream)
    other = "0194a1b2-0000-7000-8000-0000000000b9"
    await _handle_chat_response_generated(generated(response_message_id=other), inbox, svc, stream)
    assert len(repo.rows) == 1 and (CONV, uuid.UUID(other)) not in repo.rows


async def test_duplicate_is_ignored_even_when_the_inbox_has_forgotten_it():
    """Inbox rows expire (7 days) or may be missing: the stored completed answer itself is the last line of defence."""
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_generated(generated(model="m1"), inbox, svc, stream)
    inbox.seen.clear()
    await _handle_chat_response_generated(generated(full_content="overwrite attempt", model="m2"), inbox, svc, stream)
    assert len(repo.direct_calls) == 1 and repo.rows[(CONV, uuid.UUID(RESP))].content == "Hello world"
    assert stream.publish_token.await_count == 1


async def test_late_error_event_never_overwrites_a_completed_answer():
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_generated(generated(), inbox, svc, stream)
    await _handle_chat_response_generated(generated(status="error", full_content=None, error_message="boom"), inbox, svc, stream)
    assert repo.rows[(CONV, uuid.UUID(RESP))].status == MessageStatus.SENT
    assert [c["status"] for c in repo.direct_calls] == ["sent"]


# ---- cancelled ---------------------------------------------------------------------------------------------------

def cancelled(**over):
    event = {"event_type": "chat.response.cancelled", "schema_version": "2.2", "conversation_id": str(CONV),
             "user_id": str(uuid.UUID(int=3)), "message_id": REQ, "response_message_id": RESP,
             "model": "gpt-6-luna", "model_group": "smart", "hold_id": "h", "calls": []}
    event.update(over)
    return event


async def test_cancelled_records_the_model_without_touching_content_or_status():
    repo, inbox, svc, _ = deps()
    await _handle_chat_response_cancelled(cancelled(), inbox, svc)
    row = repo.rows[(CONV, uuid.UUID(RESP))]
    assert (row.model, row.model_group, row.content, row.status) == ("gpt-6-luna", "smart", "", MessageStatus.PENDING)
    row.model = None
    await _handle_chat_response_cancelled(cancelled(), inbox, svc)      # duplicate (inbox): ignored
    assert row.model is None


async def test_cancelled_without_a_row_or_old_schema_writes_nothing():
    repo, inbox, svc, _ = deps()
    repo.rows.clear()
    await _handle_chat_response_cancelled(cancelled(), inbox, svc)
    await _handle_chat_response_cancelled(cancelled(model=None, model_group=None, schema_version="2.1"), FakeInbox(), svc)
    assert repo.rows == {}


async def test_cancelled_does_not_block_a_later_real_answer():
    repo, inbox, svc, stream = deps()
    await _handle_chat_response_cancelled(cancelled(), inbox, svc)
    await _handle_chat_response_generated(generated(model="gpt-6-luna", model_group="smart"), inbox, svc, stream)
    assert repo.rows[(CONV, uuid.UUID(RESP))].status == MessageStatus.SENT


# ---- repository CQL (statements are built correctly) ---------------------------------------------------------------

class CaptureSession:
    def __init__(self):
        self.prepared, self.executed = [], []

    def prepare(self, cql):
        self.prepared.append(" ".join(cql.split()))
        return self.prepared[-1]

    def execute(self, stmt, params=()):
        self.executed.append((stmt, params))
        return SimpleNamespace(one=lambda: None)


def test_repository_writes_model_columns_only_when_given():
    repo, session = CassandraMessageRepository(), CaptureSession()
    repo.manager = SimpleNamespace(session=session)
    repo.create_message_direct(CONV, uuid.uuid4(), "assistant", "hi", "sent")
    repo.create_message_direct(CONV, uuid.uuid4(), "assistant", "hi", "sent", model="m", model_group="g")
    plain, with_model = session.prepared
    assert "model" not in plain and "citations" not in plain          # unchanged shape for old events
    assert "model, model_group) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" in with_model


# ---- notices over SSE (real Redis pub/sub, a random user id = a private channel) ---------------------------------

async def _redis():
    import redis.asyncio as aioredis
    r = aioredis.from_url("redis://127.0.0.1:6379/0", decode_responses=True, socket_connect_timeout=2)
    try:
        await r.ping()
    except Exception:
        pytest.skip("shared Redis not reachable")
    return r


def threshold(user_id, **over):
    body = {"user_id": str(user_id), "billing_account_id": str(uuid.uuid4()), "limit": "messages_per_day", "percent": 80,
            "used": 20, "allowed": 25, "resets_at": "2026-11-10T18:30:00+00:00"}
    body.update(over)
    return body


async def _open(stream, conv, user, **kw):
    gen = stream.subscribe(conv, user_id=user, **kw)
    assert (await gen.__anext__()).startswith(": ok")
    return gen


async def _next_frame(gen, timeout=5):
    async def pull():
        while True:
            frame = await gen.__anext__()
            if not frame.startswith(": heartbeat"):
                return frame
    return await asyncio.wait_for(pull(), timeout)


async def test_notice_reaches_an_open_stream():
    r = await _redis()
    stream, user, conv = StreamService(redis_client=r), uuid.uuid4(), uuid.uuid4()
    gen = await _open(stream, conv, user)
    try:
        inbox = FakeInbox()
        await _handle_usage_threshold(threshold(user), inbox, stream)
        frame = await _next_frame(gen)
        assert frame.startswith("event: notice\n")
        data = json.loads(re.search(r"data: (.*)\n", frame).group(1))
        assert data == {"notice_key": "usage.threshold_80", "metric": "messages_per_day", "used": 20, "limit": 25,
                        "resets_at": "2026-11-10T18:30:00+00:00"}
        # the same event again is a duplicate (inbox): nothing more is pushed
        await _handle_usage_threshold(threshold(user), inbox, stream)
        with pytest.raises(asyncio.TimeoutError):
            await _next_frame(gen, timeout=1.5)
    finally:
        await gen.aclose()
        await r.aclose()


async def test_notice_reaches_a_turn_scoped_stream_and_only_the_right_user():
    r = await _redis()
    stream, user, other, conv = StreamService(redis_client=r), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    mine = await _open(stream, conv, user, message_id=str(uuid.uuid4()))
    theirs = await _open(stream, uuid.uuid4(), other)
    try:
        await _handle_usage_threshold(threshold(user, percent=100, used=25), FakeInbox(), stream)
        frame = await _next_frame(mine)
        assert frame.startswith("event: notice\n") and "usage.threshold_100" in frame
        with pytest.raises(asyncio.TimeoutError):
            await _next_frame(theirs, timeout=1.5)
    finally:
        await mine.aclose()
        await theirs.aclose()
        await r.aclose()


async def test_notice_with_no_open_stream_is_dropped():
    r = await _redis()
    stream = StreamService(redis_client=r)
    inbox = FakeInbox()
    await _handle_usage_threshold(threshold(uuid.uuid4()), inbox, stream)      # no exception, nothing stored
    assert len(inbox.seen) == 1
    await r.aclose()


async def test_threshold_event_with_a_bad_user_id_is_skipped():
    stream = MagicMock()
    stream.publish_user_notice = AsyncMock()
    await _handle_usage_threshold({"user_id": "nope"}, FakeInbox(), stream)
    stream.publish_user_notice.assert_not_awaited()


# ---- real Cassandra, throwaway keyspace -------------------------------------------------------------------------

def _schema_statements():
    import pathlib
    text = (pathlib.Path(__file__).resolve().parents[2] / "app" / "db" / "schema.cql").read_text(encoding="utf-8")
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    stmts = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    return [s for s in stmts if "messages_by_conversation" in s and (s.startswith("CREATE TABLE") or s.startswith("ALTER TABLE"))]


def test_cassandra_round_trip_in_a_throwaway_keyspace():
    from cassandra.cluster import Cluster
    from cassandra.io.asyncioreactor import AsyncioConnection   # noqa: F401 (the app's reactor)
    ks = f"p6e_{uuid.uuid4().hex[:10]}"
    try:
        cluster = Cluster(["127.0.0.1"], port=9042, connect_timeout=3)
        session = cluster.connect()
    except Exception as e:
        pytest.skip(f"shared Cassandra not reachable: {type(e).__name__}")
    try:
        session.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        session.set_keyspace(ks)
        for stmt in _schema_statements():
            session.execute(stmt)
        for stmt in _schema_statements():           # re-running the schema is idempotent
            session.execute(stmt)
        repo = CassandraMessageRepository()
        repo.manager = SimpleNamespace(session=session)
        conv, mid, old = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        repo.create_message_direct(conv, old, "assistant", "old answer", "sent")                      # no model: stays NULL
        repo.create_message_direct(conv, mid, "assistant", "new answer", "sent", model="claude-sonnet-5-5", model_group="best")
        hist = {m.message_id: m for m in repo.history(conv, 10)}
        assert (hist[mid].model, hist[mid].model_group) == ("claude-sonnet-5-5", "best")
        assert (hist[old].model, hist[old].model_group) == (None, None)
        got = repo.get_message(conv, mid)
        assert got.model == "claude-sonnet-5-5" and got.content == "new answer"
        # cancelled: model recorded on an existing assistant row only, content untouched
        pend = uuid.uuid4()
        repo.create_message_direct(conv, pend, "assistant", "", "pending")
        assert repo.set_answering_model(conv, pend, "gpt-6-luna", "smart") is True
        after = repo.get_message(conv, pend)
        assert (after.model, after.model_group, after.content, after.status.value) == ("gpt-6-luna", "smart", "", "pending")
        assert repo.set_answering_model(conv, uuid.uuid4(), "x", "y") is False       # no ghost row
        assert [m.model for m in repo.history_upto(conv, mid, 10) if m.message_id == mid] == ["claude-sonnet-5-5"]
    finally:
        try:
            session.execute(f"DROP KEYSPACE IF EXISTS {ks}")
        finally:
            cluster.shutdown()
