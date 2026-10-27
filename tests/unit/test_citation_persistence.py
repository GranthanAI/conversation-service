"""Citations of an assistant answer are validated, persisted with the final message, and returned by history.

Deterministic unit tests (no Cassandra/LLM): the repository runs against a recording fake session.
"""

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.events.consumers import _handle_chat_response_generated
from app.repositories.message_repository import CassandraMessageRepository
from app.schemas.message import MessageResponse
from app.services.citations import MAX_CITATIONS, dumps_citations, loads_citations, sanitize_citations

pytestmark = pytest.mark.anyio

DOC = {"file_id": "11111111-1111-1111-1111-111111111111", "source_name": "spec.pdf", "page_number": 3, "section": "Intro"}
VIDEO = {"file_id": "22222222-2222-2222-2222-222222222222", "source_name": "YouTube: Talk", "page_number": None,
         "section": "t=75-110", "start_seconds": 75, "end_seconds": 110}


# ---- validation -----------------------------------------------------------------------

def test_valid_document_and_video_citations_round_trip_in_order():
    clean = sanitize_citations([DOC, VIDEO])
    assert clean == [DOC, VIDEO]
    assert loads_citations(dumps_citations(clean)) == clean


def test_serialization_is_deterministic():
    shuffled = dict(reversed(list(VIDEO.items())))
    assert dumps_citations(sanitize_citations([shuffled])) == dumps_citations(sanitize_citations([VIDEO]))


def test_unknown_keys_and_urls_are_dropped():
    c = sanitize_citations([{**DOC, "url": "javascript:alert(1)", "excerpt": "secret"}])[0]
    assert "url" not in c and "excerpt" not in c


@pytest.mark.parametrize("bad", [None, "x", 5, {}, {"file_id": ""}, {"file_id": "a b"}, {"file_id": "../x"},
                                 {"file_id": 7}, ["nested"]])
def test_malformed_entries_are_dropped(bad):
    assert sanitize_citations([bad, DOC]) == [DOC]


def test_non_list_is_empty():
    assert sanitize_citations(None) == [] and sanitize_citations({"a": 1}) == []


def test_invalid_time_range_does_not_fabricate_an_end():
    c = sanitize_citations([{**VIDEO, "start_seconds": 90, "end_seconds": 10}])[0]
    assert c["start_seconds"] == 90 and c["end_seconds"] is None
    assert "start_seconds" not in sanitize_citations([{**VIDEO, "start_seconds": -1}])[0]
    assert "start_seconds" not in sanitize_citations([{**VIDEO, "start_seconds": True}])[0]


def test_count_size_and_duplicates_are_bounded():
    many = [{**DOC, "file_id": f"f{i}"} for i in range(MAX_CITATIONS + 20)]
    assert len(sanitize_citations(many)) == MAX_CITATIONS
    assert sanitize_citations([DOC, DOC, DOC]) == [DOC]
    big = [{**DOC, "file_id": f"f{i}", "source_name": "x" * 300, "section": "y" * 300} for i in range(MAX_CITATIONS)]
    assert len(dumps_citations(sanitize_citations(big)).encode()) <= 32_000


@pytest.mark.parametrize("stored", [None, "", "not json", "{}", "123", '[{"file_id": "bad id"}]'])
def test_historical_or_corrupt_column_reads_as_no_citations(stored):
    assert loads_citations(stored) == []


# ---- repository: written with the answer, returned by history ---------------------------

class FakeSession:
    def __init__(self, rows=()):
        self.calls, self.rows = [], list(rows)

    def prepare(self, cql):
        return cql

    def execute(self, stmt, params=()):
        self.calls.append((stmt, params))
        return self.rows


def _repo(rows=()):
    repo = CassandraMessageRepository()
    repo.manager = SimpleNamespace(session=FakeSession(rows))
    return repo


def test_create_message_direct_writes_citations_in_the_same_insert_as_content():
    repo = _repo()
    cid, mid = uuid.uuid4(), uuid.uuid4()
    msg = repo.create_message_direct(cid, mid, "assistant", "answer [1]", "sent", citations=[DOC, VIDEO, {"bad": 1}])
    stmt, params = repo.manager.session.calls[0]
    assert "citations" in stmt and "content" in stmt          # one statement: answer + sources are atomic
    assert params[3] == "answer [1]" and json.loads(params[6]) == [DOC, VIDEO]
    assert msg.citations == [DOC, VIDEO]


def test_create_message_direct_without_citations_does_not_touch_the_column():
    repo = _repo()
    repo.create_message_direct(uuid.uuid4(), uuid.uuid4(), "assistant", "hi", "sent")
    stmt, params = repo.manager.session.calls[0]
    assert "citations" not in stmt and len(params) == 6      # no tombstone; works before the ALTER


def _row(citations):
    return SimpleNamespace(message_id=uuid.uuid4(), sender="assistant", content="a", created_at=datetime.now(timezone.utc),
                           status="sent", file_ids=None, citations=citations)


def test_history_returns_persisted_citations_and_tolerates_old_rows():
    new, old, corrupt = _row(dumps_citations([DOC, VIDEO])), _row(None), _row("{oops")
    msgs = _repo([new, old, corrupt]).history(uuid.uuid4())
    assert [m.citations for m in msgs] == [[DOC, VIDEO], [], []]


def test_get_message_and_history_upto_return_citations():
    row = _row(dumps_citations([VIDEO]))
    assert _repo(rows=[row]).history_upto(uuid.uuid4(), row.message_id)[0].citations == [VIDEO]
    one = _repo()
    one.manager.session.execute = lambda stmt, params=(): SimpleNamespace(one=lambda: row)
    assert one.get_message(uuid.uuid4(), row.message_id).citations == [VIDEO]


def test_api_schema_serializes_citations_and_defaults_to_empty():
    base = dict(message_id=uuid.uuid4(), conversation_id=uuid.uuid4(), sender="assistant", content="a",
                created_at=datetime.now(timezone.utc), status="sent")
    assert MessageResponse(**base).citations == []
    out = MessageResponse(**base, citations=[DOC, VIDEO]).model_dump(mode="json")["citations"]
    assert out[1]["start_seconds"] == 75 and out[0]["page_number"] == 3


# ---- consumer: persisted AND streamed, sanitized the same way ---------------------------

def _deps():
    inbox = MagicMock()
    inbox.exists.return_value = False
    stream = MagicMock()
    stream.publish_token = AsyncMock()
    msgs = MagicMock()
    msgs.finalize_assistant_message = AsyncMock(return_value=object())
    msgs.clear_pending_response = AsyncMock()
    return inbox, stream, msgs


def _event(citations):
    return {"response_message_id": str(uuid.uuid4()), "request_message_id": str(uuid.uuid4()),
            "conversation_id": str(uuid.uuid4()), "status": "success", "full_content": "answer", "citations": citations}


async def test_final_event_persists_validated_citations_and_streams_the_same_list():
    inbox, stream, msgs = _deps()
    await _handle_chat_response_generated(_event([DOC, VIDEO, "junk"]), inbox, msgs, stream)
    assert msgs.finalize_assistant_message.call_args.kwargs["citations"] == [DOC, VIDEO]
    assert stream.publish_token.call_args.args[1]["citations"] == [DOC, VIDEO]


async def test_final_event_without_citations_keeps_the_old_call_shape():
    inbox, stream, msgs = _deps()
    await _handle_chat_response_generated(_event([]), inbox, msgs, stream)
    assert "citations" not in msgs.finalize_assistant_message.call_args.kwargs
    assert stream.publish_token.call_args.args[1]["citations"] == []


async def test_redelivered_final_event_is_deduplicated_and_does_not_rewrite():
    inbox, stream, msgs = _deps()
    inbox.exists.return_value = True
    await _handle_chat_response_generated(_event([DOC]), inbox, msgs, stream)
    msgs.finalize_assistant_message.assert_not_called()
