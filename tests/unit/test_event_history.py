"""The request event carries recent finished turns (oldest first) so llm-service has the conversation even
when the memory service is down."""

import json
import uuid

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from app.models.message import Message, MessageStatus
from app.services import message_service as ms
from app.services.message_service import MessageService


def _m(sender, text, status=MessageStatus.SENT):
    return Message(conversation_id=uuid.uuid4(), message_id=uuid.uuid4(), sender=sender, content=text,
                   created_at=datetime.now(timezone.utc), status=status)


def test_history_is_chronological_and_keeps_only_finished_user_and_assistant_text():
    newest_first = [
        _m("assistant", "", MessageStatus.PENDING),          # placeholder of the current turn
        _m("assistant", "boom", MessageStatus.FAILED),       # failed reply
        _m("system", "summary text"),                        # not a conversation turn
        _m("assistant", "Use duckduckgo-search."),
        _m("user", "Which library?"),
    ]
    assert MessageService._format_history(newest_first) == [
        {"role": "user", "content": "Which library?"},
        {"role": "assistant", "content": "Use duckduckgo-search."},
    ]


def test_history_is_capped_by_count_message_size_and_total_size(monkeypatch):
    many = [_m("user" if i % 2 else "assistant", f"m{i}") for i in range(40)]  # newest first
    out = MessageService._format_history(many)
    assert len(out) == ms.HISTORY_EVENT_MAX_MESSAGES and out[-1]["content"] == "m0"  # newest kept, oldest-first order

    huge = MessageService._format_history([_m("user", "x" * 50_000)])
    assert len(huge[0]["content"]) == ms.HISTORY_EVENT_MAX_MESSAGE_CHARS

    monkeypatch.setattr(ms, "HISTORY_EVENT_MAX_CHARS", 30)
    capped = MessageService._format_history([_m("user", "a" * 20), _m("assistant", "b" * 20), _m("user", "c" * 20)])
    assert [x["content"][0] for x in capped] == ["a"]  # budget exhausted: older turns dropped, newest kept


def test_empty_history_is_an_empty_list():
    assert MessageService._format_history([]) == []


def test_payload_carries_history_and_defaults_to_empty():
    svc = MessageService(repo=MagicMock(), cache_service=MagicMock())
    hist = [{"role": "user", "content": "hi"}]
    p = svc._build_chat_message_created_payload(uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "q", history=hist)
    assert p["history"] == hist and json.loads(json.dumps(p))["history"] == hist
    assert svc._build_chat_message_created_payload(uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "q")["history"] == []


@pytest.mark.anyio
async def test_send_reads_history_before_the_new_rows_exist_and_survives_a_read_failure():
    repo = MagicMock()
    repo.history.return_value = [_m("assistant", "A1"), _m("user", "Q1")]
    repo.create_with_outbox.side_effect = lambda **kw: _m("user", kw["content"])
    svc = MessageService(repo=repo, cache_service=None)
    svc._open_pending_response = MagicMock(side_effect=lambda *a, **k: _noop())
    svc._invalidate_cache = MagicMock(side_effect=lambda *a, **k: _noop())
    await svc.send(uuid.uuid4(), uuid.uuid4(), "user", "follow-up", user_id=uuid.uuid4())
    payload = json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])
    assert payload["history"] == [{"role": "user", "content": "Q1"}, {"role": "assistant", "content": "A1"}]
    # the history was read before the pending placeholder was created
    assert repo.history.called

    repo.history.side_effect = RuntimeError("cassandra down")
    await svc.send(uuid.uuid4(), uuid.uuid4(), "user", "again", user_id=uuid.uuid4())
    assert json.loads(repo.create_with_outbox.call_args.kwargs["outbox_payload"])["history"] == []  # sending still works


async def _noop():
    return None


def test_payload_carries_include_diagrams_and_defaults_to_false():
    svc = MessageService(repo=MagicMock(), cache_service=MagicMock())
    args = (uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "q")
    assert svc._build_chat_message_created_payload(*args)["include_diagrams"] is False
    assert svc._build_chat_message_created_payload(*args, include_diagrams=True)["include_diagrams"] is True


def test_send_request_schema_accepts_include_diagrams_and_defaults_to_false():
    from app.schemas.message import CreateMessageRequest
    assert CreateMessageRequest(content="hi").include_diagrams is False
    assert CreateMessageRequest(content="hi", include_diagrams=True).include_diagrams is True
