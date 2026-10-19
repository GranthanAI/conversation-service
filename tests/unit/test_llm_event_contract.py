"""Contract test: the event payloads llm-service actually emits (fixtures are generated
from its ChatMessageCreatedEvent / ChatResponseChunkEvent / ChatResponseGeneratedEvent
models and its real ErrorHandler, tests/fixtures/llm_events.json) must drive this
service's consumers correctly, and this service's request payload must satisfy llm-service's
request model.

Regenerate the fixture (run in llm-service) whenever its request/response models change.
"""

import json
import pathlib
import uuid
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from app.events.consumers import _handle_chat_response_chunk, _handle_chat_response_generated
from app.services.message_service import MessageService

FIXTURES = json.loads((pathlib.Path(__file__).parent.parent / "fixtures" / "llm_events.json").read_text())
REQ = FIXTURES["request"]["message_id"]
RESP = FIXTURES["request"]["response_message_id"]

pytestmark = pytest.mark.anyio


def _deps():
    inbox = MagicMock()
    inbox.exists.return_value = False
    stream = MagicMock()
    stream.publish_token = AsyncMock()
    msgs = MagicMock()
    msgs.finalize_assistant_message = AsyncMock(return_value=object())
    msgs.fail_assistant_message = AsyncMock(return_value=object())
    msgs.clear_pending_response = AsyncMock()
    return inbox, stream, msgs


# ---- response direction: llm -> conversation ---------------------------------------


def test_fixture_distinguishes_request_and_response_identity():
    assert REQ != RESP
    for name in ("chunk", "final", "error"):
        assert FIXTURES[name]["response_message_id"] == RESP, name
    assert FIXTURES["chunk"]["message_id"] == REQ
    assert FIXTURES["final"]["request_message_id"] == FIXTURES["error"]["request_message_id"] == REQ


async def test_chunk_is_forwarded_under_the_assistant_message_and_never_final():
    inbox, stream, _ = _deps()
    await _handle_chat_response_chunk(dict(FIXTURES["chunk"]), inbox, stream)
    payload = stream.publish_token.call_args.args[1]
    assert payload["message_id"] == RESP and payload["request_message_id"] == REQ
    assert payload["content"] == "Hel" and payload["sequence_number"] == 1
    assert payload["is_final"] is False


async def test_last_chunk_does_not_end_the_turn():
    """The final event can overtake the last chunks, so is_last must not end the stream."""
    inbox, stream, _ = _deps()
    await _handle_chat_response_chunk({**FIXTURES["chunk"], "is_last": True}, inbox, stream)
    assert stream.publish_token.call_args.args[1]["is_final"] is False


async def test_final_event_persists_under_the_response_id_with_citations():
    inbox, stream, msgs = _deps()
    await _handle_chat_response_generated(dict(FIXTURES["final"]), inbox, msgs, stream)

    msgs.finalize_assistant_message.assert_awaited_once()
    args = msgs.finalize_assistant_message.call_args.args
    assert args[1] == UUID(RESP), "must NOT be the request (user) message id"
    assert args[2] == "Hello world"
    msgs.clear_pending_response.assert_awaited_once_with(UUID(RESP))

    payload = stream.publish_token.call_args.args[1]
    assert payload["message_id"] == RESP and payload["request_message_id"] == REQ
    assert payload["is_final"] is True and payload["status"] == "completed"
    assert payload["full_content"] == "Hello world"
    assert payload["citations"][0]["source_name"] == "a.pdf"
    inbox.save.assert_called_once()


async def test_error_event_from_llm_error_handler_fails_the_assistant_message():
    inbox, stream, msgs = _deps()
    await _handle_chat_response_generated(dict(FIXTURES["error"]), inbox, msgs, stream)

    args = msgs.fail_assistant_message.call_args.args
    assert args[1] == UUID(RESP)
    msgs.clear_pending_response.assert_awaited_once_with(UUID(RESP))
    payload = stream.publish_token.call_args.args[1]
    assert payload["is_final"] is True and payload["status"] == "failed" and payload["message_id"] == RESP


async def test_duplicate_final_event_is_not_reprocessed():
    inbox, stream, msgs = _deps()
    inbox.exists.return_value = True
    await _handle_chat_response_generated(dict(FIXTURES["final"]), inbox, msgs, stream)
    msgs.finalize_assistant_message.assert_not_called()
    msgs.clear_pending_response.assert_not_called()
    stream.publish_token.assert_not_called()


@pytest.mark.parametrize("kind", ["chunk", "final", "error"])
async def test_legacy_event_without_response_message_id_is_ignored(kind):
    inbox, stream, msgs = _deps()
    legacy = {k: v for k, v in FIXTURES[kind if kind != "error" else "error"].items() if k != "response_message_id"}
    if kind == "chunk":
        await _handle_chat_response_chunk(legacy, inbox, stream)
    else:
        await _handle_chat_response_generated(legacy, inbox, msgs, stream)
    msgs.finalize_assistant_message.assert_not_called()
    msgs.fail_assistant_message.assert_not_called()
    stream.publish_token.assert_not_called()


# ---- request direction: conversation -> llm ------------------------------------------


def test_request_payload_satisfies_llm_request_model():
    """Every field in llm-service's request model fixture is present, with the same
    types, in what MessageService publishes; the identity fields are distinct UUIDs."""
    svc = MessageService(repo=MagicMock())
    conv, req, resp = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    payload = svc._build_chat_message_created_payload(
        conv, req, uuid.uuid4(), "hi", file_ids=[uuid.uuid4()], response_message_id=resp
    )
    llm_request = FIXTURES["request"]
    missing = set(llm_request) - set(payload)
    assert not missing, f"llm-service request model has fields we do not send: {missing}"
    for key, value in llm_request.items():
        assert isinstance(payload[key], type(value)) or payload[key] is None, key
    assert payload["schema_version"] == llm_request["schema_version"] == "2.3"
    assert payload["message_id"] == str(req) and payload["response_message_id"] == str(resp)
    UUID(payload["message_id"]); UUID(payload["response_message_id"])


def test_generation_preferences_we_send_are_understood_by_llm_service_unchanged():
    """The fixture's `request_preferences_parsed` is what llm-service's normalizer produced
    from these preferences. Every field we send must survive parsing with the same value
    (a value llm-service did not accept would silently fall back to its default)."""
    from app.models.preferences import UserPreferences, generation_preferences

    sent = generation_preferences(UserPreferences(
        answer_length="concise", tone="simple", language="hi",
        personal_instructions="Prefer Python examples", citations="hide",
    ))
    parsed = FIXTURES["request_preferences_parsed"]
    assert set(sent) == set(parsed), f"field mismatch: {set(sent) ^ set(parsed)}"
    assert sent == parsed


def test_request_carries_defaults_applied_in_the_shape_llm_service_reads():
    assert set(FIXTURES["request"]["defaults_applied"]) == {"mode", "tools"}
    svc = MessageService(repo=MagicMock())
    payload = svc._build_chat_message_created_payload(
        uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "hi", response_message_id=uuid.uuid4(),
        defaults_applied={"mode": True, "tools": False}, preferences={"tone": "simple"},
    )
    assert payload["defaults_applied"] == {"mode": True, "tools": False}
    assert payload["preferences"] == {"tone": "simple"}
