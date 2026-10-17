"""Contract test: the event payloads llm-service actually emits (fixtures are
generated from its ChatResponseChunkEvent / ChatResponseGeneratedEvent models,
tests/fixtures/llm_events.json) must drive this service's consumers correctly.

Regenerate the fixture whenever llm-service's response models change.
"""

import json
import pathlib
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from app.events.consumers import _handle_chat_response_chunk, _handle_chat_response_generated

FIXTURES = json.loads((pathlib.Path(__file__).parent.parent / "fixtures" / "llm_events.json").read_text())

pytestmark = pytest.mark.anyio


def _deps():
    inbox = MagicMock()
    inbox.exists.return_value = False
    stream = MagicMock()
    stream.publish_token = AsyncMock()
    msgs = MagicMock()
    msgs.finalize_assistant_message = AsyncMock()
    msgs._invalidate_cache = AsyncMock()
    msgs.clear_pending_response = AsyncMock()
    msgs.repo = MagicMock()
    return inbox, stream, msgs


def _uuidify(event: dict) -> dict:
    """Consumers parse conversation/message ids as UUIDs; map the fixture's
    readable ids onto stable UUIDs without changing any other field."""
    mapping = {"c-1": "00000000-0000-0000-0000-0000000000c1", "m-1": "00000000-0000-0000-0000-0000000000a1",
               "m-2": "00000000-0000-0000-0000-0000000000a2"}
    out = dict(event)
    for key in ("conversation_id", "message_id", "request_message_id"):
        if key in out:
            out[key] = mapping[out[key]]
    return out


async def test_chunk_event_is_forwarded_and_never_final():
    inbox, stream, _ = _deps()
    event = _uuidify(FIXTURES["chunk"])
    await _handle_chat_response_chunk(event, inbox, stream)
    payload = stream.publish_token.call_args.args[1]
    assert payload["content"] == "Hel"
    assert payload["sequence_number"] == 1
    assert payload["is_final"] is False


async def test_last_chunk_does_not_end_the_turn():
    """The final event can overtake the last chunks, so is_last must not end the stream."""
    inbox, stream, _ = _deps()
    event = _uuidify({**FIXTURES["chunk"], "is_last": True})
    await _handle_chat_response_chunk(event, inbox, stream)
    assert stream.publish_token.call_args.args[1]["is_final"] is False


async def test_final_event_persists_full_content_and_carries_citations():
    inbox, stream, msgs = _deps()
    event = _uuidify(FIXTURES["final"])
    await _handle_chat_response_generated(event, inbox, msgs, stream)

    msgs.finalize_assistant_message.assert_awaited_once()
    args = msgs.finalize_assistant_message.call_args.args
    assert args[1] == UUID(event["request_message_id"])
    assert args[2] == "Hello world"

    payload = stream.publish_token.call_args.args[1]
    assert payload["is_final"] is True and payload["status"] == "completed"
    assert payload["full_content"] == "Hello world"
    assert payload["citations"][0]["source_name"] == "a.pdf"
    inbox.save.assert_called_once()
    msgs.clear_pending_response.assert_awaited_once_with(UUID(event["request_message_id"]))


async def test_error_event_marks_message_failed_and_terminates_stream():
    inbox, stream, msgs = _deps()
    event = _uuidify(FIXTURES["error"])
    await _handle_chat_response_generated(event, inbox, msgs, stream)

    kwargs = msgs.repo.create_message_direct.call_args.kwargs
    assert kwargs["status"] == "failed" and kwargs["content"] == "boom"
    payload = stream.publish_token.call_args.args[1]
    assert payload["is_final"] is True and payload["status"] == "failed"
    msgs.clear_pending_response.assert_awaited_once()


async def test_duplicate_final_event_is_not_reprocessed():
    inbox, stream, msgs = _deps()
    inbox.exists.return_value = True
    await _handle_chat_response_generated(_uuidify(FIXTURES["final"]), inbox, msgs, stream)
    msgs.finalize_assistant_message.assert_not_called()
    stream.publish_token.assert_not_called()
    msgs.clear_pending_response.assert_not_called()


@pytest.mark.parametrize("key", ["conversation_id", "message_id", "sequence_number", "content", "is_last"])
def test_llm_chunk_event_still_provides_every_field_the_consumer_reads(key):
    assert key in FIXTURES["chunk"]


@pytest.mark.parametrize("key", ["conversation_id", "request_message_id", "full_content", "status", "citations"])
def test_llm_final_event_still_provides_every_field_the_consumer_reads(key):
    assert key in FIXTURES["final"]
