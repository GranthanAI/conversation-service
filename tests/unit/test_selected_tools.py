"""selected_tools: request validation and propagation into the chat.message.created Kafka event."""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from app.schemas.message import CreateMessageRequest
from app.services.message_service import MessageService


def test_valid_tools_are_trimmed_and_deduplicated():
    req = CreateMessageRequest(content="hi", selected_tools=[" web_search ", "web_search", "calc.v2"])
    assert req.selected_tools == ["web_search", "calc.v2"]


def test_omitted_or_empty_selection_is_allowed():
    assert CreateMessageRequest(content="hi").selected_tools is None
    assert CreateMessageRequest(content="hi", selected_tools=[]).selected_tools == []


@pytest.mark.parametrize("bad", ["", "   ", "has space", "semi;colon", "../etc", "x" * 65, "<script>"])
def test_malformed_tool_ids_are_rejected(bad):
    with pytest.raises(ValidationError):
        CreateMessageRequest(content="hi", selected_tools=[bad])


def test_too_many_tools_are_rejected():
    with pytest.raises(ValidationError):
        CreateMessageRequest(content="hi", selected_tools=[f"t{i}" for i in range(11)])


def test_selected_tools_and_file_ids_reach_the_kafka_event_payload():
    svc = MessageService(repo=MagicMock(), cache_service=MagicMock())
    cid, mid, rid, fid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    payload = svc._build_chat_message_created_payload(
        cid, mid, uuid.uuid4(), "search this", mode="default",
        selected_tools=["web_search"], file_ids=[fid], response_message_id=rid,
    )
    assert payload["selected_tools"] == ["web_search"]
    assert payload["file_ids"] == [str(fid)]
    assert payload["response_message_id"] == str(rid)
    assert json.loads(json.dumps(payload))["selected_tools"] == ["web_search"]  # serializable as sent to Kafka


def test_no_selection_produces_an_empty_list_not_null():
    svc = MessageService(repo=MagicMock(), cache_service=MagicMock())
    payload = svc._build_chat_message_created_payload(uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "hi")
    assert payload["selected_tools"] == []


@pytest.mark.parametrize("ok", [None, "", "smart", "deep_research", " tutor "])
def test_valid_modes_pass(ok):
    CreateMessageRequest(content="hi", mode=ok)


@pytest.mark.parametrize("bad", ["has space", "x" * 33, "a;b", "../x"])
def test_malformed_modes_are_rejected(bad):
    with pytest.raises(ValidationError):
        CreateMessageRequest(content="hi", mode=bad)


def test_mode_reaches_the_kafka_event_as_mode_hint():
    svc = MessageService(repo=MagicMock(), cache_service=MagicMock())
    payload = svc._build_chat_message_created_payload(uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), "hi", mode="deep_research")
    assert payload["mode_hint"] == "deep_research"
