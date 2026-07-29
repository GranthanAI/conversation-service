"""
Outbox and Kafka Publishing Unit Tests.
Verifies services write to outbox tables and OutboxWorker publishes events asynchronously.
"""

import json
import uuid
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.workers.outbox_worker import start_outbox_worker
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from app.models.outbox import OutboxEvent
from app.events.topics import KafkaTopics

@pytest.fixture
def mock_repos():
    mock_conv_repo = MagicMock()
    mock_msg_repo = MagicMock()
    return mock_conv_repo, mock_msg_repo

@pytest.mark.anyio
async def test_conversation_service_create_writes_to_outbox(mock_repos):
    mock_conv_repo, _ = mock_repos
    user_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    
    mock_conv_repo.create_with_outbox.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Physics Title",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    )
    
    service = ConversationService(repo=mock_conv_repo)
    await service.create(user_id=user_id, title="Physics Title")
    
    # Assert repository create_with_outbox was called
    mock_conv_repo.create_with_outbox.assert_called_once()

@pytest.mark.anyio
async def test_message_service_send_writes_to_outbox(mock_repos):
    _, mock_msg_repo = mock_repos
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Hello Outbox",
        created_at=datetime.now(timezone.utc),
        status=MessageStatus.SENT
    )
    
    service = MessageService(repo=mock_msg_repo)
    # Patch background simulate generation pipeline task to prevent running it in tests
    with patch.object(service, "simulate_generation_pipeline", return_value=None):
        await service.send(conv_id, msg_id, "user", "Hello Outbox")
        
    mock_msg_repo.create_with_outbox.assert_called_once()

@pytest.mark.anyio
@patch("app.workers.outbox_worker.CassandraOutboxRepository")
@patch("app.workers.outbox_worker.kafka_producer_client")
async def test_outbox_worker_publishes_events(mock_producer_client, mock_outbox_repo_class):
    mock_repo = MagicMock()
    mock_outbox_repo_class.return_value = mock_repo
    
    event_id = uuid.uuid1()
    conv_id = uuid.uuid4()
    payload_str = json.dumps({"conversation_id": str(conv_id), "title": "Test"})
    
    # 1. Mock fetch_unpublished to return one event for bucket 0, and none for others
    # We will raise asyncio.CancelledError inside sleep to terminate the loop
    mock_repo.fetch_unpublished.side_effect = lambda bucket, limit: (
        [OutboxEvent(bucket=0, event_id=event_id, event_type=KafkaTopics.CONVERSATION_CREATED, payload=payload_str, published=False, created_at=datetime.now())]
        if bucket == 0 else []
    )
    
    mock_producer_client.publish = AsyncMock()

    # We patch asyncio.sleep to raise CancelledError immediately on sleep so it does only one poll cycle
    with patch("asyncio.sleep", side_effect=asyncio.CancelledError):
        await start_outbox_worker()
        
    # Verify Kafka publisher and DB updates occurred
    mock_producer_client.publish.assert_called_once_with(
        topic=KafkaTopics.CONVERSATION_CREATED,
        key=str(conv_id),
        value={"conversation_id": str(conv_id), "title": "Test"}
    )
    mock_repo.mark_published.assert_called_once_with(0, event_id)
