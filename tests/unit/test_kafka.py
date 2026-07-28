"""
Kafka Event Publishing Unit Tests.
Verifies that services trigger direct publishing of domain events to Kafka topics with correct payload contracts.
"""

import json
import uuid
from datetime import datetime, timezone
import pytest
from unittest.mock import MagicMock, AsyncMock, ANY

from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from app.events.topics import KafkaTopics

@pytest.fixture
def mock_repos():
    mock_conv_repo = MagicMock()
    mock_msg_repo = MagicMock()
    return mock_conv_repo, mock_msg_repo

@pytest.fixture
def mock_producer():
    return AsyncMock()

@pytest.mark.anyio
async def test_conversation_service_create_publishes_to_kafka(mock_repos, mock_producer):
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
    
    service = ConversationService(repo=mock_conv_repo, producer_client=mock_producer)
    await service.create(user_id=user_id, title="Physics Title")
    
    mock_producer.publish.assert_called_once_with(
        topic=KafkaTopics.CONVERSATION_CREATED,
        key=ANY,
        value={
            "conversation_id": ANY,
            "user_id": str(user_id),
            "title": "Physics Title",
            "status": "active"
        }
    )

@pytest.mark.anyio
async def test_conversation_service_rename_publishes_to_kafka(mock_repos, mock_producer):
    mock_conv_repo, _ = mock_repos
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    # Mock lookup
    mock_conv_repo.get.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Old title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    # Mock update
    mock_conv_repo.update_with_outbox.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="New title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    service = ConversationService(repo=mock_conv_repo, producer_client=mock_producer)
    await service.rename(conv_id, "New title")
    
    mock_producer.publish.assert_called_once_with(
        topic=KafkaTopics.CONVERSATION_UPDATED,
        key=str(conv_id),
        value={
            "conversation_id": str(conv_id),
            "user_id": str(user_id),
            "title": "New title",
            "status": "active"
        }
    )

@pytest.mark.anyio
async def test_message_service_send_publishes_to_kafka(mock_repos, mock_producer):
    _, mock_msg_repo = mock_repos
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Hello Kafka",
        created_at=datetime.now(timezone.utc),
        status=MessageStatus.SENT
    )
    
    service = MessageService(repo=mock_msg_repo, producer_client=mock_producer)
    await service.send(conv_id, msg_id, "user", "Hello Kafka")
    
    mock_producer.publish.assert_called_once_with(
        topic=KafkaTopics.CHAT_MESSAGE_CREATED,
        key=str(conv_id),
        value={
            "conversation_id": str(conv_id),
            "message_id": str(msg_id),
            "sender": "user",
            "content": "Hello Kafka",
            "status": "sent"
        }
    )
