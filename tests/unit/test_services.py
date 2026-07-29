"""
Service Layer Unit Tests.
Mocks repository queries and Redis Cache-Aside loops to verify orchestration.
"""

# --- Python 3.12 Compatibility Patch for Cassandra Driver ---
import sys
import types
asyncore_mock = types.ModuleType("asyncore")
class DummyDispatcher:
    pass
asyncore_mock.dispatcher = DummyDispatcher
sys.modules['asyncore'] = asyncore_mock
# -------------------------------------------------------------

import pytest
from unittest.mock import MagicMock, AsyncMock, ANY, patch
import uuid
from datetime import datetime, timezone

from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.services.cache_service import CacheService
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus

@pytest.fixture
def mock_repos():
    """
    Creates mock repositories for service DI.
    """
    mock_conv_repo = MagicMock()
    mock_msg_repo = MagicMock()
    return mock_conv_repo, mock_msg_repo

@pytest.fixture
def mock_redis_client():
    """
    Creates mock async Redis client.
    """
    mock_client = AsyncMock()
    return mock_client

@pytest.mark.anyio
async def test_conversation_service_create(mock_repos):
    mock_conv_repo, _ = mock_repos
    service = ConversationService(repo=mock_conv_repo)
    
    user_id = uuid.uuid4()
    mock_conv_repo.create_with_outbox.return_value = Conversation(
        conversation_id=uuid.uuid4(),
        user_id=user_id,
        title="Test title",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    )
    
    conv = await service.create(user_id, "Test title")
    
    assert conv.user_id == user_id
    mock_conv_repo.create_with_outbox.assert_called_once()

@pytest.mark.anyio
async def test_conversation_service_rename(mock_repos):
    mock_conv_repo, _ = mock_repos
    service = ConversationService(repo=mock_conv_repo)
    
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    # Mock lookup get
    mock_conv_repo.get.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Old Title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    # Mock update_with_outbox
    mock_conv_repo.update_with_outbox.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="New Title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    updated = await service.rename(conv_id, "New Title")
    assert updated is not None
    assert updated.title == "New Title"
    mock_conv_repo.get.assert_called_once_with(conv_id)
    mock_conv_repo.update_with_outbox.assert_called_once()

@pytest.mark.anyio
async def test_message_service_send(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))
    
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Test content",
        created_at=datetime.now(timezone.utc),
        status=MessageStatus.SENT
    )
    
    with patch.object(service, "simulate_generation_pipeline", return_value=None):
        msg = await service.send(conv_id, msg_id, "user", "Test content")
    
    assert msg.message_id == msg_id
    mock_msg_repo.create_with_outbox.assert_called_once()
    mock_redis_client.delete.assert_called_once_with(f"conversation:{conv_id}:last50")

@pytest.mark.anyio
async def test_message_service_history_cache_hit(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))
    
    conv_id = uuid.uuid4()
    
    # Mock cache hit returning serialized items
    mock_redis_client.lrange.return_value = [
        '{"conversation_id": "' + str(conv_id) + '", "message_id": "' + str(uuid.uuid4()) + '", "sender": "user", "content": "Cache Hello", "created_at": "2026-07-27T00:00:00Z", "status": "sent"}'
    ]
    
    history = await service.history(conv_id)
    assert len(history) == 1
    assert history[0].content == "Cache Hello"
    mock_redis_client.lrange.assert_called_once()
    mock_msg_repo.history.assert_not_called()

@pytest.mark.anyio
async def test_message_service_history_cache_miss(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))
    
    conv_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    # Mock cache miss
    mock_redis_client.lrange.return_value = []
    # Mock DB return
    mock_msg_repo.history.return_value = [
        Message(conversation_id=conv_id, message_id=uuid.uuid4(), sender="user", content="DB Hello", created_at=now, status=MessageStatus.SENT)
    ]
    
    history = await service.history(conv_id)
    assert len(history) == 1
    assert history[0].content == "DB Hello"
    mock_redis_client.lrange.assert_called_once()
    mock_msg_repo.history.assert_called_once_with(conv_id, 50, None)
    mock_redis_client.rpush.assert_called_once()
    mock_redis_client.expire.assert_called_once()
