"""
Caching Unit Tests.
Verifies CacheService read/write/invalidation routines, and service-level interactions.
"""

import json
import uuid
from datetime import datetime, timezone
import pytest
from unittest.mock import MagicMock, AsyncMock

from app.services.cache_service import CacheService
from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus

@pytest.fixture
def mock_redis():
    return AsyncMock()

@pytest.mark.anyio
async def test_cache_service_get_conversation_hit(mock_redis):
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    mock_redis.hgetall.return_value = {
        "conversation_id": str(conv_id),
        "user_id": str(user_id),
        "title": "Cached title",
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "status": "active"
    }
    
    cache_service = CacheService(redis_client=mock_redis)
    conv = await cache_service.get_conversation(conv_id)
    
    assert conv is not None
    assert conv.title == "Cached title"
    mock_redis.hgetall.assert_called_once_with(f"conversation:{conv_id}")

@pytest.mark.anyio
async def test_cache_service_set_conversation(mock_redis):
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    conv = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Active title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    cache_service = CacheService(redis_client=mock_redis)
    await cache_service.set_conversation(conv)
    
    mock_redis.hset.assert_called_once()
    mock_redis.expire.assert_called_once_with(f"conversation:{conv_id}", 3600)

@pytest.mark.anyio
async def test_conversation_service_caching_interactions(mock_redis):
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    mock_redis.hgetall.return_value = {}  # cache miss
    
    mock_repo = MagicMock()
    mock_repo.get.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Repo title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    cache_service = CacheService(redis_client=mock_redis)
    service = ConversationService(repo=mock_repo, cache_service=cache_service)
    
    conv = await service.get(conv_id)
    
    assert conv is not None
    assert conv.title == "Repo title"
    mock_repo.get.assert_called_once_with(conv_id)
    mock_redis.hset.assert_called_once()

@pytest.mark.anyio
async def test_message_service_caching_interactions(mock_redis):
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    mock_redis.lrange.return_value = [
        json.dumps({
            "conversation_id": str(conv_id),
            "message_id": str(msg_id),
            "sender": "user",
            "content": "Cache message",
            "created_at": now.isoformat(),
            "status": "sent"
        })
    ]
    
    mock_repo = MagicMock()
    cache_service = CacheService(redis_client=mock_redis)
    service = MessageService(repo=mock_repo, cache_service=cache_service)
    
    messages = await service.history(conv_id, limit=50, cursor=None)
    
    assert len(messages) == 1
    assert messages[0].content == "Cache message"
    mock_repo.history.assert_not_called()
    mock_redis.lrange.assert_called_once_with(f"conversation:{conv_id}:last50", 0, 49)
