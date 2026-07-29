"""
Redis Integration Tests.
Verifies caching operations, message list serialization, and cache evictions against local Redis container.
"""

import uuid
import pytest
from datetime import datetime, timezone

from app.db.redis import redis_manager
from app.services.cache_service import CacheService
from app.models.message import Message, MessageStatus

@pytest.fixture(scope="module")
async def setup_redis():
    redis_manager.initialize()
    yield
    # Shutdown handled by application runner

@pytest.mark.anyio
async def test_redis_cache_aside_operations(setup_redis):
    cache = CacheService(redis_client=redis_manager.client)
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    messages = [
        Message(
            conversation_id=conv_id,
            message_id=msg_id,
            sender="user",
            content="Cache test message",
            created_at=datetime.now(timezone.utc),
            status=MessageStatus.SENT
        )
    ]
    
    # 1. Verify Cache Miss initially
    miss = await cache.get_last_50_messages(conv_id)
    assert miss is None
    
    # 2. Write to cache
    await cache.set_last_50_messages(conv_id, messages)
    
    # 3. Read from cache and assert correctness
    hit = await cache.get_last_50_messages(conv_id)
    assert hit is not None
    assert len(hit) == 1
    assert hit[0].content == "Cache test message"
    
    # 4. Invalidate cache list
    await cache.delete_last_50_messages(conv_id)
    assert await cache.get_last_50_messages(conv_id) is None
