"""The pending-response marker must outlive the timeout deadline (otherwise the
timeout worker sees an expired key and never fails a lost generation) and must
be removed once a terminal response has been handled."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.core.config import settings
from app.services.message_service import MessageService

pytestmark = pytest.mark.anyio


def _service():
    redis = MagicMock()
    redis.set = AsyncMock()
    redis.zadd = AsyncMock()
    redis.delete = AsyncMock()
    redis.zrem = AsyncMock()
    cache = MagicMock()
    cache.redis = redis
    return MessageService(repo=MagicMock(), cache_service=cache), redis


async def test_marker_ttl_outlives_the_deadline():
    svc, redis = _service()
    await svc._register_pending_response(uuid.uuid4(), uuid.uuid4())
    ttl = redis.set.call_args.kwargs["ex"]
    assert ttl > settings.PENDING_RESPONSE_TIMEOUT_SECONDS
    assert ttl == settings.PENDING_RESPONSE_TIMEOUT_SECONDS + settings.PENDING_RESPONSE_MARKER_GRACE_SECONDS


async def test_clear_removes_key_and_zset_entry():
    svc, redis = _service()
    mid = uuid.uuid4()
    await svc.clear_pending_response(mid)
    redis.delete.assert_awaited_once_with(f"pending_response:{mid}")
    redis.zrem.assert_awaited_once_with("pending_response:zset", str(mid))


async def test_clear_never_raises_when_redis_is_down():
    svc, redis = _service()
    redis.delete.side_effect = ConnectionError("redis down")
    await svc.clear_pending_response(uuid.uuid4())  # must not propagate
