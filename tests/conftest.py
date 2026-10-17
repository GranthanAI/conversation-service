"""
Global Pytest Configuration.
Injects Python 3.12 compatibility patches globally before any module imports.
"""

import sys
import types

# --- Python 3.12 Compatibility Patch for Cassandra Driver ---
if "asyncore" not in sys.modules:
    asyncore_mock = types.ModuleType("asyncore")
    class DummyDispatcher:
        pass
    asyncore_mock.dispatcher = DummyDispatcher
    sys.modules['asyncore'] = asyncore_mock
# -------------------------------------------------------------

import pytest
from unittest.mock import patch

class MockRedis:
    def __init__(self):
        self.store = {}

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(key)

    async def delete(self, key):
        if key in self.store:
            del self.store[key]
            return 1
        return 0

    async def expire(self, key, ttl):
        return 1

    async def zadd(self, name, mapping):
        self.store.setdefault(("zset", name), {}).update(mapping)
        return len(mapping)

    async def zrem(self, name, member):
        return 1 if self.store.get(("zset", name), {}).pop(member, None) is not None else 0

    async def zrangebyscore(self, name, min, max):
        return [m for m, s in self.store.get(("zset", name), {}).items() if min <= s <= max]

@pytest.fixture(autouse=True)
def mock_redis_client(request):
    if "integration" in request.node.fspath.strpath:
        yield
        return
        
    mock_redis = MockRedis()
    with patch("app.db.redis.redis_manager.client", mock_redis), \
         patch("app.workers.outbox_worker.redis_manager.client", mock_redis), \
         patch("app.workers.retry_worker.redis_manager.client", mock_redis):
        yield mock_redis
