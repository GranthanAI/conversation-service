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
from app.db.cassandra import cassandra_manager
from app.db.redis import redis_manager

@pytest.fixture(autouse=True, scope="module")
def initialize_api_test_connections():
    """
    Ensures active connections are available during ASGI HTTP client processing.
    """
    cassandra_manager.initialize()
    redis_manager.initialize()
    yield
