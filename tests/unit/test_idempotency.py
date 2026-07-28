"""
Idempotency Unit Tests.
Verifies SETNX lock acquisition, concurrent protection, response caching, and lock eviction rules.
"""

import json
import uuid
import pytest
from fastapi import HTTPException
from unittest.mock import MagicMock, AsyncMock

from app.services.idempotency_service import IdempotencyService

@pytest.fixture
def mock_redis():
    return AsyncMock()

@pytest.mark.anyio
async def test_idempotency_claim_new_key_success(mock_redis):
    # Mock SETNX returns True (key was set)
    mock_redis.set.return_value = True
    
    key = str(uuid.uuid4())
    service = IdempotencyService(redis_client=mock_redis)
    
    status = await service.claim_key(key)
    
    assert status is None
    mock_redis.set.assert_called_once_with(f"idempotency:{key}", "processing", nx=True, ex=86400)

@pytest.mark.anyio
async def test_idempotency_claim_existing_key_processing(mock_redis):
    # Mock SETNX returns False (key already exists)
    mock_redis.set.return_value = False
    mock_redis.get.return_value = "processing"
    
    key = str(uuid.uuid4())
    service = IdempotencyService(redis_client=mock_redis)
    
    status = await service.claim_key(key)
    
    assert status == "processing"
    mock_redis.set.assert_called_once()
    mock_redis.get.assert_called_once_with(f"idempotency:{key}")

@pytest.mark.anyio
async def test_idempotency_claim_existing_key_cached_response(mock_redis):
    # Mock SETNX returns False, get returns serialized JSON response
    response_payload = {"message_id": str(uuid.uuid4()), "content": "Hello"}
    mock_redis.set.return_value = False
    mock_redis.get.return_value = json.dumps(response_payload)
    
    key = str(uuid.uuid4())
    service = IdempotencyService(redis_client=mock_redis)
    
    status = await service.claim_key(key)
    
    assert status is not None
    decoded = json.loads(status)
    assert decoded["content"] == "Hello"

@pytest.mark.anyio
async def test_idempotency_save_response(mock_redis):
    key = str(uuid.uuid4())
    payload = {"message_id": str(uuid.uuid4()), "sender": "user", "content": "Test"}
    
    service = IdempotencyService(redis_client=mock_redis)
    await service.save_response(key, payload)
    
    mock_redis.set.assert_called_once_with(f"idempotency:{key}", json.dumps(payload), ex=86400)

@pytest.mark.anyio
async def test_idempotency_remove_lock(mock_redis):
    key = str(uuid.uuid4())
    
    service = IdempotencyService(redis_client=mock_redis)
    await service.remove_lock(key)
    
    mock_redis.delete.assert_called_once_with(f"idempotency:{key}")
