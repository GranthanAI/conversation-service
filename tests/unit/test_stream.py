"""
SSE Stream Unit Tests.
Verifies Redis stream ownership locks, PubSub subscription loops, and FastAPI SSE streaming response formats.
"""

import json
import uuid
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch
from fastapi.testclient import TestClient

from app.main import app
from app.services.stream_service import StreamService
from app.api.deps import get_stream_service, get_conversation_service
from app.security.dependencies import get_current_user
from app.security.models import CurrentUser
from app.models.conversation import Conversation, ConversationStatus

@pytest.fixture
def mock_redis():
    return AsyncMock()

@pytest.mark.anyio
async def test_stream_service_register_ownership(mock_redis):
    conv_id = uuid.uuid4()
    service = StreamService(redis_client=mock_redis)
    
    await service.register_ownership(conv_id)
    
    mock_redis.set.assert_called_once_with(f"stream:{conv_id}", service.pod_id, ex=60)

@pytest.mark.anyio
async def test_stream_service_renew_ownership(mock_redis):
    conv_id = uuid.uuid4()
    service = StreamService(redis_client=mock_redis)
    
    await service.renew_ownership(conv_id)
    
    mock_redis.expire.assert_called_once_with(f"stream:{conv_id}", 60)

@pytest.mark.anyio
async def test_stream_service_release_ownership(mock_redis):
    conv_id = uuid.uuid4()
    service = StreamService(redis_client=mock_redis)
    
    # Mock owner check
    mock_redis.get.return_value = service.pod_id
    
    await service.release_ownership(conv_id)
    
    mock_redis.get.assert_called_once_with(f"stream:{conv_id}")
    mock_redis.delete.assert_called_once_with(f"stream:{conv_id}")

@pytest.mark.anyio
async def test_stream_service_publish_token(mock_redis):
    conv_id = uuid.uuid4()
    payload = {"chunk": "Test", "is_final": False}
    service = StreamService(redis_client=mock_redis)
    
    await service.publish_token(conv_id, payload)
    
    mock_redis.publish.assert_called_once_with(f"conversation:{conv_id}:stream", json.dumps(payload))

def test_api_get_stream_requires_auth():
    client = TestClient(app)
    conv_id = uuid.uuid4()
    
    response = client.get(f"/v1/stream/{conv_id}")
    
    assert response.status_code == 401

def test_api_get_stream_success():
    client = TestClient(app)
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    mock_user = CurrentUser(id=user_id, email="test@example.com")
    
    # Mock conversation ownership check
    mock_conv_service = MagicMock()
    mock_conv_service.get = AsyncMock(return_value=Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Active chat",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    ))
    
    # Mock StreamService subscribe
    mock_stream_service = MagicMock()
    async def mock_subscribe(conversation_id):
        yield "data: {\"chunk\": \"Hello\"}\n\n"
        yield "data: {\"chunk\": \" World\", \"is_final\": true}\n\n"
    
    mock_stream_service.subscribe.side_effect = mock_subscribe
    
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_conversation_service] = lambda: mock_conv_service
    app.dependency_overrides[get_stream_service] = lambda: mock_stream_service
    
    response = client.get(f"/v1/stream/{conv_id}")
    
    app.dependency_overrides.clear()
    
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    
    lines = []
    for line in response.iter_lines():
        if isinstance(line, bytes):
            lines.append(line.decode("utf-8"))
        else:
            lines.append(line)
    assert "data: {\"chunk\": \"Hello\"}" in lines
    assert "data: {\"chunk\": \" World\", \"is_final\": true}" in lines
