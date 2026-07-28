"""
API Router End-to-End Unit Tests.
Tests FastAPI controller endpoints, JSON serialization, Pydantic validation, and HTTP status codes using TestClient.
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

import uuid
from datetime import datetime, timezone
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, AsyncMock

from app.main import app
from app.api.deps import get_conversation_service, get_message_service
from app.security import get_current_user, CurrentUser
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from app.utils.pagination import encode_cursor

client = TestClient(app)

@pytest.fixture
def mock_user():
    return CurrentUser(id=uuid.uuid4(), email="test@example.com")

@pytest.fixture
def mock_services():
    mock_conv_service = MagicMock()
    mock_msg_service = AsyncMock()
    return mock_conv_service, mock_msg_service

def test_api_create_conversation(mock_user, mock_services):
    mock_conv_service, _ = mock_services
    conv_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    mock_conv_service.create.return_value = Conversation(
        conversation_id=conv_id,
        user_id=mock_user.id,
        title="Test Physics",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_conversation_service] = lambda: mock_conv_service
    
    response = client.post(
        "/v1/conversations",
        json={"title": "Test Physics"}
    )
    
    app.dependency_overrides.clear()
    
    assert response.status_code == 201
    data = response.json()
    assert data["conversation_id"] == str(conv_id)
    assert data["title"] == "Test Physics"

def test_api_list_conversations(mock_user, mock_services):
    mock_conv_service, _ = mock_services
    now = datetime.now(timezone.utc)
    conv_id = uuid.uuid4()
    
    mock_conv_service.list.return_value = [
        Conversation(
            conversation_id=conv_id,
            user_id=mock_user.id,
            title="List Conv",
            created_at=now,
            updated_at=now,
            status=ConversationStatus.ACTIVE
        )
    ]
    
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_conversation_service] = lambda: mock_conv_service
    
    response = client.get("/v1/conversations?limit=1")
    
    app.dependency_overrides.clear()
    
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["title"] == "List Conv"
    assert data["has_more"] is True
    assert data["next_cursor"] is not None

def test_api_list_conversations_with_opaque_cursor(mock_user, mock_services):
    mock_conv_service, _ = mock_services
    now = datetime.now(timezone.utc)
    conv_id = uuid.uuid4()
    
    opaque_cursor = encode_cursor({"updated_at": now.isoformat(), "conversation_id": str(conv_id)})
    
    mock_conv_service.list.return_value = []
    
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_conversation_service] = lambda: mock_conv_service
    
    response = client.get(f"/v1/conversations?limit=20&cursor={opaque_cursor}")
    
    app.dependency_overrides.clear()
    
    assert response.status_code == 200
    data = response.json()
    assert data["items"] == []
    assert data["has_more"] is False
    assert data["next_cursor"] is None

def test_api_send_message(mock_user, mock_services):
    mock_conv_service, mock_msg_service = mock_services
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    mock_conv_service.repo.get.return_value = Conversation(
        conversation_id=conv_id,
        user_id=mock_user.id,
        title="My Conv",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    mock_msg_service.send.return_value = Message(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Hello AI",
        created_at=now,
        status=MessageStatus.SENT
    )
    
    app.dependency_overrides[get_current_user] = lambda: mock_user
    app.dependency_overrides[get_conversation_service] = lambda: mock_conv_service
    app.dependency_overrides[get_message_service] = lambda: mock_msg_service
    
    response = client.post(
        f"/v1/conversations/{conv_id}/messages",
        json={"content": "Hello AI"}
    )
    
    app.dependency_overrides.clear()
    
    assert response.status_code == 202
    data = response.json()
    assert data["content"] == "Hello AI"
    assert data["sender"] == "user"
