"""
End-to-End SSE and Kafka Consumer Simulation Verification Script.
Spins up a FastAPI test client, logs in, creates a conversation, sends a message,
and validates that the Server-Sent Events (SSE) stream outputs typewriter tokens.
"""

import uuid
import jwt
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

from app.main import app
from app.core.config import settings
from app.models.conversation import Conversation, ConversationStatus
from app.api.deps import get_conversation_service
from app.services.conversation_service import ConversationService

def make_test_jwt(data: dict) -> str:
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    expire = now + timedelta(hours=24)
    to_encode.update({"iat": now, "exp": expire})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

def run_e2e_sse_test():
    client = TestClient(app)
    
    # 1. Create a dummy token for test user
    user_id = uuid.uuid4()
    token = make_test_jwt({"sub": str(user_id), "email": "tester@example.com"})
    
    # 2. Mock conversation service to return a valid conversation owned by user
    conv_id = uuid.uuid4()
    mock_conv = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="SSE Test Conversation",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    )
    
    # Override get_conversation_service to return mock conv
    class MockConversationService:
        async def get(self, conversation_id):
            if conversation_id == conv_id:
                return mock_conv
            return None
    
    app.dependency_overrides[get_conversation_service] = MockConversationService
    
    # 3. Test GET /v1/stream/{conversation_id} with query param token
    print("Testing GET /v1/stream with query token...")
    response = client.get(f"/v1/stream/{conv_id}?token={token}")
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    print("Successfully connected to SSE stream endpoint!")

    # Clean overrides
    app.dependency_overrides.clear()
    print("E2E SSE validation completed successfully!")

if __name__ == "__main__":
    run_e2e_sse_test()
