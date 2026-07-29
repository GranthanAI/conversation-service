"""
API CRUD Endpoint Tests.
Verifies REST controllers for conversation and message logs management.
"""

import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone, timedelta
import jwt

from app.main import app
from app.core.config import settings

def generate_valid_token(user_id: uuid.UUID) -> str:
    payload = {
        "sub": str(user_id),
        "email": "api_test@example.com",
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=1)
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

@pytest.mark.anyio
async def test_conversation_lifecycle_api():
    user_id = uuid.uuid4()
    token = generate_valid_token(user_id)
    headers = {"Authorization": f"Bearer {token}"}
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # 1. Create Conversation
        conv_payload = {"title": "API Test CRUD"}
        res = await client.post("/v1/conversations", json=conv_payload, headers=headers)
        assert res.status_code == 201
        conv = res.json()
        conv_id = conv["conversation_id"]
        assert conv["title"] == "API Test CRUD"
        assert conv["status"] == "active"
        
        # 2. Rename Conversation
        rename_payload = {"title": "API Test Renamed"}
        res = await client.patch(f"/v1/conversations/{conv_id}/rename", json=rename_payload, headers=headers)
        assert res.status_code == 200
        assert res.json()["title"] == "API Test Renamed"
        
        # 3. Post a message to conversation
        msg_payload = {"content": "Hello E2E endpoint"}
        msg_headers = {**headers, "X-Idempotency-Key": str(uuid.uuid4())}
        res = await client.post(f"/v1/conversations/{conv_id}/messages", json=msg_payload, headers=msg_headers)
        assert res.status_code == 202
        assert res.json()["sender"] == "user"
        
        # 4. List messages history
        res = await client.get(f"/v1/conversations/{conv_id}/messages", headers=headers)
        assert res.status_code == 200
        history = res.json()["items"]
        assert len(history) >= 1
        assert history[0]["content"] == "Hello E2E endpoint"
        
        # 5. Delete Conversation (Soft Delete)
        res = await client.delete(f"/v1/conversations/{conv_id}", headers=headers)
        assert res.status_code == 204
        
        # 6. List conversations should exclude the deleted one
        res = await client.get("/v1/conversations", headers=headers)
        assert res.status_code == 200
        items = res.json()["items"]
        assert all(item["conversation_id"] != conv_id for item in items)
