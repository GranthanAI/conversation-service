"""
API Authentication Endpoint Tests.
Verifies JWT validation, header parsing, and query parameter auth fallback.
"""

import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone, timedelta
import jwt

from app.main import app
from app.core.config import settings
from tests.api.test_crud_api import generate_valid_token

def generate_expired_token(user_id: uuid.UUID) -> str:
    payload = {
        "sub": str(user_id),
        "email": "api_test@example.com",
        "iat": datetime.now(timezone.utc) - timedelta(hours=2),
        "exp": datetime.now(timezone.utc) - timedelta(hours=1)
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

@pytest.mark.anyio
async def test_auth_header_protection():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # 1. No Authorization Header -> 401 Unauthorized
        res = await client.get("/v1/conversations")
        assert res.status_code == 401
        assert "not authenticated" in res.json()["detail"].lower()
        
        # 2. Invalid Token Signature -> 401 Unauthorized
        res = await client.get("/v1/conversations", headers={"Authorization": "Bearer badsignaturetoken"})
        assert res.status_code == 401
        
        # 3. Expired Token -> 401 Unauthorized
        expired_token = generate_expired_token(uuid.uuid4())
        res = await client.get("/v1/conversations", headers={"Authorization": f"Bearer {expired_token}"})
        assert res.status_code == 401
        assert "token has expired" in res.json()["detail"].lower()

@pytest.mark.anyio
async def test_auth_query_parameter_fallback():
    user_id = uuid.uuid4()
    token = generate_valid_token(user_id)
    conv_id = uuid.uuid4()
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # Fetching stream with query parameter auth should validate successfully
        # Wait, since the stream endpoint returns a StreamingResponse,
        # we can verify that the connection succeeds (starts yielding status 200).
        # We need to make sure we exist the conversation or mock ownership dependencies.
        # Let's mock require_conversation_owner to allow access or create a real conversation first!
        
        # Create a conversation under this user_id first
        headers = {"Authorization": f"Bearer {token}"}
        conv_res = await client.post("/v1/conversations", json={"title": "Stream Auth test"}, headers=headers)
        assert conv_res.status_code == 201
        conv_id = conv_res.json()["conversation_id"]
        
        # Query regular endpoint with token in query string
        res = await client.get(f"/v1/conversations?token={token}")
        assert res.status_code == 200
