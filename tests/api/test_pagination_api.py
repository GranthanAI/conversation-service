"""
API Pagination Endpoint Tests.
Verifies cursor-based pagination parameters on the conversation list endpoint.
"""

import uuid
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from tests.api.test_crud_api import generate_valid_token

@pytest.mark.anyio
async def test_conversation_pagination():
    user_id = uuid.uuid4()
    token = generate_valid_token(user_id)
    headers = {"Authorization": f"Bearer {token}"}
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        # 1. Create multiple conversations to paginate through
        for i in range(5):
            res = await client.post("/v1/conversations", json={"title": f"Paginated Conv {i}"}, headers=headers)
            assert res.status_code == 201
            
        # 2. Get list with limit=2
        res = await client.get("/v1/conversations?limit=2", headers=headers)
        assert res.status_code == 200
        data = res.json()
        items = data["items"]
        assert len(items) == 2
        next_cursor = data.get("next_cursor")
        assert next_cursor is not None
        
        # 3. Get next page using the cursor
        res_next = await client.get(f"/v1/conversations?limit=2&cursor={next_cursor}", headers=headers)
        assert res_next.status_code == 200
        data_next = res_next.json()
        items_next = data_next["items"]
        assert len(items_next) == 2
        
        # 4. Verify pagination returned distinct conversations
        ids = {item["conversation_id"] for item in items}
        ids_next = {item["conversation_id"] for item in items_next}
        assert ids.isdisjoint(ids_next)
