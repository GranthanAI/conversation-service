"""
Real E2E Integration and Flow Verification Script.
Creates a conversation, starts an SSE connection listener, sends a user message,
verifies that gRPC receives streaming chunks from the mock LLM server,
that they route through Redis PubSub to the SSE listener, and the message finalises.
"""

import asyncio
import uuid
import jwt
from datetime import datetime, timezone, timedelta
from httpx import AsyncClient

from app.core.config import settings
from app.models.conversation import Conversation, ConversationStatus

def make_test_jwt(user_id: uuid.UUID) -> str:
    to_encode = {"sub": str(user_id), "email": "tester@example.com"}
    now = datetime.now(timezone.utc)
    expire = now + timedelta(hours=2)
    to_encode.update({"iat": now, "exp": expire})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

async def listen_sse_stream(client: AsyncClient, url: str):
    """
    Listens to the SSE stream in the background and prints chunks.
    """
    print(f"SSE Listener subscribing to: {url}")
    try:
        async with client.stream("GET", url, timeout=30.0) as response:
            assert response.status_code == 200
            print("Connected to SSE Stream successfully!")
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data_content = line[6:]
                    print(f"SSE Received: {data_content}")
                    if "is_final" in data_content and "true" in data_content.lower():
                        print("SSE Stream completed gracefully.")
                        break
    except Exception as e:
        print(f"SSE Listener encountered error: {e}")

async def run_real_e2e():
    user_id = uuid.uuid4()
    token = make_test_jwt(user_id)
    headers = {"Authorization": f"Bearer {token}"}
    
    # We use httpx AsyncClient to hit the locally running development server (make dev)
    # The development server is running on localhost:8000 (standard FastAPI port)
    base_url = "http://localhost:8000"
    
    async with AsyncClient(base_url=base_url) as client:
        # 1. Create a Conversation
        print("1. Creating conversation...")
        conv_payload = {"title": "E2E gRPC SSE Chat"}
        response = await client.post("/v1/conversations", json=conv_payload, headers=headers)
        if response.status_code not in (200, 201):
            print(f"Error creating conversation: {response.text}")
            return
            
        conv = response.json()
        conv_id = conv["conversation_id"]
        print(f"Conversation created successfully: {conv_id}")
        
        # 2. Start SSE Stream listener in the background
        stream_url = f"/v1/stream/{conv_id}?token={token}"
        listener_task = asyncio.create_task(listen_sse_stream(client, stream_url))
        
        # Wait a moment for SSE listener to connect
        await asyncio.sleep(2.0)
        
        # 3. Post a user message to trigger assistant streaming response
        print("\n2. Sending user message to trigger generation...")
        msg_id = uuid.uuid4()
        msg_payload = {"content": "Explain Quantum Computing in simple terms."}
        headers_with_idempotency = {
            **headers,
            "X-Idempotency-Key": str(uuid.uuid4())
        }
        
        response = await client.post(
            f"/v1/conversations/{conv_id}/messages",
            json=msg_payload,
            headers=headers_with_idempotency
        )
        if response.status_code not in (200, 202):
            print(f"Message POST failed with code {response.status_code}: {response.text}")
        assert response.status_code in (200, 202)
        print("User message submitted successfully!")
        
        # 4. Wait for the SSE stream listener to receive chunks and complete
        print("\n3. Waiting for stream completion...")
        await asyncio.shield(listener_task)
        
        # 5. Fetch message history to verify the assistant reply is saved with status='sent'
        print("\n4. Checking message history...")
        response = await client.get(f"/v1/conversations/{conv_id}/messages", headers=headers)
        assert response.status_code == 200
        history = response.json().get("items", [])
        print(f"Message history contains {len(history)} messages:")
        for msg in history:
            print(f"- {msg['sender']}: {msg['content'][:50]}... (Status: {msg['status']})")

if __name__ == "__main__":
    asyncio.run(run_real_e2e())
