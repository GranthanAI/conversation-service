"""
Async Local E2E Load Testing Client.
Simulates concurrent users executing CRUD flows, posting messages, listing logs, and streaming SSE tokens.
Reports Average, p50, p90, p95, and p99 latency distributions, throughput, and error rates.
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone, timedelta
import jwt
from httpx import AsyncClient

from app.core.config import settings

# Results storage
metrics = {
    "create_conv": [],
    "post_message": [],
    "get_history": [],
    "sse_stream_first_byte": [],
    "sse_stream_total": [],
    "delete_conv": [],
    "errors": 0,
    "success": 0
}

def generate_user_token(user_id: uuid.UUID) -> str:
    payload = {
        "sub": str(user_id),
        "email": f"load_user_{user_id.hex[:6]}@example.com",
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=2)
    }
    return jwt.encode(payload, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

async def simulate_user_session(user_idx: int, base_url: str):
    """
    Simulates a complete user session lifecycle:
    Create conv -> Post message -> Listen to SSE stream -> Fetch history -> Delete conv
    """
    user_id = uuid.uuid4()
    token = generate_user_token(user_id)
    headers = {"Authorization": f"Bearer {token}"}
    
    async with AsyncClient(base_url=base_url, timeout=30.0) as client:
        try:
            # 1. Create Conversation
            start = time.perf_counter()
            res = await client.post("/v1/conversations", json={"title": f"Load test user {user_idx}"}, headers=headers)
            latency = time.perf_counter() - start
            if res.status_code != 201:
                metrics["errors"] += 1
                return
            metrics["create_conv"].append(latency)
            conv = res.json()
            conv_id = conv["conversation_id"]
            
            # 2. Start listening to SSE stream (we read first byte then stop to simulate connect time)
            start_sse = time.perf_counter()
            stream_url = f"/v1/stream/{conv_id}?token={token}"
            async with client.stream("GET", stream_url) as sse_res:
                if sse_res.status_code != 200:
                    metrics["errors"] += 1
                    return
                first_byte_latency = time.perf_counter() - start_sse
                metrics["sse_stream_first_byte"].append(first_byte_latency)
                
                # Consume a few lines
                lines_read = 0
                async for line in sse_res.aiter_lines():
                    lines_read += 1
                    if lines_read > 5:
                        break
                        
            metrics["sse_stream_total"].append(time.perf_counter() - start_sse)
            
            # 3. Post Message
            msg_payload = {"content": "Load test prompt query content"}
            msg_headers = {**headers, "X-Idempotency-Key": str(uuid.uuid4())}
            start = time.perf_counter()
            res = await client.post(f"/v1/conversations/{conv_id}/messages", json=msg_payload, headers=msg_headers)
            latency = time.perf_counter() - start
            if res.status_code not in (200, 202):
                metrics["errors"] += 1
                return
            metrics["post_message"].append(latency)
            
            # 4. Get History
            start = time.perf_counter()
            res = await client.get(f"/v1/conversations/{conv_id}/messages", headers=headers)
            latency = time.perf_counter() - start
            if res.status_code != 200:
                metrics["errors"] += 1
                return
            metrics["get_history"].append(latency)
            
            # 5. Delete Conversation
            start = time.perf_counter()
            res = await client.delete(f"/v1/conversations/{conv_id}", headers=headers)
            latency = time.perf_counter() - start
            if res.status_code != 204:
                metrics["errors"] += 1
                return
            metrics["delete_conv"].append(latency)
            
            metrics["success"] += 1
            
        except Exception as e:
            metrics["errors"] += 1

def print_percentiles(name: str, latencies: list):
    if not latencies:
        print(f"  {name}: No data")
        return
    l_ms = [l * 1000.0 for l in latencies]
    sorted_l = sorted(l_ms)
    n = len(sorted_l)
    
    avg = sum(sorted_l) / n
    p50 = sorted_l[int(n * 0.50)]
    p90 = sorted_l[int(n * 0.90)] if n > 1 else sorted_l[0]
    p95 = sorted_l[int(n * 0.95)] if n > 1 else sorted_l[0]
    p99 = sorted_l[int(n * 0.99)] if n > 1 else sorted_l[0]
    
    print(f"  {name:25} | Avg: {avg:6.2f}ms | p50: {p50:6.2f}ms | p90: {p90:6.2f}ms | p95: {p95:6.2f}ms | p99: {p99:6.2f}ms")

async def main():
    base_url = "http://localhost:8000"
    concurrency = 50
    print(f"Starting Load Test simulating {concurrency} concurrent clients against: {base_url}...")
    
    start_time = time.perf_counter()
    tasks = [simulate_user_session(i, base_url) for i in range(concurrency)]
    await asyncio.gather(*tasks)
    total_duration = time.perf_counter() - start_time
    
    print("\n========================================================================")
    print("GraphGPT Load Test Results")
    print("========================================================================")
    print(f"Total Users Simulated:  {concurrency}")
    print(f"Successful Sessions:    {metrics['success']}")
    print(f"Failed Sessions:        {metrics['errors']}")
    print(f"Total Run Duration:     {total_duration:.2f} seconds")
    
    # Calculate throughput (approx 5 requests per successful session + others)
    total_reqs = (metrics["success"] * 5) + metrics["errors"]
    throughput = total_reqs / total_duration
    print(f"Request Throughput:     {throughput:.2f} requests/second")
    print("------------------------------------------------------------------------")
    print("Latency Percentiles Distribution (in Milliseconds):")
    print_percentiles("Create Conversation", metrics["create_conv"])
    print_percentiles("Post Message", metrics["post_message"])
    print_percentiles("Get Message History", metrics["get_history"])
    print_percentiles("SSE Stream First Byte", metrics["sse_stream_first_byte"])
    print_percentiles("SSE Stream Total", metrics["sse_stream_total"])
    print_percentiles("Delete Conversation", metrics["delete_conv"])
    print("========================================================================\n")

if __name__ == "__main__":
    asyncio.run(main())
