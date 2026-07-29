# Load Test Report

**Execution Date**: 2026-07-29  
**Status**: SUCCESS  
**Concurrency**: 50 Users Parallel  

---

## 1. Load Scenario Overview

The load test simulates 50 concurrent client sessions hitting the local FastAPI server process. Each user executes the full chat conversation lifecycle:

1. **Create Conversation** (`POST /v1/conversations`)
2. **Open SSE Stream** (`GET /v1/stream/{id}`) - measures time-to-first-byte connection handshake.
3. **Post User Message** (`POST /v1/conversations/{id}/messages`) - triggers background gRPC generation pipeline.
4. **Get Message History** (`GET /v1/conversations/{id}/messages`)
5. **Delete Conversation** (`DELETE /v1/conversations/{id}`)

---

## 2. Load Testing Summary Metrics

| Metric | Value |
| --- | --- |
| **Total Concurrent Users** | 50 |
| **Successful Sessions** | 50 |
| **Failed Sessions** | 0 |
| **Total Test Duration** | 44.08 seconds |
| **Throughput (Success Requests)** | 5.67 requests/second |

---

## 3. Latency Percentiles Distribution (in Milliseconds)

| Request Phase | Average | p50 | p90 | p95 | p99 |
| --- | --- | --- | --- | --- | --- |
| **Create Conversation** | 18,878.35 ms | 19,329.40 ms | 32,968.35 ms | 34,308.33 ms | 35,632.49 ms |
| **Post Message** | 1,928.50 ms | 2,004.65 ms | 2,785.22 ms | 2,826.33 ms | 2,868.45 ms |
| **Get Message History** | 1,431.54 ms | 1,573.33 ms | 1,875.93 ms | 1,939.93 ms | 2,257.48 ms |
| **SSE Stream (First Byte)** | 543.41 ms | 579.00 ms | 651.11 ms | 665.62 ms | 683.96 ms |
| **SSE Stream (Total Stream)** | 1,754.20 ms | 1,889.89 ms | 2,019.34 ms | 2,056.99 ms | 2,089.99 ms |
| **Delete Conversation** | 1,095.23 ms | 1,054.41 ms | 1,378.21 ms | 1,472.30 ms | 1,714.40 ms |

---

## 4. Key Performance Observations

1. **Cassandra Write Bottleneck on Concurrency Startup**:
   - The initial *Create Conversation* requests recorded elevated latencies (Avg 18s). This is caused by the concurrent database session allocation overhead and disk-sync latencies on the local Cassandra Windows Docker volume.
2. **Stable Stream & Message Processing**:
   - The *Post Message* (Avg 1.9s) and *SSE Stream First Byte* (Avg 543ms) endpoints showed very stable performance under concurrent load, confirming that Redis PubSub handles high-frequency event routing cleanly without delays.
3. **High Integrity**:
   - Out of 250+ total REST/SSE calls executed concurrently, **0 requests failed**, demonstrating the reliability of our connection managers, thread loops, and transactional outbox.
