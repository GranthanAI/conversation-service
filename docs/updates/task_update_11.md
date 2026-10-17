# Task Update 11 — Kafka-Only LLM Service Integration (gRPC Removal)

This document logs the removal of the dead gRPC generation client and the wiring of the real Kafka round-trip to the LLM Service, replacing the typewriter-simulation fallback that stood in for it.

---

## 1. Background

Conversation Service was originally built expecting a **gRPC server-streaming** `GenerationService.Generate` RPC from the LLM Service (`app/clients/grpc_client.py`, `protos/generation.proto`). That server was never implemented on the LLM Service side — its HLD v2.0 (Section 24) mandates **Kafka only** for all Conversation Service ↔ LLM Service communication, with gRPC reserved exclusively for the LLM Service's own outbound calls to Memory/Graph/Retrieval. As a result, every user message fell through to a typewriter-simulation fallback (`MessageService.simulate_generation_pipeline`) instead of ever reaching a real model.

Separately, the Kafka consumer (`app/events/consumers.py`) only understood a topic named `chat.response.completed` wrapped in the service's generic envelope (`{event_id, event_type, payload}`) — but the LLM Service actually publishes flat (envelope-less) JSON to `chat.response.chunk` and `chat.response.generated`. The two services were never actually able to talk to each other.

This task removes the non-existent gRPC dependency entirely and wires the real Kafka contract on both the producer and consumer sides.

---

## 2. Changes

### 2.1 gRPC Removal
Deleted entirely (nothing else in the service used gRPC):
- `app/clients/grpc_client.py`, `app/db/grpc.py` (`GRPCClientManager`)
- `app/events/generation_pb2.py`, `generation_pb2_grpc.py`, `protos/generation.proto`
- `MessageService.simulate_generation_pipeline` (gRPC call + dev-mode typewriter fallback + production DLQ path)
- `grpcio`/`grpcio-tools` from `requirements.txt`; `LLM_SERVICE_GRPC_ENDPOINT` / `GRPC_*` settings from `app/core/config.py` and `.env.example`
- `grpc_manager` health checks removed from both `/ready` (root) and `/v1/health/ready` endpoints

### 2.2 Kafka Producer — `chat.message.created`
`MessageService._build_chat_message_created_payload()` now builds a **flat** event (no envelope) matching the LLM Service's `ChatMessageCreatedEvent` schema exactly: `message_id`, `conversation_id`, `user_id`, `content`, `mode_hint`, `selected_tools`, `file_ids`, `trace_context`, `timestamp`. Still written through the existing Transactional Outbox (`create_with_outbox`) — only the JSON shape inside `outbox_payload` changed. `MessageService.send()` and `.regenerate()` now accept optional `mode` / `selected_tools` / `file_ids`, threaded from `CreateMessageRequest` (`app/schemas/message.py`) through `POST /conversations/{id}/messages`.

### 2.3 Kafka Consumer — `chat.response.chunk` / `chat.response.generated`
`app/events/topics.py`: replaced `CHAT_RESPONSE_COMPLETED` with `CHAT_RESPONSE_CHUNK` / `CHAT_RESPONSE_GENERATED`, matching what the LLM Service actually publishes. `app/db/kafka.py` now subscribes to both.

`app/events/consumers.py` gained two dedicated branches (flat-shape parsing, no envelope unwrap, since these events carry no top-level `event_id`):
- **`chat.response.chunk`** → forwards the token to `StreamService.publish_token()` (existing Redis PubSub → SSE bridge, unchanged), deduplicated via a UUID5 derived from `message_id:sequence_number`.
- **`chat.response.generated`** → on `status="success"`, calls the existing `MessageService.finalize_assistant_message()` (Cassandra persist + cache invalidation, unchanged); on `status="error"`, marks the message `failed` and pushes a terminal SSE error event so the frontend's stream loop terminates cleanly. Deduplicated via a UUID5 derived from `message_id`.

### 2.4 Pending-Response Timeout Worker (new safety net)
Removing the gRPC fallback also removed the only mechanism that prevented a message from staying `pending` forever if the LLM Service never responds. `MessageService._register_pending_response()` now writes a `pending_response:{message_id}` Redis key (TTL) plus a sorted-set entry on every user message sent. A new `app/workers/pending_timeout_worker.py` polls that sorted set and, for any entry past its deadline, marks the message `failed`, invalidates the cache, and pushes a terminal SSE error event — mirroring the shape of the existing `cleanup_worker.py`. Wired into `app/main.py`'s lifespan alongside the other background workers.

### 2.5 Shared Infra Consolidation
Cassandra/Redis/Kafka definitions moved out of this service's local `docker-compose.yml` (deleted) into a new root-level `../docker-compose.yml` shared across all services (also now includes Neo4j and the Milvus trio). `Makefile`'s `infra-up`/`infra-down`/`infra-status`/`infra-logs-kafka` targets now run against that root compose file.

---

## 3. Verification

### 3.1 Unit Tests
```bash
uv run pytest tests/unit -v
```
```text
collected 57 items

tests/unit/test_consumers.py::test_consumer_loop_processes_title_generated PASSED
tests/unit/test_consumers.py::test_consumer_loop_processes_chat_response_generated_success PASSED
tests/unit/test_consumers.py::test_consumer_loop_processes_chat_response_generated_error PASSED
tests/unit/test_consumers.py::test_consumer_loop_forwards_chat_response_chunk PASSED
tests/unit/test_consumers.py::test_consumer_loop_deduplicates_chat_response_chunk PASSED
tests/unit/test_consumers.py::test_consumer_loop_deduplicates_events PASSED
... (51 other pre-existing tests unaffected)

======================== 57 passed, 1 warning in 3.66s ========================
```

New/updated coverage: successful `chat.response.generated` finalizes the message via `MessageService.finalize_assistant_message`; error-status `chat.response.generated` marks the message `failed` and surfaces a terminal SSE event; `chat.response.chunk` forwards to `StreamService.publish_token`; duplicate chunk/generated events are deduplicated via the inbox pattern.

### 3.2 What Remains Manual
End-to-end verification against a live LLM Service (real Kafka round-trip, SSE delivery of real generated tokens, pending-timeout worker firing when LLM Service is down) requires the shared infra stack (`make infra-up`) and both services running — not exercised by the unit suite above.

---

## 4. What Was NOT Changed

- Transactional Outbox, idempotency (Inbox pattern), Cassandra schema, Redis Cache-Aside, Redis PubSub → SSE bridge — all unchanged, per the constraint that this was an integration fix, not a redesign.
- No new gRPC calls were introduced anywhere in this service.
