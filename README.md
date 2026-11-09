# GraphGPT Conversation Service

Central orchestration microservice built with FastAPI. It manages conversational sessions, user/assistant message logs, and Server-Sent Event (SSE) token delivery pipelines for the GraphGPT platform.

---

## Key Architectural Decisions

1. **Clean Architecture Module Structure**: Isolates presentation routers, database connection drivers, models, schemas, and orchestrators into explicit layers.
2. **Linear Persistence Scale (Apache Cassandra)**: Retains conversation catalogs and message tables natively using partition indexes.
3. **Low Latency Cache-Aside (Redis)**: Buffers conversation listings and session metadata.
4. **Resilient Eventual Consistency (Kafka Transactional Outbox)**: Guarantees zero-loss delivery of business events (e.g. `chat.message.created`) using local outbox tables.
5. **Fast Client Streams (SSE & gRPC)**: Forwards generated LLM token streams over Server-Sent Events dynamically to client browsers.

---

## Directory Layout

```text
conversation-service/
│
├── app/
│   ├── api/                 # Presentation Layer
│   │   ├── deps.py          # Route dependecy hooks (authorization checks)
│   │   ├── router.py        # Central router mapping versioned paths
│   │   └── v1/              # Endpoint modules (conversations, messages, stream, health)
│   │
│   ├── core/                # Core Configs
│   │   ├── config.py        # Pydantic Settings env loader
│   │   ├── security.py      # Encryption & JWT verification helpers
│   │   ├── logging.py       # Structlog json log formatter
│   │   └── constants.py     # System boundaries static values
│   │
│   ├── db/                  # Connection Drivers
│   │   ├── cassandra.py     # Cassandra driver setup
│   │   ├── redis.py         # Redis driver setup
│   │   ├── kafka.py         # Kafka driver setup
│   │   └── grpc.py          # gRPC client driver setup
│   │
│   ├── models/              # Pure Domain Entities
│   │
│   ├── schemas/             # Pydantic Request/Response DTO Validators
│   │
│   ├── repositories/        # Database Access Adapters
│   │
│   ├── services/            # Business Logic Orchestrations
│   │
│   ├── clients/             # SDK Client Wrappers (aiokafka, grpc connection stub)
│   │
│   ├── workers/             # Outbox, summary, title extraction background workers
│   │
│   ├── middleware/          # FastAPI middleware interceptors (auth, rate limits)
│   │
│   ├── events/              # Event definitions and consumer loops
│   │
│   └── main.py              # FastAPI Application Entrypoint
│
├── tests/                   # Pytest suites (unit, integration, load)
├── scripts/                 # Migration and seeding scripts
├── Makefile                 # Platfrom independent execution scripts
└── Dockerfile               # Multi-stage release Dockerfile
```

---

## Quickstart Runbook

Follow these commands to configure and run the service locally:

### 1. Prerequisites
Ensure you have the following installed on your machine:
*   [Python 3.12](https://www.python.org/downloads/) or higher
*   [Docker Desktop](https://www.docker.com/products/docker-desktop/)
*   [uv](https://github.com/astral-sh/uv) (fast Python package installer)

### 2. Install & Configure
Clone the `.env` template and set up your python virtual environment with required libraries:
```bash
make setup
```

### 3. Start Database Services
Spin up Redis, Kafka, and Cassandra in Docker:
```bash
make infra
```

### 4. Create Cassandra Schemas
Apply CQL table indices and keyspace schemas:
```bash
make schema
```

### 5. Launch FastAPI Service
Run the development reload server:
```bash
make dev
```
The server will start listening at `http://localhost:8000`. You can inspect endpoints interactive documentation at `http://localhost:8000/docs`.

---

## Health Check Probes

*   **Liveness Check (`GET /live`)**:
    Returns `200 OK` if the process is active.
    ```json
    {
      "status": "UP",
      "message": "Service process is alive."
    }
    ```
*   **Readiness Check (`GET /ready`)**:
    Validates socket connection status of Cassandra and Redis database brokers. Returns `503 Service Unavailable` if any connection is offline.
    ```json
    {
      "status": "UP",
      "details": {
        "cassandra": "UP",
        "redis": "UP",
        "kafka": "UP",
        "grpc": "DOWN"
      }
    }
    ```

---

## Operational Makefile Target Commands

| Command | Action |
|---|---|
| `make setup` | Resolves virtual environment and creates `.env` parameters. |
| `make infra` | Starts Cassandra, Redis, and Kafka in the background. |
| `make schema` | Applies database tables to Cassandra. |
| `make dev` | Launches FastAPI reload development server on port 8000. |
| `make test-unit` | Runs the isolated Pytest unit tests suite. |
| `make test-integration` | Runs the Docker services integration tests suite. |
| `make test-api` | Runs the API controller / HTTP router tests suite. |
| `make test-load` | Runs the 50-user concurrent async load runner scenario. |
| `make test-all` | Runs the entire combined test suite altogether. |
| `make clean` | Purges compiler output caches recursively. |
| `make docker-build` | Builds the application release Docker container image. |
| `make docker-run` | Runs the application container locally on port 8000. |
| `make kafka-log-conv-created` | Streams the `conversation.created` Kafka topic. |
| `make kafka-log-conv-updated` | Streams the `conversation.updated` Kafka topic. |
| `make kafka-log-conv-deleted` | Streams the `conversation.deleted` Kafka topic. |
| `make kafka-log-msg-created` | Streams the `chat.message.created` Kafka topic. |

---

## Tailing Kafka Logs (Local Testing)
To view messages produced to Kafka in real-time when executing requests on Swagger UI (`http://localhost:8000/docs`), open a new terminal window and run:

```bash
# Using Makefile:
make kafka-log-msg-created

# Or directly using Docker CLI:
docker exec -it graphgpt-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic chat.message.created --from-beginning
```

## Billing integration (plans, limits, model choice)

This service is the main enforcement point for message limits (integration plan `billing-service/docs/Integrations`, phase P6).
Everything below is **off by default**: with `BILLING_ENFORCEMENT=off` the service behaves exactly as before (the
`chat.message.created` payload differs only in `schema_version: "2.4"`; the `billing` block is omitted).

### Settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `BILLING_GRPC_TARGET` | `127.0.0.1:50061` | billing `BillingInternal` gRPC (use `127.0.0.1`, not `localhost`, on Windows) |
| `BILLING_SERVICE_TOKEN` | empty | this service's token (`authorization: Bearer ...`), printed by billing's `dev_up.sh` |
| `BILLING_ENFORCEMENT` | `off` | `off` \| `shadow` \| `enforce` |
| `ENTITLEMENTS_CACHE_SECONDS` | `60` | per-user entitlement cache in Redis (`ent:{user_id}`) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | **on Windows set `127.0.0.1:9092`** |

### Enforcement modes and rollout order

1. **`off`** (default): no billing call, no service-owned plan limits, no `billing` block.
2. **`shadow`**: every send / regenerate calls `CheckAndReserve`, logs `billing_shadow_decision{decision,reason}` and **always
   proceeds**. An allow carries the grant (`enforcement: "shadow"`); a deny or failure carries a block with `hold_id` and
   `granted_group` null, so llm-service keeps its legacy routing. Service-owned limits are decided and logged
   (`plan_limit_shadow_decision`) but never refused. Run here until the decisions and billing's usage rows look right.
3. **`enforce`**: a deny is returned to the user and **nothing is persisted**; service-owned limits refuse with 403.

Order of one send (strict): resolve preferences and model, service-owned limits (no side effects), reserve with billing,
persist + outbox, and on any failure after the reserve `Release(reason="send_failed")` then re-raise. A regenerate reserves
too, under a new request id. Denied responses are not stored in the idempotency cache, and an idempotent replay of an
allowed send never reserves twice.

Billing unreachable in `enforce` (breaker open: 5 consecutive failures or timeouts, half-open after 30 s): **degraded mode**
allows Auto or a Fast model only, at most 10 messages per user per IST day (Redis `billing:degraded:{uid}:{yyyymmdd}`,
48 h TTL); an explicit non-Fast model or the 11th message gets `503 billing_unavailable`. Entitlements fall back to the
last cached value for up to 1 hour, then to the bundled Free limits (`app/core/plan_defaults.py`, kept equal to billing's
`plans.yaml` by a test).

### Errors

Billing denies: `429 limit.daily`, `429 limit.monthly`, `403 plan.model_not_included`, `403 account.blocked`,
`402 credits.empty`, `402 credits.spend_cap`, `402 credits.member_cap`, `503 billing.degraded`, as
`{"detail": {"code": "billing_denied", "reason": ..., "message_key": ..., "message": ..., "upgrade_to": ...}}`.
Service-owned limits: `403 {"detail": {"code": "billing_denied", "reason": "plan_limit", "limit": "<name>", "message_key": "limit.<name>", ...}}`.
English copy lives in `app/core/billing_messages.py`.

### Limits this service enforces (`-1` = unlimited; never deletes or hides data)

| Limit | Rule |
| --- | --- |
| `messages_per_conversation` | USER messages in the conversation's own turns (inherited branch history excluded; regenerate adds none). A Postgres counter row per conversation, initialised lazily from one Cassandra count, incremented in the send transaction |
| `branches_per_tree` | a branch is a conversation with a `parent_conversation_id`; the tree's existing non-deleted branches are counted when one is created |
| `documents_per_tree` | distinct document ids attached anywhere in the tree (`tree_documents`); a send is refused when it attaches new documents that would exceed the limit |
| `custom_modes` | active (non-archived) custom modes, checked atomically on create |
| `mode_instruction_chars` | instruction length on custom-mode create/update and on `personal_instructions` |

When a plan shrinks, existing content stays readable and editable; only creating more is refused.

### Model choice

`POST .../messages` and `POST .../messages/{id}/regenerate` accept an optional `model` (`"auto"` or a catalog model id,
`[a-z0-9./-]{1,64}`, format-checked only). Without it the saved `default_model` (preferences, default `"auto"`) applies.
An explicit model that billing refuses is returned to the user; a refused **saved default** is retried once as Auto and
`defaults_applied.model` is `true`. The answering model is stored on the assistant message (`model`, `model_group`) from
llm-service's 2.2 events and shown in history and the final SSE event; `usage.threshold_reached` from billing becomes an
SSE `notice` event on the user's open streams.

### Stubs, schema and metrics

- Billing proto: vendored in `proto/billing/` (commit hash in its header). Regenerate the stubs with `make proto`, or on
  Windows without make: `.venv\Scripts\python scripts\gen_billing_proto.py`.
- Apply schema changes before deploying: `make schema` (adds nullable `model`, `model_group` columns; idempotent) and
  `make db-migrate` (Alembic 0005 `default_model`, 0006 counters and `tree_documents`).
- `GET /metrics` (Prometheus text): `billing_reserve_total{mode,decision,reason}`, `billing_reserve_seconds`,
  `billing_breaker_open`, `billing_degraded_messages_total`, `billing_release_total{reason}`, `plan_limit_denied_total{limit}`.
