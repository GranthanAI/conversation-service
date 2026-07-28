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
| `make clean` | Purges compiler output caches recursively. |
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
