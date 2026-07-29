Your HLD is already solid. Below is the **updated version** with the additional improvements integrated, including **`conversation_status`** in the event contract and making the event **self-contained**.

---

# 1. Responsibilities (Update)

### Existing

* Manage conversation lifecycle
* Persist conversation metadata
* Persist messages
* Stream responses
* Publish domain events

### Add

* Capture conversation lineage metadata (`parent_conversation_id`) during conversation creation.
* Publish conversation lineage and lifecycle metadata as immutable Kafka domain events for downstream Graph Service consumption.
* Remain the single source of truth for conversation metadata while Graph Service owns all graph relationships.

---

# 2. Functional Requirements (Add)

## Conversation Branching

The Conversation Service shall support creating a conversation from an existing conversation.

Requirements:

* Root conversations have no parent.
* Branched conversations reference exactly one parent conversation.
* Validate that the parent conversation exists and is accessible to the requesting user.
* Persist the parent reference.
* Publish the parent reference through Kafka.
* Conversation lineage metadata shall remain immutable after creation.

---

# 3. Conversation Metadata (Update)

Add a new metadata field.

| Field                    | Type | Required | Description                                                                                         |
| ------------------------ | ---- | -------- | --------------------------------------------------------------------------------------------------- |
| `parent_conversation_id` | UUID | No       | Parent conversation from which this conversation was created. `NULL` indicates a root conversation. |

---

# 4. API Changes

## Update Create Conversation API

Current

```http
POST /conversations
```

Request

```json
{
  "title": "AI Assistant"
}
```

Updated

```json
{
  "title": "AI Assistant",
  "parent_conversation_id": "uuid"
}
```

### Notes

* Optional field.
* `NULL` indicates a root conversation.
* Used only when creating a branched conversation.

---

# 5. Validation Rules (Add)

When `parent_conversation_id` is supplied:

* Parent conversation must exist.
* Parent conversation must not be deleted or archived.
* Parent conversation must belong to the authenticated user (or authorized workspace).
* Parent conversation must belong to the same tenant/workspace (if multi-tenant).

Reject invalid requests with:

* **400** – Invalid UUID
* **403** – Unauthorized
* **404** – Parent conversation not found

---

# 6. Domain Events (Update)

The Conversation Service publishes immutable domain events through the Transactional Outbox.

All conversation lifecycle events follow a common event envelope.

## `conversation.created`

```json
{
  "event_id": "uuid",
  "event_type": "conversation.created",
  "event_version": 1,
  "conversation_id": "uuid",
  "parent_conversation_id": "uuid | null",
  "conversation_status": "ACTIVE",
  "user_id": "uuid",
  "created_at": "2026-07-29T10:30:00Z",
  "trace_id": "uuid",
  "correlation_id": "uuid"
}
```

For root conversations

```json
{
  "parent_conversation_id": null
}
```

### Event Design Principles

* Events are immutable.
* Events are versioned.
* Events are self-contained.
* Downstream services must not query Conversation Service to understand conversation state.
* `parent_conversation_id` enables Graph Service to reconstruct conversation lineage.
* `conversation_status` enables downstream services to process lifecycle changes (`ACTIVE`, `ARCHIVED`, `DELETED`, `RESTORED`) without additional lookups.
* Future lifecycle events (`conversation.deleted`, `conversation.archived`, `conversation.restored`) follow the same event contract.

---

# 7. Event Consumers (Update)

```text
Conversation Service
        │
Transactional Outbox
        │
Kafka
   ┌────┴─────────────────────┐
   │                          │
LLM Service             Graph Service
```

Graph Service consumes:

* `conversation.created`
* `conversation.deleted`
* `conversation.archived`
* `conversation.restored`

---

# 8. Architecture Diagram (Update)

```text
Client
      │
POST /conversations
(parent_conversation_id)
      │
      ▼
Conversation Service
      │
Persist Conversation
(parent_conversation_id)
      │
Transactional Outbox
      │
Kafka
      │
Graph Service
      │
Creates

(:Conversation)

(parent)-[:HAS_CHILD]->(child)

(child)-[:CREATED_FROM]->(parent)
```

---

# 9. Data Flow (Update)

## Branched Conversation Creation

```text
Client
      │
Create Conversation
(parent_conversation_id)
      │
      ▼
Conversation Service
      │
Validate Parent
      │
Persist Conversation
      │
Write Transactional Outbox Event
      │
Kafka
      │
Graph Service
      │
Create Conversation Node
      │
Create HAS_CHILD Relationship
      │
Create CREATED_FROM Relationship
```

---

# 10. Non-Functional Requirements (Add)

* Conversation lineage metadata shall be immutable after conversation creation.
* Conversation creation and event publication shall remain atomic through the Transactional Outbox pattern.
* Graph Service synchronization shall rely exclusively on Kafka events.
* No synchronous dependency shall exist between Conversation Service and Graph Service.
* Domain events shall be immutable, versioned, and self-contained.

---

# 11. Service Boundaries (Update)

## Conversation Service Owns

* Conversation metadata
* Conversation lifecycle
* Parent conversation reference (`parent_conversation_id`)
* Conversation status
* Event publication

## Graph Service Owns

* Conversation graph
* Parent-child relationships
* Graph traversal
* Branch visualization
* Relationship queries

---

# 12. HLD Notes (Add)

> The Conversation Service stores only the immediate parent reference (`parent_conversation_id`) required to establish conversation lineage. It does not manage graph relationships or graph traversal. The Graph Service asynchronously consumes immutable Kafka domain events to construct and maintain the Neo4j graph. Domain events are self-contained and include both lineage metadata (`parent_conversation_id`) and lifecycle metadata (`conversation_status`), enabling downstream services to process conversation state changes without synchronous dependencies on the Conversation Service.

---

### Why `conversation_status` is a good addition

Adding `conversation_status` makes every event **self-describing**. Consumers like the Graph Service, Analytics Service, Search Service, or Audit Service can react to conversation lifecycle changes without making synchronous calls back to the Conversation Service. This preserves loose coupling and aligns with event-driven architecture best practices.
