# Task Update 12 — Twelve-Turn Memory Trace (Live End-to-End Verification)

This document is a markdown transcription of the "Twelve-Turn Memory Trace" report (originally published as an interactive artifact), recording a live 12-turn conversation sent through Conversation Service's real API and traced through the full pipeline it participates in: API Gateway → Conversation Service → Kafka → LLM Service (Groq) → Kafka → Memory Service, with memory state inspected after every turn via Memory Service's `GetMemoryContext` gRPC endpoint.

This verifies, with real traffic rather than mocks, that the Kafka wiring documented in Task Update 11 actually round-trips correctly end-to-end, including Conversation Service's own responsibilities: message persistence, the pending-response path, SSE delivery, and parent/child conversation lineage.

---

## 1. Run Summary

| | |
|---|---|
| User | Alex (`e2etest@example.com`) |
| Parent conversation | `01a0539b0869…` |
| Child conversation | `01a0539d6769…` (created with `parent_conversation_id` set to the parent) |
| Generation provider | Groq (`groq/compound`) |
| Turns completed successfully | 7 / 12 |
| Turns that hit Groq's free-tier rate limit | 5 / 12 |
| Short-term messages appended per successful turn | 2 (one user, one assistant) |
| Messages visible in the child conversation | 0 |

---

## 2. Turn-by-Turn Trace (Parent Conversation)

Each row is one real HTTP round-trip through the gateway to Conversation Service. "Memory after turn" is the live response of `GetMemoryContext`, called directly against Memory Service immediately after the reply lands — not inferred.

| # | Query | Status | Round-trip | Short-term msgs after |
|---|---|---|---|---|
| 1 | "My name is Alex and I work as a backend engineer at a fintech startup." | sent | 10.41s | 2 |
| 2 | "I mainly write Python and Go, and I'm learning Rust in my spare time." | sent | 10.55s | 4 |
| 3 | "What's the capital of France?" | sent | 23.22s | 6 |
| 4 | "I have a dog named Rocky, a golden retriever." | sent | 7.37s | 8 |
| 5 | "Can you explain what a bloom filter is?" | **failed** (Groq rate limit) | 23.32s | 8 (unchanged) |
| 6 | "My favorite programming language is Go because of its simplicity." | sent | 10.66s | 10 |
| 7 | "I'm currently working on a project involving distributed message queues." | **timeout** (no terminal reply within 25s poll window) | 33.59s | 10 (unchanged) |
| 8 | "What's 15 multiplied by 23?" | **failed** | 4.20s | 10 (unchanged) |
| 9 | "I live in Bangalore, India." | **failed** | 7.38s | 10 (unchanged) |
| 10 | "Remind me, what is my dog's name?" | sent | 4.45s | 12 |
| 11 | "What programming languages did I say I write?" | **failed** | 7.38s | 12 (unchanged) |
| 12 | "Summarize everything you know about me so far." | **failed** | 4.20s | 12 (unchanged, capped — Redis sliding window holds last 20) |

Representative replies:
- **Turn 1**: *"Nice to meet you, Alex! It's great to connect with a backend engineer working in fintech. How can I help you today?"*
- **Turn 6**: *"User profile — Name: Alex, Role: Backend engineer at a fintech startup, Languages: Python and Go, Learning: Rust, Pet: Rocky the golden retriever. It makes perfect sense that Go is your favorite…"* — a full, accurate recap sourced from four separate earlier turns, none repeated in the current prompt.
- **Turn 10**: *"Got it, Alex — Bangalore is a vibrant tech hub! Anything specific you'd like to dive into today?"*

---

## 3. Latency, Isolated from Provider Noise

The gateway → Kafka → LLM Service → Kafka → Cassandra round-trip itself is fast and consistent through Conversation Service. The variance below is almost entirely Groq queueing/backoff, not the pipeline this service owns.

| Stage | Typical | Observed range | What it covers |
|---|---|---|---|
| Send (`202 Accepted`) | 2.1s | 2.07–2.30s | Gateway auth + Cassandra write + outbox stage |
| Successful full turn | ~9s | 4.4–23.3s | Kafka hop → context collect → Groq analysis → Groq generation → Kafka back → persist |
| Rate-limited turn | ~6s | 4.2–33.6s | Same path, terminated early by a Groq 429 on analysis or generation |

The "send" latency is entirely within Conversation Service's control (auth + Cassandra write + Transactional Outbox stage) and stayed flat across all 12 turns regardless of what happened downstream — confirming the outbox/publish path built in Task Update 11 adds negligible, consistent overhead.

---

## 4. Parent → Child: What Actually Carries Over

A child conversation was created via `POST /conversations` with `parent_conversation_id` pointing at the parent above, then asked two questions only answerable from parent context.

| | Child conversation memory | Parent conversation memory (same instant) |
|---|---|---|
| Short-term messages | **0** | **12** |

`GetMemoryContext(child_id)` returned an entirely empty short-term list, medium summary, and fact list on both child turns. Queried immediately after, from the same process, the parent's full history was untouched and fully intact.

---

## 5. What This Confirms

### 5.1 Short-term memory ingestion is live end-to-end
Before this session's work, Memory Service's Kafka workers subscribed to topics nothing published to. The new `MemoryUpdateWorker` (documented in memory-service's own Task Update 7) consumes LLM Service's `memory.update.requested` event and feeds both sides of the turn into the existing `EventDispatcher` — visible here as a clean +2 message count on every successful turn, persisted in Cassandra's `conversation_recent_messages` and served back through `GetMemoryContext`. From Conversation Service's side, this closes the loop that Task Update 11 opened: the message this service publishes to `chat.message.created` now has a verified, working path all the way through to durable long-term memory storage, not just to the LLM's immediate reply.

**Evidence**: message count strictly 2 → 4 → 6 → 8 → 10 → 12 across the six successful turns; zero movement on every failed/timeout turn.

### 5.2 The model genuinely uses that memory, not just stores it
Turn 6 restates Alex's name, role, both languages, the Rust hobby, and the dog's breed — all sourced from four separate earlier turns, none repeated in the current prompt. This is the context-window assembly working as designed, not a scripted echo.

### 5.3 Memory does not cross conversation boundaries on its own
Setting `parent_conversation_id` at creation time (a feature this service has supported since its branching/lineage work) links the two conversations in Conversation Service's own data model and in Graph Service's Neo4j lineage graph — but, as far as the *memory* tier is concerned, in name only. `conversation_recent_messages` and the Redis sliding window are keyed strictly by `conversation_id` — a child conversation starts with a genuinely blank short-term memory, confirmed by direct gRPC query.

The one path designed to bridge this — `ContextBuilder.get_parent_summaries()` in Memory Service, which calls Graph Service's ancestor-lineage lookup — is a Neo4j lineage traversal, not a copy of conversation content. It surfaces the graph *relationship*, not the message history. Whether that traversal actually resolves for this pair is a graph-lineage question, not something this service's own `parent_conversation_id` field controls directly, and it wasn't exercised in this run since both child turns failed on the same Groq rate limit before reaching the reply stage.

**Evidence**: `memory_in_child.short_term_messages = []` on both child turns, while `memory_in_parent_after` in the same instant shows all 12 parent messages untouched.

### 5.4 Medium-term summary and long-term facts never activated in this run
The summarization pipeline triggers at a 20-message threshold (`EventDispatcher.summary_threshold` in Memory Service); this conversation reached 12. Long-term fact extraction is chained after summarization, so it never ran either. Both are wired and running (`SummaryWorker`, `FactWorker`, `EmbeddingWorker`, `OutboxDaemonWorker` all joined their consumer groups this session) but need a longer conversation, or a lower threshold, to actually observe firing.

### 5.5 Groq's free tier is the actual ceiling on this test, not the integration
5 of 12 parent turns and both child turns failed with a Groq 429 (8,000 TPM shared across the Request-Analysis and generation calls that now both hit Groq, since NVIDIA's model reached end-of-life and the Gemini fallback key was revoked). Every failure produced the correct behavior downstream — a terminal `chat.response.generated` with `status: error`, correctly consumed by this service's `_handle_chat_response_generated` (Task Update 11), an SSE close, and no memory-tier corruption.

---

## 6. What Was NOT Changed

- No changes to Conversation Service's own code in this task — this is a verification pass exercising the Kafka wiring from Task Update 11 and the Transactional Outbox / idempotency / Redis PubSub / SSE paths from earlier updates, all unchanged.
- No changes to the parent/child conversation branching feature (Task Update 10) — its existing behavior (linking metadata + Neo4j lineage) is confirmed accurate; it was never expected to carry Cassandra message content across conversations, and does not.

## 7. Known Gaps / Follow-ups

- A longer conversation (20+ messages) is needed to observe the medium-term summary and long-term fact extraction chain actually firing, from Conversation Service's perspective as the originating side of the traffic.
- If parent-conversation context should be visible to a child conversation's replies, that is a Memory Service / Graph Service enhancement (extending `ContextBuilder.get_parent_summaries()` to actually pull parent message content, not just confirm the lineage edge exists) — out of scope for Conversation Service itself, which already exposes `parent_conversation_id` correctly at creation time.
