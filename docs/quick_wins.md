# Quick wins: events and storage (conversation-service)

## Events
- `chat.message.created` **2.5** (to llm-service, graph-service). Additive: `think` (true only when asked AND the plan allowed it),
  `temporary`, `preferences.suggest_followups` (default true).
- `conversation.created` payload gains `temporary` and `edited_from_message_id`.
- `chat.response.generated` **2.3** (from llm-service) adds `think`: reasoning was used for the answer. Stored on the assistant row
  and returned as `MessageResponse.think`; asked-but-not-applied gives the SSE `notice_key` `think.not_applied`.
- NEW consumed topic `chat.response.suggestions` 1.0 (llm-service): `{event_id, conversation_id, response_message_id, suggestions: [str]}`.
  Stored on a SENT assistant row of that conversation only (1..3 items, each <= 120 chars, no newlines), deduplicated on `event_id`,
  history cache invalidated. The topic must exist (see billing's `scripts/topics.txt` for where shared topics are created).

## Storage
- Cassandra (`make schema`, additive): `conversations.temporary / expires_at / edited_from_message_id`,
  `messages_by_conversation.suggestions / think`. Run it BEFORE deploying this version (the new code SELECTs the columns).
  Temporary conversations are written to `conversations` only, never `conversations_by_user`.
- PostgreSQL (`make db-migrate`, revision 0008): `message_feedback`, `message_edits`, `shared_links`, `temporary_conversations`,
  `think_usage_daily`, and `user_preferences.suggest_followups`.
- Object store: `shares/<share_id>.json.gz` (the exports store and settings).

## Settings
`PENDING_RESPONSE_TIMEOUT_THINK_SECONDS` (300), `TEMPORARY_CHAT_TTL_HOURS` (24), `TEMPORARY_EXPIRY_WORKER_ENABLED`,
`TEMPORARY_EXPIRY_INTERVAL_SECONDS` (600), `SHARE_MAX_ACTIVE_PER_USER` (50), `SHARE_PUBLIC_RATE_LIMIT_PER_MINUTE` (60),
`FEEDBACK_ADMIN_ROLES` (support,admin).

## Plan limit
`think_longer_per_day` is enforced here (billing only carries the number): 0 = not in the plan, -1 = unlimited, otherwise one atomic
upsert per IST day in `think_usage_daily`, taken BEFORE billing reserves anything and given back when the send fails.
