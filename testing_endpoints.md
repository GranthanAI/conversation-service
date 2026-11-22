# Conversation Service — API Testing Guide

This guide contains instructions, example HTTP request bodies, and cURL / PowerShell commands to test all endpoints.

---

## 1. Generate Test JWT Token

Run the following command in your terminal to generate a valid Bearer token for a test user (`user_id: 11111111-1111-1111-1111-111111111111`):

```bash
uv run python -c "import jwt, datetime; print(jwt.encode({'sub': '11111111-1111-1111-1111-111111111111', 'email': 'testuser@example.com', 'exp': datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=24)}, 'supersecretjwtkeyforauthservicelocaldvelopment12345', algorithm='HS256'))"
```

Save the generated string into an environment variable:
- **PowerShell:** `$TOKEN="<paste_token_here>"`
- **Bash:** `export TOKEN="<paste_token_here>"`

---

## 2. Health Check Endpoints (Public)

### Liveness Probe
```bash
curl -X GET "http://localhost:8000/v1/health/live"
```

### Readiness Probe
```bash
curl -X GET "http://localhost:8000/v1/health/ready"
```
**Response:**
```json
{
  "status": "UP",
  "details": {
    "cassandra": "UP",
    "redis": "UP",
    "kafka": "UP",
    "grpc": "UP"
  }
}
```

---

## 3. Conversation Catalog Endpoints (Protected)

### 3.1 Create Conversation
- **Method:** `POST`
- **Path:** `http://localhost:8000/v1/conversations`
- **Headers:** `Authorization: Bearer $TOKEN`, `Content-Type: application/json`

**Request Body:**
```json
{
  "title": "Quantum Physics Exploration"
}
```

**cURL Command:**
```bash
curl -X POST "http://localhost:8000/v1/conversations" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "Quantum Physics Exploration"}'
```

**Response (201 Created):**
```json
{
  "conversation_id": "0194ad56-3c22-79e0-8111-89e43210abcd",
  "user_id": "11111111-1111-1111-1111-111111111111",
  "title": "Quantum Physics Exploration",
  "created_at": "2026-07-27T18:30:00Z",
  "updated_at": "2026-07-27T18:30:00Z",
  "status": "active"
}
```

---

### 3.2 List User Conversations
- **Method:** `GET`
- **Path:** `http://localhost:8000/v1/conversations?limit=20`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X GET "http://localhost:8000/v1/conversations?limit=20" \
  -H "Authorization: Bearer $TOKEN"
```

**Response (200 OK):**
```json
{
  "items": [
    {
      "conversation_id": "0194ad56-3c22-79e0-8111-89e43210abcd",
      "user_id": "11111111-1111-1111-1111-111111111111",
      "title": "Quantum Physics Exploration",
      "created_at": "2026-07-27T18:30:00Z",
      "updated_at": "2026-07-27T18:30:00Z",
      "status": "active"
    }
  ],
  "next_cursor": null
}
```

---

### 3.3 Get Single Conversation
- **Method:** `GET`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X GET "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd" \
  -H "Authorization: Bearer $TOKEN"
```

---

### 3.4 Rename Conversation
- **Method:** `PATCH`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}/rename`
- **Headers:** `Authorization: Bearer $TOKEN`, `Content-Type: application/json`

**Request Body:**
```json
{
  "title": "Advanced Quantum Entanglement & Computing"
}
```

**cURL Command:**
```bash
curl -X PATCH "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd/rename" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "Advanced Quantum Entanglement & Computing"}'
```

---

### 3.5 Archive Conversation
- **Method:** `POST`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}/archive`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X POST "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd/archive" \
  -H "Authorization: Bearer $TOKEN"
```

---

### 3.6 Soft Delete Conversation
- **Method:** `DELETE`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X DELETE "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd" \
  -H "Authorization: Bearer $TOKEN"
```
**Response:** `204 No Content`

---

## 4. Message History & Streaming Endpoints (Protected)

### 4.1 Send User Message
- **Method:** `POST`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}/messages`
- **Headers:** `Authorization: Bearer $TOKEN`, `Content-Type: application/json`

**Request Body:**
```json
{
  "content": "Explain Schrödinger's cat experiment in simple terms."
}
```

**cURL Command:**
```bash
curl -X POST "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd/messages" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content": "Explain Schrödinger'\''s cat experiment in simple terms."}'
```

**Response (202 Accepted):**
```json
{
  "message_id": "0194ad57-8e99-71a2-9222-123456789abc",
  "conversation_id": "0194ad56-3c22-79e0-8111-89e43210abcd",
  "sender": "user",
  "content": "Explain Schrödinger's cat experiment in simple terms.",
  "created_at": "2026-07-27T18:31:00Z",
  "status": "sent"
}
```

---

### 4.2 Get Message History (Cache-Aside)
- **Method:** `GET`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}/messages?limit=50`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X GET "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd/messages?limit=50" \
  -H "Authorization: Bearer $TOKEN"
```

---

### 4.3 Regenerate Assistant Message
- **Method:** `POST`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}/messages/{message_id}/regenerate`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X POST "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd/messages/0194ad57-8e99-71a2-9222-123456789abc/regenerate" \
  -H "Authorization: Bearer $TOKEN"
```

---

### 4.4 Soft Delete Message
- **Method:** `DELETE`
- **Path:** `http://localhost:8000/v1/conversations/{conversation_id}/messages/{message_id}`
- **Headers:** `Authorization: Bearer $TOKEN`

**cURL Command:**
```bash
curl -X DELETE "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd/messages/0194ad57-8e99-71a2-9222-123456789abc" \
  -H "Authorization: Bearer $TOKEN"
```
**Response:** `204 No Content`

---

## 5. Security Validation Tests

### Test 1: Missing Token (401 Unauthorized)
```bash
curl -X GET "http://localhost:8000/v1/conversations"
```
**Response:** `401 Unauthorized` (`{"detail": "Not authenticated"}`)

### Test 2: Expired or Invalid Token (401 Unauthorized)
```bash
curl -X GET "http://localhost:8000/v1/conversations" \
  -H "Authorization: Bearer invalid.token.value"
```
**Response:** `401 Unauthorized` (`{"detail": "Invalid authentication token"}`)

### Test 3: Ownership Protection (403 Forbidden)
Generate a token for a different user (`user_id: 22222222-2222-2222-2222-222222222222`) and try accessing `user_1`'s conversation:
```bash
export OTHER_TOKEN=$(uv run python -c "import jwt, datetime; print(jwt.encode({'sub': '22222222-2222-2222-2222-222222222222'}, 'supersecretjwtkeyforauthservicelocaldvelopment12345', algorithm='HS256'))")

curl -X GET "http://localhost:8000/v1/conversations/0194ad56-3c22-79e0-8111-89e43210abcd" \
  -H "Authorization: Bearer $OTHER_TOKEN"
```
**Response:** `403 Forbidden` (`{"detail": "Forbidden: You do not own this conversation"}`)

---

## 6. Quick wins (edit, thumbs, share, temporary chat, follow-ups, think longer)

All routes are under `/v1` (the gateway adds the `/api` prefix). `$TOKEN` and `$CID` are as in section 1.

### Edit a sent message (a new sibling branch)
```bash
curl -X POST "http://localhost:8000/v1/conversations/$CID/messages/$MID/edit" \
  -H "Authorization: Bearer $TOKEN" -H "X-Idempotency-Key: $(uuidgen)" -H "Content-Type: application/json" \
  -d '{"content": "Explain it for a class 10 student", "think": false}'
```
**Response:** `202 {"conversation": {..., "parent_conversation_id", "branch_message_id", "edited_from_message_id"}, "message": {..., "response_message_id"}}`.
The branch goes to the message before `$MID` in `$CID` (case 1); for the first message of a branch it becomes a sibling of
that branch (case 2); for the first message of a root it is a new root (case 3). `file_ids` omitted keeps the original files.
`409` codes: `not_user_message`, `message_not_sent`, `temporary_conversation`, `conversation_busy`. A retry with the same key returns the
same conversation and message.
```bash
curl "http://localhost:8000/v1/conversations/$CID/messages/$MID/edits" -H "Authorization: Bearer $TOKEN"
# [{"conversation_id", "title", "created_at"}]  deleted versions are not listed
```

### Thumbs up / down
```bash
curl -X PUT "http://localhost:8000/v1/conversations/$CID/messages/$ANSWER_ID/feedback" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d '{"rating": "down", "reason": "too_long", "comment": "Too much text"}'
curl -X DELETE "http://localhost:8000/v1/conversations/$CID/messages/$ANSWER_ID/feedback" -H "Authorization: Bearer $TOKEN"   # 204
```
`reason`: inaccurate, not_helpful, wrong_level, too_long, too_short, formatting, unsafe, other (thumbs down only). `409 not_assistant_message`
unless the message is a finished answer. History rows carry your own `feedback: {rating, reason}` or `null`.
Staff (roles `support` or `admin`):
```bash
curl "http://localhost:8000/v1/feedback/admin?rating=down&model_group=smart&reason=too_long&from=2026-11-01&to=2026-11-22&limit=50" \
  -H "Authorization: Bearer $STAFF_TOKEN"        # 403 for anyone else; no user id or email in the items
```

### Share link
```bash
curl -X POST "http://localhost:8000/v1/shares" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d "{\"conversation_id\": \"$CID\", \"scope\": \"hierarchy\"}"       # 201 {share_id, url_token (shown once), ...}
curl "http://localhost:8000/v1/shares" -H "Authorization: Bearer $TOKEN"                          # active links, no tokens
curl "http://localhost:8000/v1/shares/public/$URL_TOKEN"                                           # PUBLIC, no Authorization header
curl -X POST "http://localhost:8000/v1/shares/$SHARE_ID/refresh" -H "Authorization: Bearer $TOKEN"
curl -X DELETE "http://localhost:8000/v1/shares/$SHARE_ID" -H "Authorization: Bearer $TOKEN"       # 204; the link then answers 404 share_not_found
```
Errors: `409 temporary_conversation`, `409 share_limit` (50 active links), `413 share_too_large`; the public read answers
`429` with `Retry-After` past 60 requests a minute per IP, and `404 share_not_found` for unknown and revoked tokens alike.

### Temporary chat
```bash
curl -X POST "http://localhost:8000/v1/conversations" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"title": "scratch", "temporary": true}'      # response has temporary: true and expires_at (+24 h); not in GET /v1/conversations
```
A temporary chat cannot have a parent (`400`), cannot be branched from, edited or shared (`409 temporary_conversation`).
The expiry worker deletes it after `expires_at` through the normal delete path.

### Follow-ups and think longer
`PATCH /v1/preferences {"suggest_followups": false}` turns the suggestions off. After an answer, `GET .../messages` returns
`suggestions: [...]` on the assistant row once llm-service has published them. Send, edit and regenerate accept `"think": true`; the limit
`think_longer_per_day` (plan) is checked first (`403 plan_limit`, `limit: think_longer_per_day`). Assistant rows carry `think: true`
when the answer used reasoning; the final SSE event carries `notice_key: "think.not_applied"` when it was asked for but not applied.
