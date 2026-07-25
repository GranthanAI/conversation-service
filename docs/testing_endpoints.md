# API Testing Guidelines - Conversation Management Module

You can test all endpoints directly using client tools (like Postman, curl, or the built-in Swagger UI).

---

## 1. Base Configuration

- **Base URL**: `http://localhost:8000`
- **Interactive API Docs (Swagger UI)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Authentication Headers**:
  - `Authorization: Bearer <any-mock-token>`
  - `X-User-Id: 00000000-0000-0000-0000-000000000001` (Use this header to simulate a specific authenticated user UUID locally)

---

## 2. API Endpoints Catalog

### A. Create Conversation
- **Method & Path**: `POST /v1/conversations`
- **Request Headers**:
  - `Content-Type: application/json`
  - `X-User-Id: 00000000-0000-0000-0000-000000000001`
- **Input JSON Payload**:
  ```json
  {
    "title": "Introduction to Cassandra"
  }
  ```
- **Example cURL Command**:
  ```bash
  curl -X POST "http://localhost:8000/v1/conversations" \
       -H "Content-Type: application/json" \
       -H "X-User-Id: 00000000-0000-0000-0000-000000000001" \
       -d "{\"title\": \"Introduction to Cassandra\"}"
  ```
- **Expected Success Response (201 Created)**:
  ```json
  {
    "conversation_id": "4a737d2f-5b4d-44a9-a084-6bce8ac16b1d",
    "user_id": "00000000-0000-0000-0000-000000000001",
    "title": "Introduction to Cassandra",
    "created_at": "2026-07-26T00:20:00Z",
    "updated_at": "2026-07-26T00:20:00Z",
    "status": "active"
  }
  ```

---

### B. List Conversations
- **Method & Path**: `GET /v1/conversations`
- **Query Parameters**:
  - `limit` (Optional, integer, default: 20, max: 100)
  - `cursor` (Optional, ISO-datetime string for paginated pages)
- **Request Headers**:
  - `X-User-Id: 00000000-0000-0000-0000-000000000001`
- **Example cURL Command**:
  ```bash
  curl -X GET "http://localhost:8000/v1/conversations?limit=10" \
       -H "X-User-Id: 00000000-0000-0000-0000-000000000001"
  ```
- **Expected Success Response (200 OK)**:
  ```json
  {
    "conversations": [
      {
        "conversation_id": "4a737d2f-5b4d-44a9-a084-6bce8ac16b1d",
        "user_id": "00000000-0000-0000-0000-000000000001",
        "title": "Introduction to Cassandra",
        "created_at": "2026-07-26T00:20:00Z",
        "updated_at": "2026-07-26T00:20:00Z",
        "status": "active"
      }
    ],
    "next_cursor": null
  }
  ```

---

### C. Retrieve Details of a Conversation
- **Method & Path**: `GET /v1/conversations/{conversation_id}`
- **Request Headers**:
  - `X-User-Id: 00000000-0000-0000-0000-000000000001`
- **Example cURL Command**:
  ```bash
  curl -X GET "http://localhost:8000/v1/conversations/4a737d2f-5b4d-44a9-a084-6bce8ac16b1d" \
       -H "X-User-Id: 00000000-0000-0000-0000-000000000001"
  ```
- **Expected Success Response (200 OK)**:
  ```json
  {
    "conversation_id": "4a737d2f-5b4d-44a9-a084-6bce8ac16b1d",
    "user_id": "00000000-0000-0000-0000-000000000001",
    "title": "Introduction to Cassandra",
    "created_at": "2026-07-26T00:20:00Z",
    "updated_at": "2026-07-26T00:20:00Z",
    "status": "active"
  }
  ```

---

### D. Rename Conversation
- **Method & Path**: `PATCH /v1/conversations/{conversation_id}/title`
- **Request Headers**:
  - `Content-Type: application/json`
  - `X-User-Id: 00000000-0000-0000-0000-000000000001`
- **Input JSON Payload**:
  ```json
  {
    "title": "Advanced Cassandra Partitioning Strategies"
  }
  ```
- **Example cURL Command**:
  ```bash
  curl -X PATCH "http://localhost:8000/v1/conversations/4a737d2f-5b4d-44a9-a084-6bce8ac16b1d/title" \
       -H "Content-Type: application/json" \
       -H "X-User-Id: 00000000-0000-0000-0000-000000000001" \
       -d "{\"title\": \"Advanced Cassandra Partitioning Strategies\"}"
  ```
- **Expected Success Response (200 OK)**:
  ```json
  {
    "conversation_id": "4a737d2f-5b4d-44a9-a084-6bce8ac16b1d",
    "user_id": "00000000-0000-0000-0000-000000000001",
    "title": "Advanced Cassandra Partitioning Strategies",
    "created_at": "2026-07-26T00:20:00Z",
    "updated_at": "2026-07-26T00:23:15Z",
    "status": "active"
  }
  ```

---

### E. Delete Conversation (Soft-Delete)
- **Method & Path**: `DELETE /v1/conversations/{conversation_id}`
- **Request Headers**:
  - `X-User-Id: 00000000-0000-0000-0000-000000000001`
- **Example cURL Command**:
  ```bash
  curl -X DELETE "http://localhost:8000/v1/conversations/4a737d2f-5b4d-44a9-a084-6bce8ac16b1d" \
       -H "X-User-Id: 00000000-0000-0000-0000-000000000001"
  ```
- **Expected Success Response (204 No Content)**:
  *(Empty Response)*
