"""Assistant-response identity and pending-message lifecycle.

Models the real flow end to end with in-memory fakes of Cassandra, Redis, the Kafka
inbox and the SSE stream, and a `fake_llm` that behaves exactly like llm-service's
emitters (it echoes `message_id` as the request id and `response_message_id` from the
request event):

    send -> user row + pending assistant row + Redis marker + outbox event
         -> (llm) chunks + final event -> assistant row finalized, marker cleared

Invariants under test: the user's row is never overwritten, the assistant reply is
stored under the pre-allocated pending id, SSE events identify the assistant message,
and the timeout marker is cleared for the *right* id.
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import pytest

from app.events.consumers import _handle_chat_response_chunk, _handle_chat_response_generated
from app.models.message import Message, MessageStatus
from app.services.message_service import MessageService
from app.utils.helpers import uuidv7

pytestmark = pytest.mark.anyio

CONV = uuid.UUID("00000000-0000-7000-8000-0000000000c0")
USER = uuid.UUID("00000000-0000-7000-8000-00000000001a")
FILE = uuid.UUID("00000000-0000-4000-8000-0000000000f1")
ZSET = "pending_response:zset"


# --------------------------------------------------------------------------- fakes


class FakeRepo:
    def __init__(self):
        self.rows: dict[tuple, Message] = {}
        self.outbox: list[dict] = []
        self.ops: list[tuple] = []
        self.fail_on: set[str] = set()
        self.on_outbox = None  # callback(payload) fired when the outbox row is staged

    def _put(self, conv, mid, sender, content, status, file_ids=None):
        msg = Message(conv, mid, sender, content, datetime.now(timezone.utc), MessageStatus(status), list(file_ids or []))
        self.rows[(conv, mid)] = msg
        return msg

    def create_message_direct(self, conversation_id, message_id, sender, content, status):
        if status == "pending" and "placeholder" in self.fail_on:
            raise RuntimeError("cassandra unavailable (placeholder)")
        self.ops.append(("direct", message_id, sender, status))
        return self._put(conversation_id, message_id, sender, content, status)

    def create_with_outbox(self, conversation_id, message_id, sender, content, status, event_id, event_type,
                           outbox_payload, file_ids=None):
        if "outbox" in self.fail_on:
            raise RuntimeError("cassandra unavailable (outbox)")
        payload = json.loads(outbox_payload)
        self.ops.append(("outbox", message_id, sender, status))
        self.outbox.append(payload)
        msg = self._put(conversation_id, message_id, sender, content, status, file_ids)
        if self.on_outbox:
            self.on_outbox(payload)
        return msg

    def get_message(self, conversation_id, message_id) -> Optional[Message]:
        return self.rows.get((conversation_id, message_id))

    def history(self, conversation_id, limit=50, cursor=None):
        rows = [m for (c, _), m in self.rows.items() if c == conversation_id and m.status != "deleted"]
        return sorted(rows, key=lambda m: m.message_id, reverse=True)

    def delete(self, conversation_id, message_id):
        if (conversation_id, message_id) in self.rows:
            self.rows[(conversation_id, message_id)].status = "deleted"
        self.ops.append(("delete", message_id))
        return True


class FakeRedis:
    def __init__(self):
        self.kv, self.z = {}, {}
        self.fail_set = False

    async def set(self, key, value, ex=None):
        if self.fail_set:
            raise ConnectionError("redis down")
        self.kv[key] = value

    async def get(self, key):
        return self.kv.get(key)

    async def delete(self, key):
        self.kv.pop(key, None)

    async def zadd(self, name, mapping):
        self.z.setdefault(name, {}).update(mapping)

    async def zrem(self, name, member):
        self.z.get(name, {}).pop(member, None)

    async def zrangebyscore(self, name, min, max):
        return [m for m, s in self.z.get(name, {}).items() if min <= s <= max]


class FakeCache:
    def __init__(self, redis):
        self.redis = redis

    async def delete_last_50_messages(self, conversation_id):
        pass


class FakeInbox:
    def __init__(self):
        self.seen = set()

    def exists(self, key):
        return key in self.seen

    def save(self, key):
        self.seen.add(key)


class FakeStream:
    def __init__(self):
        self.events = []

    async def publish_token(self, conversation_id, payload):
        self.events.append(payload)


@pytest.fixture
def world():
    repo, redis = FakeRepo(), FakeRedis()
    svc = MessageService(repo=repo, cache_service=FakeCache(redis))
    return type("World", (), {"repo": repo, "redis": redis, "svc": svc, "inbox": FakeInbox(), "stream": FakeStream()})


# ----------------------------------------------------------------- llm-service stand-in


def fake_llm_events(request_payload: dict, text: str = "The answer.", *, status="success"):
    """What llm-service 2.1 emits for a request event: chunks keyed by `message_id`
    (request id) and `response_message_id` (copied from the request), then a final
    event. Mirrors app/models/response.py."""
    resp = request_payload.get("response_message_id")
    base = {"conversation_id": request_payload["conversation_id"], "user_id": request_payload["user_id"]}
    chunks = [
        {**base, "message_id": request_payload["message_id"], "response_message_id": resp,
         "chunk_index": i, "sequence_number": i + 1, "content": part, "is_last": i == 1}
        for i, part in enumerate(text.split(" ", 1)[:1] + [text.split(" ", 1)[1]] if " " in text else [text])
    ]
    final = {**base, "request_message_id": request_payload["message_id"], "response_message_id": resp,
             "status": status, "full_content": text if status == "success" else None,
             "error_message": None if status == "success" else "Upstream AI provider error occurred.",
             "citations": [{"file_id": str(FILE), "source_name": "a.pdf", "page_number": 1, "section": "s"}]}
    return chunks, final


async def deliver(world, chunks, final):
    for ev in chunks:
        await _handle_chat_response_chunk(ev, world.inbox, world.stream)
    await _handle_chat_response_generated(final, world.inbox, world.svc, world.stream)


def user_rows(world):
    return [m for m in world.repo.rows.values() if m.sender == "user"]


def assistant_rows(world):
    return [m for m in world.repo.rows.values() if m.sender == "assistant"]


async def send(world, content="What does the handbook say?", file_ids=(FILE,)):
    return await world.svc.send(CONV, USER, "user", content, user_id=uuid.uuid4(), file_ids=list(file_ids))


# ============================================================ identity at send time


async def test_user_and_assistant_rows_have_different_ids_and_the_assistant_sorts_after(world):
    await send(world)
    (user,), (assistant,) = user_rows(world), assistant_rows(world)
    assert user.message_id == USER
    assert assistant.message_id != user.message_id
    assert assistant.message_id > user.message_id, "history sorts by message_id DESC; the reply must come after its question"


async def test_request_event_carries_the_pending_assistant_id_and_a_distinct_request_id(world):
    await send(world)
    (payload,) = world.repo.outbox
    (assistant,) = assistant_rows(world)
    assert payload["response_message_id"] == str(assistant.message_id)
    assert payload["message_id"] == str(USER), "request/correlation id keeps its meaning"
    assert payload["response_message_id"] != payload["message_id"]
    assert payload["schema_version"] == "2.2"


async def test_pending_marker_uses_the_same_id_as_placeholder_and_event(world):
    msg = await send(world)
    (assistant,) = assistant_rows(world)
    (payload,) = world.repo.outbox
    rid = str(assistant.message_id)
    assert f"pending_response:{rid}" in world.redis.kv
    assert rid in world.redis.z[ZSET]
    assert payload["response_message_id"] == rid
    assert msg.response_message_id == assistant.message_id, "the API must tell the client which message will stream"


async def test_placeholder_and_marker_exist_before_the_outbox_event_can_be_published(world):
    seen = {}

    def snapshot(payload):
        rid = payload["response_message_id"]
        seen["placeholder"] = any(m.message_id == UUID(rid) and m.status == "pending" for m in world.repo.rows.values())
        seen["marker"] = f"pending_response:{rid}" in world.redis.kv and rid in world.redis.z.get(ZSET, {})

    world.repo.on_outbox = snapshot
    await send(world)
    assert seen == {"placeholder": True, "marker": True}


# ============================================================ finalisation (the corruption)


async def test_final_event_fills_the_assistant_row_and_never_touches_the_user_row(world):
    await send(world, content="original question")
    (payload,) = world.repo.outbox
    chunks, final = fake_llm_events(payload, "The answer is 42.")
    await deliver(world, chunks, final)

    (user,), (assistant,) = user_rows(world), assistant_rows(world)
    assert (user.content, user.sender, user.status) == ("original question", "user", MessageStatus.SENT)
    assert (assistant.content, assistant.status) == ("The answer is 42.", MessageStatus.SENT)
    assert assistant.message_id == UUID(payload["response_message_id"])
    assert len(world.repo.rows) == 2, "no third row, no overwritten row"


async def test_success_clears_the_marker_for_the_response_id(world):
    await send(world)
    (payload,) = world.repo.outbox
    rid = payload["response_message_id"]
    await deliver(world, *fake_llm_events(payload))
    assert f"pending_response:{rid}" not in world.redis.kv
    assert rid not in world.redis.z.get(ZSET, {})
    assert world.redis.kv == {} and world.redis.z.get(ZSET, {}) == {}


async def test_sse_events_identify_the_assistant_message_not_the_users(world):
    await send(world)
    (payload,) = world.repo.outbox
    await deliver(world, *fake_llm_events(payload))
    rid = payload["response_message_id"]
    assert world.stream.events, "something must be streamed"
    assert {e["message_id"] for e in world.stream.events} == {rid}
    assert {e["request_message_id"] for e in world.stream.events} == {payload["message_id"]}
    final = world.stream.events[-1]
    assert final["is_final"] is True and final["status"] == "completed"
    assert final["full_content"] == "The answer." and final["citations"][0]["source_name"] == "a.pdf"
    assert all(e["is_final"] is False for e in world.stream.events[:-1]), "only the generated event ends a turn"


async def test_error_event_fails_the_assistant_row_clears_its_marker_and_keeps_the_user_row(world):
    await send(world, content="q")
    (payload,) = world.repo.outbox
    rid = payload["response_message_id"]
    await deliver(world, [], fake_llm_events(payload, status="error")[1])

    (user,), (assistant,) = user_rows(world), assistant_rows(world)
    assert (user.content, user.status) == ("q", MessageStatus.SENT)
    assert assistant.status == MessageStatus.FAILED and assistant.message_id == UUID(rid)
    assert f"pending_response:{rid}" not in world.redis.kv and rid not in world.redis.z.get(ZSET, {})
    last = world.stream.events[-1]
    assert last["message_id"] == rid and last["is_final"] is True and last["status"] == "failed"


async def test_duplicate_chunks_and_final_events_are_idempotent(world):
    await send(world)
    (payload,) = world.repo.outbox
    chunks, final = fake_llm_events(payload)
    await deliver(world, chunks, final)
    emitted = len(world.stream.events)
    snapshot = {k: (m.content, m.status) for k, m in world.repo.rows.items()}
    await deliver(world, chunks, final)  # Kafka redelivery
    assert len(world.stream.events) == emitted
    assert {k: (m.content, m.status) for k, m in world.repo.rows.items()} == snapshot


# ============================================================ safety nets


async def test_event_without_response_message_id_is_ignored_and_writes_nothing(world):
    """A legacy/incomplete event must never be mapped onto the user's message id."""
    await send(world, content="keep me")
    (payload,) = world.repo.outbox
    chunks, final = fake_llm_events({**payload, "response_message_id": None})
    before = {k: (m.content, m.status) for k, m in world.repo.rows.items()}
    await deliver(world, chunks, final)
    assert {k: (m.content, m.status) for k, m in world.repo.rows.items()} == before
    assert world.stream.events == []
    assert any(k.startswith("pending_response:") for k in world.redis.kv), "left for the timeout to fail"


async def test_a_response_id_that_points_at_a_user_row_is_refused(world):
    await send(world, content="do not overwrite")
    (payload,) = world.repo.outbox
    chunks, final = fake_llm_events({**payload, "response_message_id": str(USER)})
    await deliver(world, chunks, final)
    (user,) = user_rows(world)
    assert user.content == "do not overwrite" and user.sender == "user"


# ============================================================ timeout worker


async def test_timeout_does_not_fail_a_completed_response(world):
    from app.workers.pending_timeout_worker import process_expired_pending

    await send(world)
    (payload,) = world.repo.outbox
    await deliver(world, *fake_llm_events(payload))
    # time passes beyond the deadline
    failed = await process_expired_pending(world.redis, world.repo, FakeCache(world.redis), world.stream, now_ts=1e12)
    assert failed == 0
    (assistant,) = assistant_rows(world)
    assert assistant.status == MessageStatus.SENT and assistant.content == "The answer."


async def test_timeout_fails_a_genuinely_unanswered_response(world):
    from app.workers.pending_timeout_worker import process_expired_pending

    await send(world)
    (payload,) = world.repo.outbox
    rid = payload["response_message_id"]
    failed = await process_expired_pending(world.redis, world.repo, FakeCache(world.redis), world.stream, now_ts=1e12)
    assert failed == 1
    (assistant,) = assistant_rows(world)
    assert assistant.status == MessageStatus.FAILED and assistant.message_id == UUID(rid)
    (user,) = user_rows(world)
    assert user.status == MessageStatus.SENT
    assert world.stream.events[-1]["message_id"] == rid and world.stream.events[-1]["is_final"] is True


async def test_a_late_valid_response_still_wins_over_a_timeout_failure(world):
    from app.workers.pending_timeout_worker import process_expired_pending

    await send(world)
    (payload,) = world.repo.outbox
    await process_expired_pending(world.redis, world.repo, FakeCache(world.redis), world.stream, now_ts=1e12)
    await deliver(world, *fake_llm_events(payload, "Late but real."))
    (assistant,) = assistant_rows(world)
    assert (assistant.status, assistant.content) == (MessageStatus.SENT, "Late but real.")


# ============================================================ regeneration


async def _answered_turn(world):
    await send(world, content="the prompt")
    (payload,) = world.repo.outbox
    await deliver(world, *fake_llm_events(payload, "First answer."))
    (assistant,) = assistant_rows(world)
    return payload, assistant


async def test_regeneration_gets_a_fresh_response_id_and_keeps_prompt_files_and_identity(world):
    first_payload, old = await _answered_turn(world)
    result = await world.svc.regenerate(CONV, old.message_id)
    assert result is not None

    regen = world.repo.outbox[-1]
    new_rid = UUID(regen["response_message_id"])
    assert new_rid not in (old.message_id, USER)
    assert regen["response_message_id"] != regen["message_id"], "request id and response id stay distinct"
    assert regen["message_id"] != first_payload["message_id"], "a regeneration is a new request (dedup must not drop it)"
    assert regen["file_ids"] == [str(FILE)] and regen["content"] == "the prompt"
    assert new_rid > USER, "the new reply sorts after the prompt"

    (user,) = user_rows(world)
    assert (user.message_id, user.content, user.status) == (USER, "the prompt", MessageStatus.SENT)
    assert world.repo.rows[(CONV, old.message_id)].status == "deleted"
    pending = world.repo.rows[(CONV, new_rid)]
    assert pending.sender == "assistant" and pending.status == MessageStatus.PENDING
    assert f"pending_response:{new_rid}" in world.redis.kv and str(new_rid) in world.redis.z[ZSET]


async def test_regenerated_reply_completes_under_the_new_id_without_touching_the_prompt(world):
    _, old = await _answered_turn(world)
    await world.svc.regenerate(CONV, old.message_id)
    regen = world.repo.outbox[-1]
    await deliver(world, *fake_llm_events(regen, "Second answer."))

    new_row = world.repo.rows[(CONV, UUID(regen["response_message_id"]))]
    assert (new_row.status, new_row.content) == (MessageStatus.SENT, "Second answer.")
    (user,) = user_rows(world)
    assert user.content == "the prompt"
    assert world.redis.kv == {} and world.redis.z.get(ZSET, {}) == {}


# ============================================================ failure policy


async def test_placeholder_failure_aborts_the_send_with_nothing_left_behind(world):
    world.repo.fail_on.add("placeholder")
    with pytest.raises(RuntimeError):
        await send(world)
    assert world.repo.rows == {} and world.repo.outbox == []
    assert world.redis.kv == {} and world.redis.z == {}


async def test_marker_registration_failure_aborts_and_removes_the_placeholder(world):
    from app.services.message_service import PendingResponseRegistrationError

    world.redis.fail_set = True
    with pytest.raises(PendingResponseRegistrationError):
        await send(world)
    assert world.repo.outbox == [], "no request may be published without timeout protection"
    assert user_rows(world) == []
    assert all(m.status == "deleted" for m in assistant_rows(world)), "no live orphan pending message"


async def test_outbox_failure_compensates_marker_and_placeholder(world):
    world.repo.fail_on.add("outbox")
    with pytest.raises(RuntimeError):
        await send(world)
    assert user_rows(world) == []
    assert all(m.status == "deleted" for m in assistant_rows(world))
    assert world.redis.kv == {} and world.redis.z.get(ZSET, {}) == {}, "marker must not outlive a failed send"


async def test_regeneration_outbox_failure_compensates_and_keeps_the_old_answer_visible_state_sane(world):
    _, old = await _answered_turn(world)
    world.repo.fail_on.add("outbox")
    with pytest.raises(RuntimeError):
        await world.svc.regenerate(CONV, old.message_id)
    assert world.redis.kv == {} and world.redis.z.get(ZSET, {}) == {}
    live_pending = [m for m in assistant_rows(world) if m.status == "pending"]
    assert live_pending == []


# ============================================================ reproduction of the original defect


async def test_legacy_event_shape_must_not_overwrite_the_users_message(world):
    """The pre-fix production behaviour: the request carried only the user's message id,
    llm-service echoed it as `request_message_id` on the final event, and Conversation
    Service finalized the assistant reply under THAT id - i.e. over the user's own row
    (primary key is (conversation_id, message_id)). It must now be impossible."""
    await send(world, content="my private question")
    legacy_final = {
        "conversation_id": str(CONV),
        "request_message_id": str(USER),  # the user's message id, as llm-service 2.0 echoed it
        "status": "success",
        "full_content": "ASSISTANT TEXT THAT MUST NOT LAND ON THE USER ROW",
    }
    await _handle_chat_response_generated(legacy_final, world.inbox, world.svc, world.stream)

    row = world.repo.rows[(CONV, USER)]
    assert (row.sender, row.content) == ("user", "my private question"), (
        f"user row was overwritten: sender={row.sender!r} content={row.content!r}"
    )
