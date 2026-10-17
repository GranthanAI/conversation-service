"""
Replayable SSE stream: event log, resume by message_id/since, dedupe, and late-final recovery.

Runs against the real local Redis (same as the repo's other Redis tests) using unique
conversation ids, so it exercises the actual XADD/XRANGE/PUBLISH behaviour rather than mocks.
"""

import asyncio
import json
import uuid

import pytest
import redis.asyncio as aioredis

from app.core.config import settings
from app.services.stream_service import StreamService


@pytest.fixture
async def redis_client():
    client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception:
        pytest.skip("local Redis is not available")
    yield client
    await client.aclose()


def _chunk(cid, mid, seq, text):
    return {"conversation_id": str(cid), "message_id": str(mid), "sender": "assistant",
            "content": text, "sequence_number": seq, "is_final": False}


def _final(cid, mid, text="full", status="completed"):
    base = {"conversation_id": str(cid), "message_id": str(mid), "sender": "assistant",
            "content": "" if status == "completed" else text, "is_final": True, "status": status}
    if status == "completed":
        base["full_content"] = text
        base["citations"] = []
    return base


def _parse(frames):
    out = []
    for f in frames:
        for block in f.split("\n\n"):
            data = [ln[6:] for ln in block.split("\n") if ln.startswith("data: ")]
            eid = [ln[4:] for ln in block.split("\n") if ln.startswith("id: ")]
            if data:
                ev = json.loads(data[0])
                out.append((eid[0] if eid else None, ev))
    return out


async def _drain(gen, limit=50, timeout=5.0):
    frames = []

    async def run():
        async for f in gen:
            frames.append(f)
            if len(frames) >= limit:
                break

    await asyncio.wait_for(run(), timeout)
    return _parse(frames)


@pytest.mark.anyio
async def test_events_are_logged_with_increasing_ids_and_published_with_event_id(redis_client):
    cid, mid = uuid.uuid4(), uuid.uuid4()
    svc = StreamService(redis_client=redis_client)
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"conversation:{cid}:stream")
    await svc.publish_token(cid, _chunk(cid, mid, 1, "a"))
    await svc.publish_token(cid, _chunk(cid, mid, 2, "b"))
    ev = await svc.replay(cid, str(mid), None)
    assert [e["content"] for e in ev] == ["a", "b"]
    assert svc.event_id_key(ev[0]["event_id"]) < svc.event_id_key(ev[1]["event_id"])
    seen = []
    for _ in range(5):
        m = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        if m:
            seen.append(json.loads(m["data"]))
    assert [s["event_id"] for s in seen] == [e["event_id"] for e in ev]
    await redis_client.delete(f"conversation:{cid}:events")
    await pubsub.aclose()


@pytest.mark.anyio
async def test_late_subscriber_recovers_everything_published_before_it_connected(redis_client):
    cid, mid = uuid.uuid4(), uuid.uuid4()
    svc = StreamService(redis_client=redis_client)
    # whole turn happens before anybody subscribes (the original race)
    await svc.publish_token(cid, _chunk(cid, mid, 1, "Hel"))
    await svc.publish_token(cid, _chunk(cid, mid, 2, "lo"))
    await svc.publish_token(cid, _final(cid, mid, "Hello"))
    events = await _drain(svc.subscribe(cid, message_id=str(mid), since="0"))
    contents = [e["content"] for _, e in events if not e["is_final"]]
    assert contents == ["Hel", "lo"]
    assert events[-1][1]["is_final"] and events[-1][1]["full_content"] == "Hello"
    assert all(eid for eid, _ in events)  # SSE id: lines present
    await redis_client.delete(f"conversation:{cid}:events")


@pytest.mark.anyio
async def test_resume_after_cursor_skips_already_delivered_and_follows_live(redis_client):
    cid, mid = uuid.uuid4(), uuid.uuid4()
    svc = StreamService(redis_client=redis_client)
    await svc.publish_token(cid, _chunk(cid, mid, 1, "one"))
    first = (await svc.replay(cid, str(mid), None))[0]["event_id"]
    await svc.publish_token(cid, _chunk(cid, mid, 2, "two"))

    async def later():
        await asyncio.sleep(0.5)
        await svc.publish_token(cid, _chunk(cid, mid, 3, "three"))
        await svc.publish_token(cid, _final(cid, mid, "onetwothree"))

    task = asyncio.create_task(later())
    events = await _drain(svc.subscribe(cid, message_id=str(mid), since=first))
    await task
    assert [e["content"] for _, e in events if not e["is_final"]] == ["two", "three"]  # no "one"
    assert events[-1][1]["full_content"] == "onetwothree"
    ids = [eid for eid, _ in events]
    assert ids == sorted(ids, key=svc.event_id_key) and len(set(ids)) == len(ids)  # ordered, no dupes
    await redis_client.delete(f"conversation:{cid}:events")


@pytest.mark.anyio
async def test_stream_is_scoped_to_one_message_and_ignores_other_turns(redis_client):
    cid, mine, other = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    svc = StreamService(redis_client=redis_client)
    await svc.publish_token(cid, _chunk(cid, other, 1, "NOT-MINE"))
    await svc.publish_token(cid, _final(cid, other, "other done"))
    await svc.publish_token(cid, _chunk(cid, mine, 1, "mine"))
    await svc.publish_token(cid, _final(cid, mine, "mine"))
    events = await _drain(svc.subscribe(cid, message_id=str(mine), since="0"))
    assert {e["message_id"] for _, e in events} == {str(mine)}
    await redis_client.delete(f"conversation:{cid}:events")


@pytest.mark.anyio
async def test_real_reply_beats_an_earlier_timeout_failure_on_replay(redis_client):
    cid, mid = uuid.uuid4(), uuid.uuid4()
    svc = StreamService(redis_client=redis_client)
    await svc.publish_token(cid, _final(cid, mid, "Generation timed out", status="failed"))
    await svc.publish_token(cid, _final(cid, mid, "The real answer"))  # late success
    events = await _drain(svc.subscribe(cid, message_id=str(mid), since="0"))
    assert [e.get("status") for _, e in events] == ["completed"]
    assert events[0][1]["full_content"] == "The real answer"
    await redis_client.delete(f"conversation:{cid}:events")


@pytest.mark.anyio
async def test_legacy_subscription_without_message_id_still_ends_at_first_terminal(redis_client):
    cid, mid = uuid.uuid4(), uuid.uuid4()
    svc = StreamService(redis_client=redis_client)

    async def producer():
        await asyncio.sleep(0.4)
        await svc.publish_token(cid, _chunk(cid, mid, 1, "x"))
        await svc.publish_token(cid, _final(cid, mid, "x"))

    task = asyncio.create_task(producer())
    events = await _drain(svc.subscribe(cid))
    await task
    assert events[-1][1]["is_final"]
    await redis_client.delete(f"conversation:{cid}:events")


@pytest.mark.anyio
async def test_publish_still_works_when_the_event_log_fails(redis_client):
    cid, mid = uuid.uuid4(), uuid.uuid4()

    class Flaky:
        def __init__(self, inner):
            self.inner = inner

        async def xadd(self, *a, **k):
            raise RuntimeError("log down")

        def __getattr__(self, n):
            return getattr(self.inner, n)

    svc = StreamService(redis_client=Flaky(redis_client))
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"conversation:{cid}:stream")
    await svc.publish_token(cid, _chunk(cid, mid, 1, "live-only"))
    got = None
    for _ in range(5):
        m = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
        if m:
            got = json.loads(m["data"])
            break
    assert got and got["content"] == "live-only" and "event_id" not in got
    await pubsub.aclose()
