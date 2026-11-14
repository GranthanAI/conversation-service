"""G5: user.deleted. Routes answer 404 for the deleted user at once, open streams close, and after the retention period the
user's data is purged through the existing delete paths; replays keep the first deletion time and a partial failure resumes.
Real PostgreSQL (throwaway database in the shared Postgres), fake Cassandra conversation service and object store."""

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import jwt
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import settings
from app.events.consumers import _handle_entitlements_event, record_user_deleted
from app.security.dependencies import get_current_user
from app.workers.user_purge_worker import purge_due_users
from tests.support.shared_pg import migrated_pg_url  # noqa: F401  (fixture)

pytestmark = pytest.mark.anyio


@pytest.fixture
async def sf(migrated_pg_url):
    eng = create_async_engine(migrated_pg_url)
    yield async_sessionmaker(bind=eng, expire_on_commit=False, autoflush=False)
    await eng.dispose()


class FakeConversations:
    """The two calls the purge uses: list (sync, like ConversationService.list) and delete (async)."""

    def __init__(self):
        self.items: dict[uuid.UUID, list] = {}
        self.deleted: list = []
        self.fail_on: uuid.UUID | None = None

    def add(self, user, n=1):
        ids = []
        for _ in range(n):
            cid = uuid.uuid4()
            self.items.setdefault(user, []).append(SimpleNamespace(conversation_id=cid, updated_at=datetime.now(timezone.utc)))
            ids.append(cid)
        return ids

    def list(self, user_id, limit=100, cursor=None):
        return list(self.items.get(user_id, []))[:limit]

    async def delete(self, conversation_id):
        if conversation_id == self.fail_on:
            raise RuntimeError("cassandra down")
        for user, lst in self.items.items():
            self.items[user] = [c for c in lst if c.conversation_id != conversation_id]
        self.deleted.append(conversation_id)
        return True


class FakeStore:
    def __init__(self):
        self.deleted: list[str] = []

    async def delete(self, key):
        self.deleted.append(key)


def bearer(user):
    token = jwt.encode({"sub": str(user), "exp": datetime.now(timezone.utc) + timedelta(minutes=5)}, settings.JWT_SECRET_KEY,
                       algorithm=settings.JWT_ALGORITHM)
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


async def make_due(sf, user):
    async with sf() as db:
        await db.execute(text("UPDATE user_deletions SET purge_after = now() - interval '1 minute' WHERE user_id = :u"), {"u": user})
        await db.commit()


async def seed(sf, user, convs):
    """One row in every table the purge must empty."""
    root, child = convs[0], convs[-1]
    mode = uuid.uuid4()
    async with sf() as db:
        await db.execute(text("INSERT INTO custom_modes (mode_id, owner_user_id, name, name_normalized, instructions, execution_style)"
                              " VALUES (:m, :u, 'm', 'm', 'be brief', 'direct')"), {"m": mode, "u": user})
        await db.execute(text("INSERT INTO user_preferences (user_id, default_custom_mode_id) VALUES (:u, :m)"), {"u": user, "m": mode})
        await db.execute(text("INSERT INTO conversation_message_counters (conversation_id, user_message_count) VALUES (:c, 3)"), {"c": root})
        await db.execute(text("INSERT INTO tree_documents (root_id, document_id) VALUES (:r, :d)"), {"r": root, "d": uuid.uuid4()})
        await db.execute(text("INSERT INTO conversation_notes (id, owner_id, conversation_id, content) VALUES (:i, :u, :c, 'n')"), {"i": uuid.uuid4(), "u": user, "c": child})
        await db.execute(text("INSERT INTO export_jobs (job_id, owner_user_id, conversation_id, scope, format, request_key, snapshot, object_key)"
                              " VALUES (:j, :u, :c, 'conversation', 'markdown', 'k', '{}', 'exports/x.zip')"), {"j": uuid.uuid4(), "u": user, "c": root})
        await db.execute(text("INSERT INTO artifacts (artifact_id, owner_user_id, artifact_type, fingerprint, spec_text, origin_conversation_id,"
                              " object_key, thumbnail_key) VALUES (:a, :u, 'mermaid', :f, 'graph TD', :c, 'art/a.mp4', 'art/a.png')"),
                         {"a": uuid.uuid4(), "u": user, "f": "f" * 64, "c": root})
        await db.commit()


async def counts(sf, user, convs):
    q = {
        "custom_modes": ("SELECT count(*) FROM custom_modes WHERE owner_user_id = :u", {"u": user}),
        "user_preferences": ("SELECT count(*) FROM user_preferences WHERE user_id = :u", {"u": user}),
        "notes": ("SELECT count(*) FROM conversation_notes WHERE owner_id = :u", {"u": user}),
        "exports": ("SELECT count(*) FROM export_jobs WHERE owner_user_id = :u", {"u": user}),
        "artifacts": ("SELECT count(*) FROM artifacts WHERE owner_user_id = :u", {"u": user}),
        "counters": ("SELECT count(*) FROM conversation_message_counters WHERE conversation_id = ANY(:i)", {"i": convs}),
        "tree_documents": ("SELECT count(*) FROM tree_documents WHERE root_id = ANY(:i)", {"i": convs}),
    }
    async with sf() as db:
        return {k: (await db.execute(text(sql), p)).scalar_one() for k, (sql, p) in q.items()}


# ---- the 404 and the stream ------------------------------------------------------------------------------------------

async def test_every_user_route_is_404_after_deletion_and_not_before(sf):
    user, other = uuid.uuid4(), uuid.uuid4()
    async with sf() as db:
        assert (await get_current_user(bearer(user), None, db)).id == user
    await record_user_deleted(user, sf)
    async with sf() as db:
        with pytest.raises(HTTPException) as e:
            await get_current_user(bearer(user), None, db)             # what require_conversation_owner, preferences, ... use
        assert e.value.status_code == 404
        assert (await get_current_user(bearer(other), None, db)).id == other


async def test_user_deleted_event_records_once_closes_streams_and_dedupes(sf, monkeypatch):
    user, event_id = uuid.uuid4(), uuid.uuid4()
    inbox = MagicMock()
    inbox.exists.return_value = False
    entitlements = MagicMock()
    entitlements.invalidate = AsyncMock()
    published = []

    class FakeRedis:
        async def publish(self, channel, data):
            published.append((channel, json.loads(data)))
            return 1

    from app.db.redis import redis_manager
    monkeypatch.setattr(redis_manager, "client", FakeRedis())
    from app.db import session as session_mod
    monkeypatch.setattr(session_mod, "get_session_factory", lambda: sf)
    event = {"event_id": str(event_id), "user_id": str(user)}
    await _handle_entitlements_event("user.deleted", event, inbox, entitlements)
    assert published == [(f"user:{user}:stream", {"sse_event": "close"})]
    entitlements.invalidate.assert_awaited_once()
    inbox.save.assert_called_once_with(event_id)
    async with sf() as db:
        assert (await db.execute(text("SELECT count(*) FROM user_deletions WHERE user_id = :u"), {"u": user})).scalar_one() == 1
    inbox.exists.return_value = True                                    # redelivery of the same event: nothing happens
    await _handle_entitlements_event("user.deleted", event, inbox, entitlements)
    assert len(published) == 1


async def test_open_stream_ends_on_close():
    import asyncio
    from app.services.stream_service import StreamService
    redis = MagicMock()
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.close = AsyncMock()
    messages = [{"data": json.dumps({"sse_event": "close"})}]
    pubsub.get_message = AsyncMock(side_effect=lambda **k: messages.pop(0) if messages else None)
    redis.pubsub.return_value = pubsub
    redis.set = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.expire = AsyncMock()
    stream = StreamService(redis_client=redis)
    frames = [f async for f in stream.subscribe(uuid.uuid4(), user_id=uuid.uuid4())]
    assert frames == [": ok\n\n"]                                         # connected, then closed by the deletion


async def test_replay_keeps_the_first_deletion_time(sf):
    user = uuid.uuid4()
    await record_user_deleted(user, sf)
    async with sf() as db:
        first = (await db.execute(text("SELECT deleted_at, purge_after FROM user_deletions WHERE user_id = :u"), {"u": user})).one()
    await record_user_deleted(user, sf)
    async with sf() as db:
        again = (await db.execute(text("SELECT deleted_at, purge_after FROM user_deletions WHERE user_id = :u"), {"u": user})).one()
    assert tuple(again) == tuple(first) and (first.purge_after - first.deleted_at).days == settings.USER_DATA_RETENTION_DAYS == 30


# ---- the purge ---------------------------------------------------------------------------------------------------------

async def test_nothing_is_purged_before_the_retention_period(sf):
    user, convs, store = uuid.uuid4(), FakeConversations(), FakeStore()
    ids = convs.add(user, 2)
    await seed(sf, user, ids)
    await record_user_deleted(user, sf)
    assert await purge_due_users(sf, convs, store) == 0
    assert convs.deleted == [] and store.deleted == []
    assert (await counts(sf, user, ids))["notes"] == 1


async def test_purge_after_retention_goes_through_the_delete_paths(sf):
    user, other, convs, store = uuid.uuid4(), uuid.uuid4(), FakeConversations(), FakeStore()
    ids, other_ids = convs.add(user, 3), convs.add(other, 1)
    await seed(sf, user, ids)
    await seed(sf, other, other_ids)
    await record_user_deleted(user, sf)
    await make_due(sf, user)
    assert await purge_due_users(sf, convs, store) == 1
    assert sorted(convs.deleted) == sorted(ids)                              # each conversation via ConversationService.delete
    assert sorted(store.deleted) == ["art/a.mp4", "art/a.png", "exports/x.zip"]
    assert set((await counts(sf, user, ids)).values()) == {0}                # everything of theirs is gone
    assert (await counts(sf, other, other_ids))["notes"] == 1 and convs.items[other]   # someone else's data is untouched
    async with sf() as db:
        assert (await db.execute(text("SELECT purged_at IS NOT NULL FROM user_deletions WHERE user_id = :u"), {"u": user})).scalar_one()
    assert await purge_due_users(sf, convs, store) == 0                      # idempotent: nothing is due any more
    assert len(convs.deleted) == 3


async def test_partial_failure_resumes_where_it_stopped(sf):
    user, convs, store = uuid.uuid4(), FakeConversations(), FakeStore()
    ids = convs.add(user, 3)
    await seed(sf, user, ids)
    await record_user_deleted(user, sf)
    await make_due(sf, user)
    convs.fail_on = ids[1]
    assert await purge_due_users(sf, convs, store) == 0                      # the second conversation failed
    async with sf() as db:
        assert (await db.execute(text("SELECT purged_at FROM user_deletions WHERE user_id = :u"), {"u": user})).scalar_one() is None
    assert ids[0] in convs.deleted and ids[1] not in convs.deleted
    convs.fail_on = None
    assert await purge_due_users(sf, convs, store) == 1                      # the next sweep finishes the job
    assert sorted(convs.deleted) == sorted(ids) and len(convs.deleted) == 3 # none deleted twice
    assert set((await counts(sf, user, ids)).values()) == {0}


async def test_a_failing_object_delete_keeps_the_row_and_resumes(sf):
    user, convs = uuid.uuid4(), FakeConversations()
    ids = convs.add(user, 1)
    await seed(sf, user, ids)
    await record_user_deleted(user, sf)
    await make_due(sf, user)

    class Flaky(FakeStore):
        async def delete(self, key):
            if key == "art/a.png" and not getattr(self, "ok", False):
                raise RuntimeError("minio down")
            await super().delete(key)

    store = Flaky()
    assert await purge_due_users(sf, convs, store) == 0
    assert (await counts(sf, user, ids))["artifacts"] == 1                   # the row stays until its objects are gone
    store.ok = True
    assert await purge_due_users(sf, convs, store) == 1
    assert (await counts(sf, user, ids))["artifacts"] == 0
