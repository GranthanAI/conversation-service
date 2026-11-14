"""user.deleted, second half: after USER_DATA_RETENTION_DAYS the deleted user's data is removed.

The user's conversations go through the EXISTING delete path (ConversationService.delete: soft delete + the
`conversation.deleted` outbox event), so everything that listens to it (this service's cleanup worker hard-deletes messages and
metadata; memory-service removes its data) still hears about each one. Everything else the user owns in PostgreSQL is deleted
directly, in dependency order: P6 counters and tree documents, notes, source edits, exports and artifacts (with their stored
objects), preferences, custom modes.

Idempotent and resumable: every step can be repeated, `purged_at` is set only after the last step succeeded, and a failure
leaves the user due, so the next sweep continues where it stopped. Replays of user.deleted keep the first deleted_at.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.logging import logger

PAGE = 100


def _all_conversations(conversations, user_id: UUID) -> list:
    """Every non-deleted conversation of the user (paged through the catalog)."""
    out, cursor = [], None
    while True:
        page = conversations.list(user_id, limit=PAGE, cursor=cursor)
        out.extend(page)
        if len(page) < PAGE:
            return out
        cursor = page[-1].updated_at


async def purge_user(factory: async_sessionmaker[AsyncSession], conversations, object_store, user_id: UUID) -> None:
    """Raises on any failure (the caller leaves the user due and tries again next sweep)."""
    convs = _all_conversations(conversations, user_id)
    ids = [c.conversation_id for c in convs]

    async with factory() as db:
        if ids:                                        # counters and documents are keyed by conversation / tree root id
            await db.execute(text("DELETE FROM conversation_message_counters WHERE conversation_id = ANY(:ids)"), {"ids": ids})
            await db.execute(text("DELETE FROM tree_documents WHERE root_id = ANY(:ids)"), {"ids": ids})
        await db.commit()

    for conv in convs:                                 # the existing delete path: soft delete + conversation.deleted event
        await conversations.delete(conv.conversation_id)

    async with factory() as db:
        await db.execute(text("DELETE FROM conversation_notes WHERE owner_id = :u"), {"u": user_id})
        await db.execute(text("DELETE FROM source_edits WHERE owner_id = :u"), {"u": user_id})
        await db.commit()

    for table, columns in (("export_jobs", ("object_key",)), ("artifacts", ("object_key", "thumbnail_key"))):
        # stored objects first: a failed delete keeps the row, so the next sweep resumes
        async with factory() as db:
            keys = []
            for column in columns:
                keys += [k for (k,) in (await db.execute(
                    text(f"SELECT {column} FROM {table} WHERE owner_user_id = :u AND {column} IS NOT NULL"), {"u": user_id}))]
        for key in keys:
            await object_store.delete(key)
        async with factory() as db:
            if table == "artifacts":
                await db.execute(text("DELETE FROM artifact_refs WHERE owner_user_id = :u"), {"u": user_id})
            await db.execute(text(f"DELETE FROM {table} WHERE owner_user_id = :u"), {"u": user_id})
            await db.commit()

    async with factory() as db:
        await db.execute(text("DELETE FROM user_preferences WHERE user_id = :u"), {"u": user_id})   # before custom_modes (FK)
        await db.execute(text("DELETE FROM custom_modes WHERE owner_user_id = :u"), {"u": user_id})
        await db.commit()


async def purge_due_users(factory: async_sessionmaker[AsyncSession], conversations, object_store,
                          now: Optional[datetime] = None) -> int:
    """One sweep. Returns how many users were fully purged; a user whose purge failed stays due."""
    now = now or datetime.now(timezone.utc)
    async with factory() as db:
        due = [u for (u,) in (await db.execute(
            text("SELECT user_id FROM user_deletions WHERE purge_after <= :now AND purged_at IS NULL ORDER BY purge_after"),
            {"now": now}))]
    done = 0
    for user_id in due:
        try:
            await purge_user(factory, conversations, object_store, user_id)
        except Exception as e:  # noqa: BLE001 - one user must not stop the others; resumed next sweep
            logger.error("user_purge_failed", user_id=str(user_id), error=str(e))
            continue
        async with factory() as db:
            await db.execute(text("UPDATE user_deletions SET purged_at = now() WHERE user_id = :u"), {"u": user_id})
            await db.commit()
        logger.info("user_purged", user_id=str(user_id))
        done += 1
    return done


async def start_user_purge_worker() -> None:
    from app.artifacts.runtime import get_object_store
    from app.db.redis import redis_manager
    from app.db.session import get_session_factory
    from app.repositories.conversation_repository import CassandraConversationRepository
    from app.services.cache_service import CacheService
    from app.services.conversation_service import ConversationService

    logger.info("Starting user purge worker...")
    conversations = ConversationService(repo=CassandraConversationRepository(),
                                        cache_service=CacheService(redis_client=redis_manager.client))
    try:
        while True:
            try:
                await purge_due_users(get_session_factory(), _Sync(conversations), get_object_store())
            except Exception as e:  # noqa: BLE001
                logger.error("user_purge_sweep_failed", error=str(e))
            await asyncio.sleep(settings.USER_PURGE_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("User purge worker shutdown.")


class _Sync:
    """ConversationService.list is synchronous, delete is async: the purge uses the same two calls on either shape."""

    def __init__(self, service) -> None:
        self._s = service

    def list(self, user_id, limit=100, cursor=None):
        return self._s.list(user_id, limit=limit, cursor=cursor)

    async def delete(self, conversation_id):
        return await self._s.delete(conversation_id)
