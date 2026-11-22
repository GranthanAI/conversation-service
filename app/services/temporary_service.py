"""Temporary chats: bookkeeping and the expiry sweep.

A temporary conversation is written to `conversations` only (never `conversations_by_user`), so it is not listed, searched or
exported with "all conversations". Because the catalog cannot enumerate it, this module keeps the `temporary_conversations`
table: the expiry worker lists due rows from it, and the user purge lists a deleted user's temporary chats from it.

Expiry goes through the NORMAL delete path (ConversationService.delete: soft delete + `conversation.deleted`), so the cleanup
worker purges Cassandra and Redis, and memory-service and graph-service react as for any deleted conversation.
"""

import asyncio
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.core.logging import logger
from app.models.conversation import Conversation


class TemporaryService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def register(self, conv: Conversation) -> None:
        """Records a new temporary conversation for the expiry sweep (in the request's transaction)."""
        await self.session.execute(
            text("INSERT INTO temporary_conversations (conversation_id, user_id, expires_at) "
                 "VALUES (:c, :u, :e) ON CONFLICT (conversation_id) DO NOTHING"),
            {"c": conv.conversation_id, "u": conv.user_id, "e": conv.expires_at})


async def expire_due(factory: async_sessionmaker[AsyncSession], conversations, now: Optional[datetime] = None) -> int:
    """One sweep: deletes every temporary conversation whose expires_at has passed and marks its row deleted.
    Idempotent (a deleted row is not picked again; deleting an already-deleted conversation is harmless) and one bad
    row never stops the others. Returns how many rows were expired."""
    now = now or datetime.now(timezone.utc)
    async with factory() as db:
        due = [c for (c,) in (await db.execute(
            text("SELECT conversation_id FROM temporary_conversations "
                 "WHERE expires_at <= :now AND deleted_at IS NULL ORDER BY expires_at LIMIT 500"), {"now": now}))]
    done = 0
    for conversation_id in due:
        try:
            await conversations.delete(conversation_id)             # soft delete + conversation.deleted
            async with factory() as db:
                await db.execute(text("UPDATE temporary_conversations SET deleted_at = now() "
                                      "WHERE conversation_id = :c AND deleted_at IS NULL"), {"c": conversation_id})
                await db.commit()
            done += 1
        except Exception as e:  # noqa: BLE001 - one bad row must not stop the sweep; it is retried next time
            logger.error("temporary_expiry_failed", conversation_id=str(conversation_id), error=str(e))
    if done:
        logger.info("temporary_conversations_expired", count=done)
    return done


async def temporary_ids_of_user(factory: async_sessionmaker[AsyncSession], user_id: UUID) -> list[UUID]:
    """Temporary chats of a user that are not deleted yet (they are not in conversations_by_user)."""
    async with factory() as db:
        return [c for (c,) in (await db.execute(
            text("SELECT conversation_id FROM temporary_conversations WHERE user_id = :u AND deleted_at IS NULL"),
            {"u": user_id}))]


async def start_temporary_expiry_worker() -> None:
    from app.db.redis import redis_manager
    from app.db.session import get_session_factory
    from app.repositories.conversation_repository import CassandraConversationRepository
    from app.services.cache_service import CacheService
    from app.services.conversation_service import ConversationService

    logger.info("Starting temporary chat expiry worker...")
    conversations = ConversationService(repo=CassandraConversationRepository(),
                                        cache_service=CacheService(redis_client=redis_manager.client))
    try:
        while True:
            try:
                await expire_due(get_session_factory(), conversations)
            except Exception as e:  # noqa: BLE001
                logger.error("temporary_expiry_sweep_failed", error=str(e))
            await asyncio.sleep(settings.TEMPORARY_EXPIRY_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("Temporary chat expiry worker shutdown.")
