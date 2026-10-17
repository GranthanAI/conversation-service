"""
Pending Response Timeout Worker.
Polls the pending_response:zset Redis sorted set (populated by MessageService
when a user message is sent) and fails any assistant message that hasn't
received a chat.response.generated event from llm-service within the
configured timeout, so the client is never left waiting forever.

Every id in the set and every `pending_response:<id>` key is the ASSISTANT response
id (the pending row's message_id), never the user's message id.
"""

import asyncio
from datetime import datetime, timezone
from uuid import UUID

from app.core.config import settings
from app.core.logging import logger
from app.db.redis import redis_manager
from app.repositories.message_repository import CassandraMessageRepository
from app.services.cache_service import CacheService
from app.services.stream_service import StreamService

PENDING_ZSET_KEY = "pending_response:zset"

TIMEOUT_MESSAGE = "Generation timed out: no response from the AI service."


async def process_expired_pending(redis, msg_repo, cache_service, stream_service, now_ts: float | None = None) -> int:
    """One scan pass. Fails every pending assistant message whose deadline has passed
    and whose marker is still registered. Returns the number of messages failed.

    A message that already completed (status `sent`) is never touched: the marker is
    simply cleaned up. A reply that arrives *after* the timeout still wins, because
    finalize overwrites the failed row with the real content."""
    if now_ts is None:
        now_ts = datetime.now(timezone.utc).timestamp()
    try:
        expired_ids = await redis.zrangebyscore(PENDING_ZSET_KEY, min=0, max=now_ts)
    except Exception as e:
        logger.warning("Pending Timeout Worker failed to scan zset", error=str(e))
        return 0

    failed = 0
    for message_id_str in expired_ids:
        if isinstance(message_id_str, bytes):
            message_id_str = message_id_str.decode("utf-8")

        try:
            key = f"pending_response:{message_id_str}"
            conversation_id_str = await redis.get(key)

            # Key already deleted (response arrived) - just clean up the zset entry.
            if not conversation_id_str:
                await redis.zrem(PENDING_ZSET_KEY, message_id_str)
                continue

            if isinstance(conversation_id_str, bytes):
                conversation_id_str = conversation_id_str.decode("utf-8")

            conversation_id = UUID(conversation_id_str)
            message_id = UUID(message_id_str)

            # Belt and braces for the window between a final event being written and its
            # marker being cleared: never turn a completed reply into a failure.
            existing = msg_repo.get_message(conversation_id, message_id)
            if existing is not None and existing.sender == "assistant" and existing.status == "sent":
                await redis.delete(key)
                await redis.zrem(PENDING_ZSET_KEY, message_id_str)
                continue

            logger.warning(
                "Pending response timed out - no chat.response.generated received. Marking failed.",
                conversation_id=conversation_id_str,
                message_id=message_id_str,
            )

            msg_repo.create_message_direct(
                conversation_id=conversation_id,
                message_id=message_id,
                sender="assistant",
                content=TIMEOUT_MESSAGE,
                status="failed",
            )
            await cache_service.delete_last_50_messages(conversation_id)

            await stream_service.publish_token(conversation_id, {
                "conversation_id": conversation_id_str,
                "message_id": message_id_str,
                "sender": "assistant",
                "content": TIMEOUT_MESSAGE,
                "is_final": True,
                "status": "failed",
            })

            await redis.delete(key)
            await redis.zrem(PENDING_ZSET_KEY, message_id_str)
            failed += 1
        except Exception as e:
            logger.error(
                "Pending Timeout Worker failed to process expired entry",
                message_id=message_id_str,
                error=str(e),
            )
    return failed


async def start_pending_timeout_worker():
    """
    Periodically scans for pending-response markers whose deadline has
    elapsed, marks the corresponding assistant message failed, and pushes a
    terminal SSE error event so the frontend's stream loop terminates cleanly.
    """
    logger.info("Starting Pending Response Timeout Worker...")

    msg_repo = CassandraMessageRepository()
    cache_service = CacheService(redis_client=redis_manager.client)
    stream_service = StreamService(redis_client=redis_manager.client)

    try:
        while True:
            await asyncio.sleep(settings.PENDING_TIMEOUT_WORKER_POLL_INTERVAL_SECONDS)
            redis = redis_manager.client
            if not redis:
                continue
            await process_expired_pending(redis, msg_repo, cache_service, stream_service)
    except asyncio.CancelledError:
        logger.info("Pending Response Timeout Worker shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in Pending Response Timeout Worker loop", error=str(e))
