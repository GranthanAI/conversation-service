"""
Pending Response Timeout Worker.
Polls the pending_response:zset Redis sorted set (populated by MessageService
when a user message is sent) and fails any assistant message that hasn't
received a chat.response.generated event from llm-service within the
configured timeout, so the client is never left waiting forever.
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

            try:
                now_ts = datetime.now(timezone.utc).timestamp()
                expired_ids = await redis.zrangebyscore(PENDING_ZSET_KEY, min=0, max=now_ts)
            except Exception as e:
                logger.warning("Pending Timeout Worker failed to scan zset", error=str(e))
                continue

            for message_id_str in expired_ids:
                if isinstance(message_id_str, bytes):
                    message_id_str = message_id_str.decode("utf-8")

                try:
                    key = f"pending_response:{message_id_str}"
                    conversation_id_str = await redis.get(key)

                    # Key already deleted (response arrived) — just clean up the zset entry.
                    if not conversation_id_str:
                        await redis.zrem(PENDING_ZSET_KEY, message_id_str)
                        continue

                    if isinstance(conversation_id_str, bytes):
                        conversation_id_str = conversation_id_str.decode("utf-8")

                    conversation_id = UUID(conversation_id_str)
                    message_id = UUID(message_id_str)

                    logger.warning(
                        "Pending response timed out — no chat.response.generated received. Marking failed.",
                        conversation_id=conversation_id_str,
                        message_id=message_id_str,
                    )

                    msg_repo.create_message_direct(
                        conversation_id=conversation_id,
                        message_id=message_id,
                        sender="assistant",
                        content="Generation timed out: no response from the AI service.",
                        status="failed",
                    )
                    await cache_service.delete_last_50_messages(conversation_id)

                    await stream_service.publish_token(conversation_id, {
                        "conversation_id": conversation_id_str,
                        "message_id": message_id_str,
                        "sender": "assistant",
                        "content": "Generation timed out: no response from the AI service.",
                        "is_final": True,
                        "status": "failed",
                    })

                    await redis.delete(key)
                    await redis.zrem(PENDING_ZSET_KEY, message_id_str)
                except Exception as e:
                    logger.error(
                        "Pending Timeout Worker failed to process expired entry",
                        message_id=message_id_str,
                        error=str(e),
                    )
    except asyncio.CancelledError:
        logger.info("Pending Response Timeout Worker shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in Pending Response Timeout Worker loop", error=str(e))
