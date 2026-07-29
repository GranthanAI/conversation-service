"""
Stream Service.
Manages Server-Sent Events (SSE) connection mapping, ownership heartbeats in Redis,
and token stream distribution using Redis PubSub channels.
"""

import asyncio
import json
import uuid
from typing import AsyncGenerator, Optional
from uuid import UUID
import redis.asyncio as aioredis

from app.core.logging import logger
from app.core.config import settings

class StreamService:
    """
    Handles SSE stream ownership mapping, Redis PubSub subscription buffers,
    and simulated/real token pipeline routing.
    """
    def __init__(self, redis_client: Optional[aioredis.Redis] = None):
        self.redis = redis_client
        self.pod_id = f"pod_{uuid.uuid4().hex[:8]}"

    def _get_ownership_key(self, conversation_id: UUID) -> str:
        return f"stream:{conversation_id}"

    def _get_pubsub_channel(self, conversation_id: UUID) -> str:
        return f"conversation:{conversation_id}:stream"

    async def register_ownership(self, conversation_id: UUID) -> None:
        """
        Claims/renews ownership of the active stream for a conversation in Redis.
        """
        if not self.redis:
            return
        key = self._get_ownership_key(conversation_id)
        try:
            await self.redis.set(key, self.pod_id, ex=settings.STREAM_OWNERSHIP_TTL_SECONDS)
            logger.info("SSE Stream ownership claimed", conversation_id=str(conversation_id), pod_id=self.pod_id)
        except Exception as e:
            logger.warning("Failed to claim stream ownership in Redis", conversation_id=str(conversation_id), error=str(e))

    async def renew_ownership(self, conversation_id: UUID) -> None:
        """
        Renews the TTL for the active stream ownership to keep it alive during active connection.
        """
        if not self.redis:
            return
        key = self._get_ownership_key(conversation_id)
        try:
            await self.redis.expire(key, settings.STREAM_OWNERSHIP_TTL_SECONDS)
        except Exception as e:
            logger.warning("Failed to renew stream ownership TTL in Redis", conversation_id=str(conversation_id), error=str(e))

    async def release_ownership(self, conversation_id: UUID) -> None:
        """
        Releases ownership of the active stream on clean disconnect.
        """
        if not self.redis:
            return
        key = self._get_ownership_key(conversation_id)
        try:
            current_owner = await self.redis.get(key)
            if current_owner == self.pod_id:
                await self.redis.delete(key)
                logger.info("SSE Stream ownership released", conversation_id=str(conversation_id), pod_id=self.pod_id)
        except Exception as e:
            logger.warning("Failed to release stream ownership in Redis", conversation_id=str(conversation_id), error=str(e))

    async def publish_token(self, conversation_id: UUID, chunk_data: dict) -> None:
        """
        Publishes a token chunk to the Redis PubSub channel.
        """
        if not self.redis:
            return
        channel = self._get_pubsub_channel(conversation_id)
        try:
            await self.redis.publish(channel, json.dumps(chunk_data))
        except Exception as e:
            logger.warning("Failed to publish token chunk to Redis PubSub", conversation_id=str(conversation_id), error=str(e))

    async def subscribe(self, conversation_id: UUID) -> AsyncGenerator[str, None]:
        """
        Subscribes to the conversation's Redis PubSub channel and yields SSE-formatted events.
        Includes a background task for periodic heartbeats.
        """
        if not self.redis:
            # Yield error event and exit if Redis is unavailable
            yield "event: error\ndata: {\"message\": \"Cache service offline\"}\n\n"
            return

        channel_name = self._get_pubsub_channel(conversation_id)
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(channel_name)

        # Claim initial ownership
        await self.register_ownership(conversation_id)

        # Heartbeat loop task
        async def heartbeat_loop():
            try:
                while True:
                    await asyncio.sleep(settings.STREAM_HEARTBEAT_INTERVAL_SECONDS)
                    await self.renew_ownership(conversation_id)
            except asyncio.CancelledError:
                pass

        hb_task = asyncio.create_task(heartbeat_loop())

        try:
            # Yield initial connect response comment
            yield ": ok\n\n"
            
            while True:
                try:
                    # Non-blocking check for new messages
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=settings.STREAM_PUBSUB_TIMEOUT_SECONDS)
                    if msg:
                        data_str = msg["data"]
                        if isinstance(data_str, bytes):
                            data_str = data_str.decode("utf-8")
                        
                        # Parse data to ensure correctness
                        data = json.loads(data_str)
                        
                        # Format as SSE event
                        yield f"data: {json.dumps(data)}\n\n"
                        
                        if data.get("is_final") or data.get("status") == "completed":
                            # End of stream indicator
                            break
                    else:
                        # Yield standard SSE comment heartbeat to keep connection alive in client browser
                        yield ": heartbeat\n\n"
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("Error reading from Redis PubSub channel", channel=channel_name, error=str(e))
                    await asyncio.sleep(settings.STREAM_ERROR_SLEEP_SECONDS)
        finally:
            # Cleanup
            hb_task.cancel()
            await pubsub.unsubscribe(channel_name)
            await pubsub.close()
            await self.release_ownership(conversation_id)
