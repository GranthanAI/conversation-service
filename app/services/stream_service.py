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

    def _get_event_log_key(self, conversation_id: UUID) -> str:
        return f"conversation:{conversation_id}:events"

    @staticmethod
    def event_id_key(event_id: Optional[str]) -> tuple:
        """Orders Redis Stream ids ("<ms>-<seq>"); unknown/missing ids sort first."""
        try:
            ms, _, seq = str(event_id).partition("-")
            return int(ms), int(seq or 0)
        except (TypeError, ValueError):
            return (-1, -1)

    async def publish_token(self, conversation_id: UUID, chunk_data: dict) -> None:
        """
        Appends the event to the conversation's replay log (assigning it a monotonically
        increasing `event_id`) and then publishes it on the live Pub/Sub channel. The log is
        best-effort: if it fails, the event is still published live without an id.
        """
        if not self.redis:
            return
        channel = self._get_pubsub_channel(conversation_id)
        payload = dict(chunk_data)
        try:
            log_key = self._get_event_log_key(conversation_id)
            event_id = await self.redis.xadd(
                log_key,
                {"d": json.dumps(chunk_data)},
                maxlen=settings.STREAM_EVENT_LOG_MAXLEN,
                approximate=True,
            )
            await self.redis.expire(log_key, settings.STREAM_EVENT_LOG_TTL_SECONDS)
            payload["event_id"] = event_id.decode() if isinstance(event_id, bytes) else str(event_id)
        except Exception as e:
            logger.warning("Failed to append event to replay log", conversation_id=str(conversation_id), error=str(e))
        try:
            await self.redis.publish(channel, json.dumps(payload))
        except Exception as e:
            logger.warning("Failed to publish token chunk to Redis PubSub", conversation_id=str(conversation_id), error=str(e))

    async def replay(self, conversation_id: UUID, message_id: str, since: Optional[str]) -> list:
        """
        Events of one assistant message retained in the log with event_id > `since`
        (None/"0" = everything retained), oldest first. If the turn has a `completed`
        terminal event, an earlier `failed` one (the timeout watchdog firing before a late
        real reply) is dropped so the client sees the real outcome.
        """
        if not self.redis:
            return []
        lo = "-" if not since or since == "0" else f"({since}"
        try:
            rows = await self.redis.xrange(self._get_event_log_key(conversation_id), min=lo, max="+")
        except Exception as e:
            logger.warning("Failed to read replay log", conversation_id=str(conversation_id), error=str(e))
            return []
        events = []
        for eid, fields in rows:
            eid = eid.decode() if isinstance(eid, bytes) else str(eid)
            raw = fields.get(b"d", fields.get("d")) if isinstance(fields, dict) else None
            if raw is None:
                continue
            try:
                data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
            except ValueError:
                continue
            if data.get("message_id") != message_id:
                continue
            data["event_id"] = eid
            events.append(data)
        if any(e.get("status") == "completed" for e in events):
            events = [e for e in events if e.get("status") != "failed"]
        return events

    @staticmethod
    def format_sse(data: dict) -> str:
        """SSE frame; `id:` lets EventSource-style clients resume with Last-Event-ID."""
        eid = data.get("event_id")
        return (f"id: {eid}\n" if eid else "") + f"data: {json.dumps(data)}\n\n"

    async def subscribe(
        self,
        conversation_id: UUID,
        message_id: Optional[str] = None,
        since: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Subscribes to the conversation's Redis PubSub channel and yields SSE-formatted events.
        Includes a background task for periodic heartbeats.

        Resumable mode (`message_id` = the assistant response id): after the subscription is
        live, retained events of that message newer than `since` are replayed from the event
        log, then live events continue (deduplicated by `event_id`), and the stream ends after
        that message's terminal event or STREAM_MAX_CONNECTION_SECONDS. Without `message_id`
        the original behaviour applies (live only; ends at the first terminal event).
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

            last_eid = (-1, -1)
            loop = asyncio.get_event_loop()
            deadline = loop.time() + settings.STREAM_MAX_CONNECTION_SECONDS

            if message_id:
                # Subscribed first, then replay: anything published during the replay is
                # buffered by the subscription and deduplicated below.
                for ev in await self.replay(conversation_id, message_id, since):
                    last_eid = max(last_eid, self.event_id_key(ev.get("event_id")))
                    yield self.format_sse(ev)
                    if ev.get("is_final") or ev.get("status") == "completed":
                        return

            while True:
                if message_id and loop.time() > deadline:
                    return
                try:
                    # Non-blocking check for new messages
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=settings.STREAM_PUBSUB_TIMEOUT_SECONDS)
                    if msg:
                        data_str = msg["data"]
                        if isinstance(data_str, bytes):
                            data_str = data_str.decode("utf-8")

                        # Parse data to ensure correctness
                        data = json.loads(data_str)

                        if message_id:
                            if data.get("message_id") != message_id:
                                continue
                            key = self.event_id_key(data.get("event_id"))
                            if data.get("event_id") and key <= last_eid:
                                continue  # already delivered by the replay
                            last_eid = max(last_eid, key)

                        # Format as SSE event
                        yield self.format_sse(data)

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
