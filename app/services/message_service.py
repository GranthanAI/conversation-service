"""
Message Service.
Coordinates Cache-Aside caching, database operations, and direct Kafka event publishing.
"""

import json
import uuid
from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID

from app.repositories.message_repository import CassandraMessageRepository
from app.models.message import Message, MessageStatus
from app.events.topics import KafkaTopics
from app.core.logging import logger
from app.services.cache_service import CacheService
from app.clients.kafka_producer import KafkaProducerClient
from app.core.config import settings
from app.middleware.correlation import get_correlation_id
from app.utils.helpers import uuidv7, uuidv7_after


class PendingResponseRegistrationError(Exception):
    """The Redis timeout marker for a pending assistant message could not be registered.

    A request must not be dispatched without timeout protection, otherwise a lost
    generation would leave the client waiting forever, so the send is aborted and the
    placeholder removed."""


class MessageService:
    """
    Service orchestrator handling conversational message logs and streaming triggers.
    """
    def __init__(
        self,
        repo: CassandraMessageRepository,
        cache_service: Optional[CacheService] = None,
        producer_client: Optional[KafkaProducerClient] = None
    ):
        self.repo = repo
        self.cache = cache_service
        self.producer = producer_client

    async def _invalidate_cache(self, conversation_id: UUID):
        """
        Deletes the cached message history list from Redis via CacheService.
        """
        if self.cache:
            await self.cache.delete_last_50_messages(conversation_id)

    def _build_chat_message_created_payload(
        self,
        conversation_id: UUID,
        message_id: UUID,
        user_id: Optional[UUID],
        content: str,
        mode: Optional[str] = None,
        selected_tools: Optional[List[str]] = None,
        file_ids: Optional[List[UUID]] = None,
        response_message_id: Optional[UUID] = None,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        Builds the flat chat.message.created payload matching llm-service's
        ChatMessageCreatedEvent (app/models/request.py) exactly - no envelope
        wrapper, since llm-service's Kafka consumer deserializes the raw
        message body directly into that Pydantic model.

        Identity contract (schema_version 2.1):
          message_id            request / correlation id. For a send this is the user's
                                message id; for a regeneration it is a fresh request id.
          response_message_id   the pending ASSISTANT message this request will fill,
                                allocated here before dispatch. llm-service echoes it
                                unchanged on every chunk / final / error event.
        file_ids are the attached File Service document_ids.
        """
        now = datetime.now(timezone.utc).isoformat()
        correlation_id = get_correlation_id()
        return {
            "event_type": "chat.message.created",
            "schema_version": "2.1",
            "message_id": str(message_id),
            "response_message_id": str(response_message_id) if response_message_id else None,
            "conversation_id": str(conversation_id),
            "user_id": str(user_id) if user_id else "",
            "content": content,
            "mode_hint": mode,
            "selected_tools": selected_tools or [],
            "file_ids": [str(fid) for fid in file_ids] if file_ids else [],
            "metadata": metadata or {},
            "trace_context": {"traceparent": correlation_id, "tracestate": ""},
            "timestamp": now,
        }

    async def send(
        self,
        conversation_id: UUID,
        message_id: UUID,
        sender: str,
        content: str,
        user_id: Optional[UUID] = None,
        mode: Optional[str] = None,
        selected_tools: Optional[List[str]] = None,
        file_ids: Optional[List[UUID]] = None,
    ) -> Message:
        """
        Persists a message atomically (message + outbox event in one LOGGED BATCH) and
        returns it. Response generation happens asynchronously when llm-service
        publishes chat.response.chunk / chat.response.generated back over Kafka.

        For a user message the assistant reply's identity is fixed FIRST, and ordering
        guarantees that before the outbox row can be published:
          1. the response id is allocated (always sorts after the user message),
          2. the pending assistant row exists under that id,
          3. the Redis timeout marker is registered under that same id,
          4. the request event carries that id.
        Cassandra and Redis do not share a transaction, so each step has an explicit
        compensation: if a later step fails, the earlier ones are undone (placeholder
        soft-deleted, marker removed) and the error is raised to the caller.
        """
        response_message_id = uuidv7_after(message_id) if sender == "user" else None
        event_id = uuid.uuid1()
        payload = self._build_chat_message_created_payload(
            conversation_id, message_id, user_id, content, mode, selected_tools, file_ids,
            response_message_id=response_message_id,
        )

        if response_message_id:
            await self._open_pending_response(conversation_id, response_message_id)

        try:
            msg = self.repo.create_with_outbox(
                conversation_id=conversation_id,
                message_id=message_id,
                sender=sender,
                content=content,
                status="sent",
                event_id=event_id,
                event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
                outbox_payload=json.dumps(payload),
                file_ids=file_ids
            )
        except Exception:
            if response_message_id:
                await self._discard_pending_response(conversation_id, response_message_id)
            raise

        # Invalidate history cache
        await self._invalidate_cache(conversation_id)
        msg.response_message_id = response_message_id
        return msg

    async def _open_pending_response(self, conversation_id: UUID, response_message_id: UUID) -> None:
        """Creates the pending assistant row and registers its timeout marker (in that
        order). If the marker cannot be registered the row is removed again."""
        self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=response_message_id,
            sender="assistant",
            content="",
            status="pending",
        )
        try:
            await self._register_pending_response(conversation_id, response_message_id)
        except PendingResponseRegistrationError:
            self._soft_delete_placeholder(conversation_id, response_message_id)
            raise

    async def _discard_pending_response(self, conversation_id: UUID, response_message_id: UUID) -> None:
        """Compensation for a send that failed after the placeholder/marker were created."""
        self._soft_delete_placeholder(conversation_id, response_message_id)
        await self.clear_pending_response(response_message_id)

    def _soft_delete_placeholder(self, conversation_id: UUID, response_message_id: UUID) -> None:
        try:
            self.repo.delete(conversation_id, response_message_id)
        except Exception as e:  # compensation is best effort; never mask the original error
            logger.error("Failed to remove pending assistant placeholder",
                         message_id=str(response_message_id), error=str(e))

    async def _register_pending_response(self, conversation_id: UUID, message_id: UUID) -> None:
        """
        Records a pending-response marker in Redis so the PendingTimeoutWorker can
        fail a message that never receives a chat.response.generated event.

        `message_id` is the ASSISTANT response id. Raises
        PendingResponseRegistrationError when the marker cannot be written: dispatching
        a request without timeout protection could strand the client indefinitely.
        """
        if not self.cache or not self.cache.redis:
            raise PendingResponseRegistrationError("Redis is not available for pending-response tracking")
        try:
            key = f"pending_response:{message_id}"
            await self.cache.redis.set(
                key,
                str(conversation_id),
                ex=settings.PENDING_RESPONSE_TIMEOUT_SECONDS + settings.PENDING_RESPONSE_MARKER_GRACE_SECONDS,
            )
            await self.cache.redis.zadd(
                "pending_response:zset",
                {str(message_id): datetime.now(timezone.utc).timestamp() + settings.PENDING_RESPONSE_TIMEOUT_SECONDS},
            )
        except Exception as e:
            logger.warning("Failed to register pending-response timeout marker", message_id=str(message_id), error=str(e))
            # Do not leave a half-registered marker behind.
            await self.clear_pending_response(message_id)
            raise PendingResponseRegistrationError(str(e)) from e

    async def clear_pending_response(self, message_id: UUID) -> None:
        """Removes the pending-response marker once a terminal response (success
        or error) has been handled, so the timeout worker never fails a message
        that was already answered."""
        try:
            await self.cache.redis.delete(f"pending_response:{message_id}")
            await self.cache.redis.zrem("pending_response:zset", str(message_id))
        except Exception as e:
            logger.warning("Failed to clear pending-response marker", message_id=str(message_id), error=str(e))

    async def history(self, conversation_id: UUID, limit: int = settings.MESSAGE_HISTORY_DEFAULT_LIMIT, cursor: Optional[UUID] = None) -> List[Message]:
        """
        Returns message history page using Cache-Aside.
        """
        if cursor is None and self.cache:
            messages = await self.cache.get_last_50_messages(conversation_id, limit)
            if messages is not None:
                return messages

        logger.info("Message history cache miss. Fetching from database...", conversation_id=conversation_id)
        messages = self.repo.history(conversation_id, limit, cursor)

        if cursor is None and self.cache and messages:
            await self.cache.set_last_50_messages(conversation_id, messages)

        return messages

    async def delete(self, conversation_id: UUID, message_id: UUID) -> bool:
        """
        Soft-deletes a message and invalidates cache.
        """
        success = self.repo.delete(conversation_id, message_id)
        if success:
            await self._invalidate_cache(conversation_id)
            return True
        return False

    async def regenerate(self, conversation_id: UUID, message_id: UUID) -> Optional[Message]:
        """
        Regenerates an assistant message.

        Identity: a regeneration is a NEW request, so it gets a fresh request/correlation
        id (llm-service de-duplicates on it, so reusing the original would drop the
        event) and a fresh assistant response id that sorts after the prompt. The
        original prompt row, its id and its attached file_ids are untouched; the prompt
        id is recorded in the event metadata for tracing.

        Ordering matches send(): pending row + timeout marker first, then the outbox
        event; the previous assistant answer is soft-deleted only after the new request
        is staged, so a failed regeneration never costs the user their existing answer.
        """
        history_msgs = self.repo.history(conversation_id, limit=settings.CACHE_HISTORY_LIMIT)

        target_msg = None
        prompt_msg = None

        for i, m in enumerate(history_msgs):
            if m.message_id == message_id:
                target_msg = m
                if i + 1 < len(history_msgs):
                    prompt_msg = history_msgs[i + 1]
                break

        if not target_msg:
            return None

        request_id = uuidv7()
        response_message_id = uuidv7_after(prompt_msg.message_id if prompt_msg else target_msg.message_id)
        prompt_content = prompt_msg.content if prompt_msg else ""
        prompt_user_id = getattr(prompt_msg, "user_id", None)

        # Carries the original prompt's file_ids forward so a regenerated response is
        # re-derived against the same attached documents.
        payload = self._build_chat_message_created_payload(
            conversation_id, request_id, prompt_user_id, prompt_content,
            file_ids=prompt_msg.file_ids if prompt_msg else None,
            response_message_id=response_message_id,
            metadata={
                "regeneration": True,
                "prompt_message_id": str(prompt_msg.message_id) if prompt_msg else None,
                "replaces_message_id": str(message_id),
            },
        )

        await self._open_pending_response(conversation_id, response_message_id)
        try:
            self.repo.create_with_outbox(
                conversation_id=conversation_id,
                message_id=response_message_id,
                sender="assistant",
                content="",
                status="pending",
                event_id=uuid.uuid1(),
                event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
                outbox_payload=json.dumps(payload),
            )
        except Exception:
            await self._discard_pending_response(conversation_id, response_message_id)
            raise

        if target_msg.sender == "assistant":
            try:
                self.repo.delete(conversation_id, message_id)  # soft delete the replaced answer
            except Exception as e:
                logger.error("Failed to soft-delete replaced assistant message",
                             message_id=str(message_id), error=str(e))

        await self._invalidate_cache(conversation_id)
        target_msg.response_message_id = response_message_id
        return target_msg

    def _is_writable_assistant_row(self, conversation_id: UUID, message_id: UUID) -> bool:
        """False if `message_id` already belongs to a non-assistant (user/system) row.

        The table key is (conversation_id, message_id), so writing assistant output
        under a user message's id would silently replace the user's message. This is the
        last line of defence if an upstream ever sends a wrong response id."""
        existing = self.repo.get_message(conversation_id, message_id)
        if existing is not None and existing.sender != "assistant":
            logger.error(
                "Refusing to write assistant output over a non-assistant message",
                conversation_id=str(conversation_id), message_id=str(message_id), existing_sender=existing.sender,
            )
            return False
        return True

    async def finalize_assistant_message(
        self,
        conversation_id: UUID,
        message_id: UUID,
        content: str
    ) -> Optional[Message]:
        """
        Saves the completed assistant message and evicts the history cache. `message_id`
        is the ASSISTANT response id. Returns None (and writes nothing) if that id
        belongs to a user message.
        """
        if not self._is_writable_assistant_row(conversation_id, message_id):
            return None
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=message_id,
            sender="assistant",
            content=content,
            status="sent"
        )
        await self._invalidate_cache(conversation_id)
        return msg

    async def fail_assistant_message(
        self,
        conversation_id: UUID,
        message_id: UUID,
        error_message: str
    ) -> Optional[Message]:
        """Marks the assistant message failed (same user-row guard as finalize)."""
        if not self._is_writable_assistant_row(conversation_id, message_id):
            return None
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=message_id,
            sender="assistant",
            content=error_message,
            status="failed",
        )
        await self._invalidate_cache(conversation_id)
        return msg

    async def attach_summary(
        self,
        conversation_id: UUID,
        summary: str
    ) -> Message:
        """
        Attaches the summary by saving a special system summary message to Cassandra and evicting the cache.
        """
        summary_msg_id = uuid.uuid4()
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=summary_msg_id,
            sender="system",
            content=f"Summary: {summary}",
            status="sent"
        )
        await self._invalidate_cache(conversation_id)
        return msg

