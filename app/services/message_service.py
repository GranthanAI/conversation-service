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
    ) -> dict:
        """
        Builds the flat chat.message.created payload matching llm-service's
        ChatMessageCreatedEvent (app/models/request.py) exactly — no envelope
        wrapper, since llm-service's Kafka consumer deserializes the raw
        message body directly into that Pydantic model. file_ids are the
        attached File Service document_ids — retrieval can use them to
        restrict/boost context against the uploaded documents.
        """
        now = datetime.now(timezone.utc).isoformat()
        correlation_id = get_correlation_id()
        return {
            "event_type": "chat.message.created",
            "schema_version": "2.0",
            "message_id": str(message_id),
            "conversation_id": str(conversation_id),
            "user_id": str(user_id) if user_id else "",
            "content": content,
            "mode_hint": mode,
            "selected_tools": selected_tools or [],
            "file_ids": [str(fid) for fid in file_ids] if file_ids else [],
            "metadata": {},
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
        Persists a message atomically, updates cache, and stages outbox task.
        The user's message publishes a flat chat.message.created event (no
        envelope) matching llm-service's expected Kafka Consumer shape.
        Response generation happens asynchronously when llm-service publishes
        chat.response.chunk / chat.response.generated back over Kafka — this
        method does not call llm-service directly.
        """
        event_id = uuid.uuid1()
        payload = self._build_chat_message_created_payload(
            conversation_id, message_id, user_id, content, mode, selected_tools, file_ids
        )

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

        # Invalidate history cache
        await self._invalidate_cache(conversation_id)

        # Create the pending assistant placeholder and register a response
        # timeout so the client isn't left waiting forever if llm-service
        # never responds (see PendingTimeoutWorker).
        if sender == "user":
            assistant_msg_id = uuid.uuid4()
            self.repo.create_message_direct(
                conversation_id=conversation_id,
                message_id=assistant_msg_id,
                sender="assistant",
                content="",
                status="pending"
            )
            await self._register_pending_response(conversation_id, assistant_msg_id)

        return msg

    async def _register_pending_response(self, conversation_id: UUID, message_id: UUID) -> None:
        """
        Records a pending-response marker in Redis with a TTL so the
        PendingTimeoutWorker can detect and fail messages that never receive
        a chat.response.generated event from llm-service.
        """
        if not self.cache or not self.cache.redis:
            return
        try:
            key = f"pending_response:{message_id}"
            await self.cache.redis.set(
                key, str(conversation_id), ex=settings.PENDING_RESPONSE_TIMEOUT_SECONDS
            )
            await self.cache.redis.zadd(
                "pending_response:zset",
                {str(message_id): datetime.now(timezone.utc).timestamp() + settings.PENDING_RESPONSE_TIMEOUT_SECONDS},
            )
        except Exception as e:
            logger.warning("Failed to register pending-response timeout marker", message_id=str(message_id), error=str(e))

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
        Soft-deletes the target assistant message and atomically stages regeneration event.
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

        # Calculate outbox params for new assistant streaming response
        event_id = uuid.uuid1()
        new_msg_id = uuid.uuid1()  # generate a new response task ID
        prompt_content = prompt_msg.content if prompt_msg else ""
        prompt_user_id = getattr(prompt_msg, "user_id", None)

        # Carries the original prompt's file_ids forward so a regenerated
        # response is re-derived against the same attached documents —
        # only possible now that history() actually returns file_ids.
        payload = self._build_chat_message_created_payload(
            conversation_id, new_msg_id, prompt_user_id, prompt_content,
            file_ids=prompt_msg.file_ids if prompt_msg else None
        )

        # Stage regeneration atomically by soft deleting the old response and executing batch outbox stage
        if target_msg.sender == "assistant":
            # Soft delete old response
            self.repo.delete(conversation_id, message_id)

        # Create new pending message atomic with outbox
        self.repo.create_with_outbox(
            conversation_id=conversation_id,
            message_id=new_msg_id,
            sender="assistant",
            content="",
            status="pending",
            event_id=event_id,
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            outbox_payload=json.dumps(payload)
        )

        await self._invalidate_cache(conversation_id)

        # Register a response timeout in case llm-service never responds
        await self._register_pending_response(conversation_id, new_msg_id)

        return target_msg

    async def finalize_assistant_message(
        self,
        conversation_id: UUID,
        message_id: UUID,
        content: str
    ) -> Message:
        """
        Saves the completed assistant message directly to Cassandra and evicts the history cache.
        """
        msg = self.repo.create_message_direct(
            conversation_id=conversation_id,
            message_id=message_id,
            sender="assistant",
            content=content,
            status="sent"
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

