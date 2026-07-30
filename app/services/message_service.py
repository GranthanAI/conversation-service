"""
Message Service.
Coordinates Cache-Aside caching, database operations, and direct Kafka event publishing.
"""

import json
import uuid
import asyncio
from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID

from app.repositories.message_repository import CassandraMessageRepository
from app.models.message import Message, MessageStatus
from app.events.topics import KafkaTopics
from app.events.producers import build_event_envelope
from app.core.logging import logger
from app.services.cache_service import CacheService
from app.clients.kafka_producer import KafkaProducerClient
from app.core.config import settings

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

    async def send(
        self,
        conversation_id: UUID,
        message_id: UUID,
        sender: str,
        content: str,
        user_id: Optional[UUID] = None,
    ) -> Message:
        """
        Persists a message atomically, updates cache, and stages outbox task.
        'sender' is stored internally; 'role' is the canonical Kafka field name.
        """
        event_id = uuid.uuid1()
        envelope = build_event_envelope(
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            payload={
                "conversation_id": str(conversation_id),
                "message_id":      str(message_id),
                "role":            sender,   # canonical field name for consumers
                "content":         content,
                "user_id":         str(user_id) if user_id else None,
            },
            causation_id=str(event_id),
        )
        envelope["event_id"] = str(event_id)

        msg = self.repo.create_with_outbox(
            conversation_id=conversation_id,
            message_id=message_id,
            sender=sender,
            content=content,
            status="sent",
            event_id=event_id,
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            outbox_payload=json.dumps(envelope)
        )

        # Invalidate history cache
        await self._invalidate_cache(conversation_id)

        # Trigger simulated/real LLM response in background if user sent the message
        if sender == "user":
            assistant_msg_id = uuid.uuid4()
            self.repo.create_message_direct(
                conversation_id=conversation_id,
                message_id=assistant_msg_id,
                sender="assistant",
                content="",
                status="pending"
            )
            asyncio.create_task(self.simulate_generation_pipeline(conversation_id, assistant_msg_id, content))

        return msg

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

        envelope = build_event_envelope(
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            payload={
                "conversation_id": str(conversation_id),
                "message_id":      str(new_msg_id),
                "role":            "assistant",
                "content":         "",
                "prompt_content":  prompt_msg.content if prompt_msg else "",
            },
            causation_id=str(event_id),
        )
        envelope["event_id"] = str(event_id)

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
            outbox_payload=json.dumps(envelope)
        )

        await self._invalidate_cache(conversation_id)

        # Trigger simulated/real LLM response in background
        prompt_content = prompt_msg.content if prompt_msg else ""
        asyncio.create_task(self.simulate_generation_pipeline(conversation_id, new_msg_id, prompt_content))
            
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

    async def simulate_generation_pipeline(self, conversation_id: UUID, message_id: UUID, prompt: str) -> None:
        """
        Calls downstream LLM gRPC service, falls back to typewriter simulation in development environment.
        Stages any failures to the chat.message.dlq Kafka topic.
        """
        from app.clients.grpc_client import grpc_generation_client
        from app.services.stream_service import StreamService
        from app.repositories.outbox_repository import CassandraOutboxRepository
        from app.core.config import settings

        stream_service = StreamService(redis_client=self.cache.redis if self.cache else None)
        accumulated_content = ""

        try:
            # 1. Attempt to run real gRPC client token generator stream
            async for chunk in grpc_generation_client.generate(conversation_id, message_id, prompt):
                accumulated_content += chunk["content"]
                await stream_service.publish_token(conversation_id, chunk)
            
            # Finalize message on successful completion
            await self.finalize_assistant_message(conversation_id, message_id, accumulated_content)
            
        except Exception as e:
            logger.warning("gRPC generation connection failed. Evaluating fallback policy...", error=str(e))
            
            if settings.ENVIRONMENT == "development":
                logger.info("Falling back to typewriter simulation generator (Development Mode).")
                # Wait a short moment
                await asyncio.sleep(0.5)
                full_text = f"This is a simulated AI assistant streaming response for your prompt: '{prompt}'."
                chunks = [full_text[i:i+4] for i in range(0, len(full_text), 4)]
                
                for index, chunk in enumerate(chunks):
                    is_final = (index == len(chunks) - 1)
                    chunk_payload = {
                        "conversation_id": str(conversation_id),
                        "message_id": str(message_id),
                        "sender": "assistant",
                        "content": chunk,
                        "is_final": is_final
                    }
                    await stream_service.publish_token(conversation_id, chunk_payload)
                    await asyncio.sleep(0.05)
                
                await self.finalize_assistant_message(conversation_id, message_id, full_text)
            else:
                # 2. Production Failure Recovery (Retry & DLQ)
                logger.error("Generation failed. Updating Cassandra message status and routing to DLQ.", error=str(e))
                # Set message status to failed
                self.repo.create_message_direct(
                    conversation_id=conversation_id,
                    message_id=message_id,
                    sender="assistant",
                    content=accumulated_content,
                    status="failed"
                )
                
                # Push failure notification to SSE stream
                err_payload = {
                    "conversation_id": str(conversation_id),
                    "message_id": str(message_id),
                    "sender": "assistant",
                    "content": "Generation aborted: service connection lost",
                    "is_final": True,
                    "status": "failed"
                }
                await stream_service.publish_token(conversation_id, err_payload)
                
                # Write failure payload to Outbox mapped to the Kafka DLQ topic
                dlq_payload = {
                    "conversation_id": str(conversation_id),
                    "message_id": str(message_id),
                    "sender": "assistant",
                    "prompt_content": prompt,
                    "accumulated_content": accumulated_content,
                    "error_details": str(e),
                    "failed_at": datetime.now(timezone.utc).isoformat()
                }
                outbox_repo = CassandraOutboxRepository()
                outbox_repo.save(
                    bucket=conversation_id.int % 32,
                    event_id=uuid.uuid1(),
                    event_type="chat.message.dlq",
                    payload=json.dumps(dlq_payload)
                )
