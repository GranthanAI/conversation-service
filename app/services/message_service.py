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
from app.core.logging import logger
from app.services.cache_service import CacheService
from app.clients.kafka_producer import KafkaProducerClient

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

    async def send(self, conversation_id: UUID, message_id: UUID, sender: str, content: str) -> Message:
        """
        Persists a message atomically, updates cache, and publishes directly to Kafka.
        """
        event_id = uuid.uuid1()
        payload = {
            "conversation_id": str(conversation_id),
            "message_id": str(message_id),
            "sender": sender,
            "content": content,
            "status": "sent"
        }
        
        msg = self.repo.create_with_outbox(
            conversation_id=conversation_id,
            message_id=message_id,
            sender=sender,
            content=content,
            status="sent",
            event_id=event_id,
            event_type=KafkaTopics.CHAT_MESSAGE_CREATED,
            outbox_payload=json.dumps(payload)
        )
        
        # Invalidate history cache
        await self._invalidate_cache(conversation_id)
        
        # Direct Kafka publish
        if self.producer:
            await self.producer.publish(
                topic=KafkaTopics.CHAT_MESSAGE_CREATED,
                key=str(conversation_id),
                value=payload
            )

        # Trigger simulated LLM response in background if user sent the message
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

    async def history(self, conversation_id: UUID, limit: int = 50, cursor: Optional[UUID] = None) -> List[Message]:
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
        Soft-deletes the target assistant message and atomically stages and publishes regeneration event.
        """
        history_msgs = self.repo.history(conversation_id, limit=50)
        
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
        new_msg_id = uuid.uuid1() # generate a new response task ID
        
        payload = {
            "conversation_id": str(conversation_id),
            "message_id": str(new_msg_id),
            "sender": "assistant",
            "content": "",
            "prompt_content": prompt_msg.content if prompt_msg else "",
            "status": "pending"
        }

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
        
        # Direct Kafka publish
        if self.producer:
            await self.producer.publish(
                topic=KafkaTopics.CHAT_MESSAGE_CREATED,
                key=str(conversation_id),
                value=payload
            )

        # Trigger simulated LLM response in background
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
        Simulates downstream LLM generation by typing out a response chunk-by-chunk,
        publishing to Redis PubSub and finalising the message state in Cassandra.
        """
        # Wait a short moment to let SSE client connect
        await asyncio.sleep(1.0)
        
        full_text = f"This is a simulated AI assistant streaming response for your prompt: '{prompt}'."
        chunks = [full_text[i:i+4] for i in range(0, len(full_text), 4)]
        
        from app.services.stream_service import StreamService
        stream_service = StreamService(redis_client=self.cache.redis if self.cache else None)
        
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
