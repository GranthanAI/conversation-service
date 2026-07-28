"""
Kafka Consumer Loop.
Subscribes to inbound event topics and routes them to service-level handlers,
guaranteeing idempotency and at-least-once delivery semantics using the Inbox pattern.
"""

import asyncio
import json
import uuid
from uuid import UUID

from app.db.kafka import kafka_manager
from app.events.topics import KafkaTopics
from app.core.logging import logger

from app.repositories.inbox_repository import CassandraInboxRepository
from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.services.cache_service import CacheService
from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.db.redis import redis_manager

async def start_kafka_consumer():
    """
    Spawns the async polling consumer loop, routing events to service handlers.
    Commit offsets only after handler updates successfully complete.
    """
    consumer = kafka_manager.consumer
    if not consumer:
        logger.error("Kafka Consumer connection not active. Skipping polling loop.")
        return

    logger.info("Starting background Kafka consumer loop...")

    # Instantiate services and repos
    inbox_repo = CassandraInboxRepository()
    conv_repo = CassandraConversationRepository()
    msg_repo = CassandraMessageRepository()
    cache_service = CacheService(redis_client=redis_manager.client)

    conv_service = ConversationService(repo=conv_repo, cache_service=cache_service)
    msg_service = MessageService(repo=msg_repo, cache_service=cache_service)

    try:
        async for msg in consumer:
            try:
                # 1. Parse JSON payload envelope
                payload_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value
                
                # If already deserialized by lambda serializer
                if isinstance(payload_str, dict):
                    event = payload_str
                else:
                    event = json.loads(payload_str)

                event_id_str = event.get("event_id")
                event_type = event.get("event_type")
                inner_payload = event.get("payload", {})

                if not event_id_str:
                    logger.warning("Consumed Kafka message without event_id envelope. Skipping.", topic=msg.topic)
                    await consumer.commit()
                    continue

                event_id = UUID(event_id_str)

                # 2. Inbox Pattern: Deduplicate request to enforce effectively-once semantics
                if inbox_repo.exists(event_id):
                    logger.info("Kafka event already processed (deduplication hit). Skipping.", event_id=event_id_str)
                    await consumer.commit()
                    continue

                logger.info("Received Kafka event for consumption", topic=msg.topic, event_id=event_id_str)

                # 3. Route Event Type to Handler
                if msg.topic == KafkaTopics.TITLE_GENERATED:
                    conversation_id = UUID(inner_payload["conversation_id"])
                    title = inner_payload["title"]
                    await conv_service.set_title(conversation_id, title)
                    logger.info("Processed conversation title update from Kafka", conversation_id=str(conversation_id))

                elif msg.topic == KafkaTopics.CHAT_RESPONSE_COMPLETED:
                    conversation_id = UUID(inner_payload["conversation_id"])
                    message_id = UUID(inner_payload["message_id"])
                    full_content = inner_payload["full_content"]
                    await msg_service.finalize_assistant_message(conversation_id, message_id, full_content)
                    logger.info("Processed assistant message completion from Kafka", message_id=str(message_id))

                elif msg.topic == KafkaTopics.SUMMARY_GENERATED:
                    conversation_id = UUID(inner_payload["conversation_id"])
                    summary = inner_payload["summary"]
                    await msg_service.attach_summary(conversation_id, summary)
                    logger.info("Processed conversation summary attachment from Kafka", conversation_id=str(conversation_id))

                # 4. Mark processed in inbox
                inbox_repo.save(event_id)

                # 5. Commit offset to broker
                await consumer.commit()

            except Exception as e:
                logger.error("Failed to process consumed Kafka message", topic=msg.topic, error=str(e))
                # Do not commit to allow recovery or retry of this partition offset
                await asyncio.sleep(1) # throttled retry wait
    except asyncio.CancelledError:
        logger.info("Kafka consumer background loop shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in background Kafka consumer loop", error=str(e))
