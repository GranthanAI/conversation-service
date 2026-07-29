"""
Conversation Summary Background Worker.
Consumes conversation.summary.generated events and attaches summaries to conversations.
"""

import asyncio
import json
from uuid import UUID
from aiokafka import AIOKafkaConsumer

from app.core.config import settings
from app.core.logging import logger
from app.events.topics import KafkaTopics
from app.repositories.inbox_repository import CassandraInboxRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.services.cache_service import CacheService
from app.services.message_service import MessageService
from app.db.redis import redis_manager

async def start_summary_worker():
    """
    Subscribes to conversation.summary.generated topic and registers summaries in the message log.
    Uses the Inbox pattern to ensure idempotency.
    """
    logger.info("Starting background Summary Worker consumer...")
    
    # Instantiate repos and services locally
    inbox_repo = CassandraInboxRepository()
    msg_repo = CassandraMessageRepository()
    cache_service = CacheService(redis_client=redis_manager.client)
    msg_service = MessageService(repo=msg_repo, cache_service=cache_service)
    
    consumer = AIOKafkaConsumer(
        KafkaTopics.SUMMARY_GENERATED,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{settings.KAFKA_CONSUMER_GROUP}-summary",
        enable_auto_commit=False
    )
    
    try:
        await consumer.start()
        logger.info("Summary Worker consumer connected to Kafka successfully.")
        
        async for msg in consumer:
            try:
                payload_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value
                if isinstance(payload_str, dict):
                    event = payload_str
                else:
                    event = json.loads(payload_str)
                    
                event_id_str = event.get("event_id")
                inner_payload = event.get("payload", {})
                
                if not event_id_str:
                    logger.warning("Summary Worker consumed message without event_id. Skipping.")
                    await consumer.commit()
                    continue
                    
                event_id = UUID(event_id_str)
                
                # Inbox Pattern Deduplication
                if inbox_repo.exists(event_id):
                    logger.info("Summary Worker event already processed (deduplication hit).", event_id=event_id_str)
                    await consumer.commit()
                    continue
                    
                conversation_id_str = inner_payload.get("conversation_id")
                summary = inner_payload.get("summary")
                
                if not conversation_id_str or not summary:
                    logger.warning("Summary Worker received event missing required fields.", event_id=event_id_str)
                    await consumer.commit()
                    continue
                    
                conversation_id = UUID(conversation_id_str)
                logger.info("Summary Worker processing summary attachment", conversation_id=conversation_id_str)
                
                # Save system summary message in Cassandra and evict cache
                await msg_service.attach_summary(conversation_id, summary)
                
                # Mark event as processed in Inbox table
                inbox_repo.save(event_id)
                
                logger.info("Successfully registered conversation summary", conversation_id=conversation_id_str)
                await consumer.commit()
                
            except Exception as e:
                logger.error("Error in Summary Worker processing message. Retrying...", error=str(e))
                await asyncio.sleep(settings.SUMMARY_WORKER_ERROR_SLEEP_SECONDS)
                
    except asyncio.CancelledError:
        logger.info("Summary Worker background loop shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in background Summary Worker loop", error=str(e))
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass
