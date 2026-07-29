"""
Cache Cleanup Background Worker.
Consumes conversation.deleted events and purges database rows and Redis keys.
"""

import asyncio
import json
from uuid import UUID
from aiokafka import AIOKafkaConsumer

from app.core.config import settings
from app.core.logging import logger
from app.events.topics import KafkaTopics
from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.services.cache_service import CacheService
from app.db.redis import redis_manager

async def start_cleanup_worker():
    """
    Subscribes to conversation.deleted topic and purges all conversation
    and message records from Cassandra and Redis.
    """
    logger.info("Starting background Cleanup Worker consumer...")
    
    # Instantiate repos and services locally to prevent startup ordering issues
    conv_repo = CassandraConversationRepository()
    msg_repo = CassandraMessageRepository()
    cache_service = CacheService(redis_client=redis_manager.client)
    
    consumer = AIOKafkaConsumer(
        KafkaTopics.CONVERSATION_DELETED,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{settings.KAFKA_CONSUMER_GROUP}-cleanup",
        enable_auto_commit=False
    )
    
    try:
        await consumer.start()
        logger.info("Cleanup Worker consumer connected to Kafka successfully.")
        
        async for msg in consumer:
            try:
                payload_str = msg.value.decode("utf-8") if isinstance(msg.value, bytes) else msg.value
                if isinstance(payload_str, dict):
                    event = payload_str
                else:
                    event = json.loads(payload_str)
                    
                inner_payload = event.get("payload", {})
                conv_id_str = inner_payload.get("conversation_id")
                
                if not conv_id_str:
                    logger.warning("Cleanup Worker consumed message without conversation_id. Skipping.")
                    await consumer.commit()
                    continue
                    
                conversation_id = UUID(conv_id_str)
                logger.info("Cleanup Worker processing hard deletion for conversation", conversation_id=str(conversation_id))
                
                # 1. Purge Redis Cache keys
                await cache_service.delete_conversation(conversation_id)
                await cache_service.delete_last_50_messages(conversation_id)
                
                # 2. Hard delete conversation messages
                msg_repo.delete_all_for_conversation(conversation_id)
                
                # 3. Hard delete conversation metadata
                conv_repo.hard_delete(conversation_id)
                
                logger.info("Successfully hard purged conversation data from databases and cache", conversation_id=str(conversation_id))
                
                # Commit offset
                await consumer.commit()
                
            except Exception as e:
                logger.error("Error in Cleanup Worker processing message. Skipping to avoid blocking.", error=str(e))
                await consumer.commit()
                
    except asyncio.CancelledError:
        logger.info("Cleanup Worker background loop shutdown signal received.")
    except Exception as e:
        logger.error("Fatal error in background Cleanup Worker loop", error=str(e))
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass
