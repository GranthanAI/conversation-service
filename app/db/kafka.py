"""
Kafka Connection Manager.
Only manages the low-level connection sockets lifecycles and settings
for the raw producer and consumer instances.
"""

import json
from typing import Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.core.config import settings
from app.core.logging import logger
from app.events.topics import KafkaTopics

class KafkaConnectionManager:
    """
    Manages direct connections pool lifecycles to Kafka brokers.
    """
    def __init__(self):
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None

    async def initialize(self):
        """
        Attempts broker connections and raises exceptions if startup fails (production guarantee).
        """
        # 1. Initialize Producer
        if not self.producer:
            logger.info("Initializing production Kafka Producer...", brokers=settings.KAFKA_BOOTSTRAP_SERVERS)
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    acks="all",
                    enable_idempotence=True,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    compression_type="gzip",
                    linger_ms=5,
                    max_batch_size=16384,
                    max_request_size=1048576
                )
                await self.producer.start()
                logger.info("Kafka Producer started successfully.")
            except Exception as e:
                logger.error("Failed to start Kafka Producer client", error=str(e))
                self.producer = None
                raise e

        # 2. Initialize Consumer
        if not self.consumer:
            logger.info("Initializing production Kafka Consumer...",
                        brokers=settings.KAFKA_BOOTSTRAP_SERVERS,
                        group=settings.KAFKA_CONSUMER_GROUP)
            try:
                self.consumer = AIOKafkaConsumer(
                    KafkaTopics.TITLE_GENERATED,
                    KafkaTopics.CHAT_RESPONSE_COMPLETED,
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=settings.KAFKA_CONSUMER_GROUP,
                    enable_auto_commit=False
                )
                await self.consumer.start()
                logger.info("Kafka Consumer started and subscribed to topics.")
            except Exception as e:
                logger.error("Failed to start Kafka Consumer client", error=str(e))
                self.consumer = None
                raise e

    async def check_health(self) -> bool:
        """
        Probes Kafka broker cluster metadata readiness.
        """
        if not self.producer or not self.consumer:
            logger.info("Kafka connection managers not active, attempting lazy reconnection...")
            try:
                await self.initialize()
            except Exception as e:
                logger.warning("Kafka lazy initialization failed during health probe", error=str(e))
                return False

        if not self.producer or not self.consumer:
            return False

        try:
            # Probe metadata updates from the cluster (verifies active broker response)
            prod_ok = False
            if self.producer.client:
                await self.producer.client.fetch_all_metadata()
                prod_ok = True

            cons_ok = False
            if self.consumer._client:
                await self.consumer._client.fetch_all_metadata()
                cons_ok = True

            return prod_ok and cons_ok
        except Exception as e:
            logger.warning("Kafka cluster metadata health probe failed", error=str(e))
            return False

    async def close(self):
        """
        Shuts down both producer and consumer socket connections.
        """
        if self.producer:
            try:
                await self.producer.stop()
                logger.info("Kafka Producer shutdown complete.")
            except Exception as e:
                logger.error("Error shutting down Kafka Producer", error=str(e))
            self.producer = None

        if self.consumer:
            try:
                await self.consumer.stop()
                logger.info("Kafka Consumer shutdown complete.")
            except Exception as e:
                logger.error("Error shutting down Kafka Consumer", error=str(e))
            self.consumer = None

kafka_manager = KafkaConnectionManager()
