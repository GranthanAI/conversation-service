"""
Kafka Event Producer Wrapper.
Publishes domain event payloads using the connection manager.
"""

from typing import Dict, Any
from app.db.kafka import kafka_manager
from app.core.logging import logger

class KafkaProducerClient:
    """
    Client wrapper isolating business services from raw Kafka producer instances.
    """
    def __init__(self):
        self.manager = kafka_manager

    async def publish(self, topic: str, key: str, value: Dict[str, Any]):
        """
        Publishes a message dict using the partition routing key.
        The serialization is handled natively by the producer's configured value_serializer.
        """
        if not self.manager.producer:
            logger.error("Kafka connection pool not initialized. Cannot publish.", topic=topic)
            raise RuntimeError("Kafka Connection Manager is down")

        try:
            # Key must be bytes
            key_bytes = key.encode("utf-8") if isinstance(key, str) else str(key).encode("utf-8")
            
            logger.info("Publishing event to Kafka topic...", topic=topic, key=key)
            # Send payload directly (serializer automatically applies)
            await self.manager.producer.send_and_wait(
                topic=topic,
                key=key_bytes,
                value=value
            )
            logger.info("Published event successfully.", topic=topic)
        except Exception as e:
            logger.error("Error publishing message to Kafka broker", topic=topic, error=str(e))
            raise e

kafka_producer_client = KafkaProducerClient()
