"""
Kafka Connection and Stream Integration Tests.
Verifies real event publishing and event streaming loops against the local Kafka brokers.
"""

import uuid
import pytest
import asyncio
from aiokafka import AIOKafkaConsumer

from app.db.kafka import kafka_manager
from app.events.topics import KafkaTopics

@pytest.fixture(scope="module")
async def setup_kafka():
    await kafka_manager.initialize()
    yield
    # Shutdown handled by application lifespan

@pytest.mark.anyio
async def test_kafka_publish_and_consume(setup_kafka):
    producer = kafka_manager.producer
    assert producer is not None
    
    test_topic = KafkaTopics.CONVERSATION_CREATED
    test_key = str(uuid.uuid4())
    test_payload = {
        "event_id": str(uuid.uuid4()),
        "event_type": test_topic,
        "payload": {
            "conversation_id": test_key,
            "title": "Integration Test Topic"
        }
    }
    
    # Start a custom consumer specifically for this test topic to prevent interference
    consumer = AIOKafkaConsumer(
        test_topic,
        bootstrap_servers=kafka_manager.consumer._client._bootstrap_servers,
        group_id=f"test-group-{uuid.uuid4()}",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    
    try:
        # 1. Publish Event
        await producer.send_and_wait(
            topic=test_topic,
            key=test_key.encode("utf-8"),
            value=test_payload
        )
        
        # 2. Consume and verify
        consumed = None
        try:
            # Poll with timeout to avoid blocking forever if brokers fail
            async with asyncio.timeout(5.0):
                async for msg in consumer:
                    if msg.key.decode("utf-8") == test_key:
                        consumed = msg
                        break
        except TimeoutError:
            pytest.fail("Kafka consume timeout - message not delivered in 5 seconds.")
            
        assert consumed is not None
        
    finally:
        await consumer.stop()
