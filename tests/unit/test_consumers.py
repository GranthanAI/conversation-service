"""
Kafka Consumer Unit Tests.
Verifies event polling loops, Inbox pattern deduplication, and service updates integration.
"""

import json
import uuid
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, AsyncMock, patch

from app.events.consumers import start_kafka_consumer
from app.events.topics import KafkaTopics

class MockKafkaMessage:
    def __init__(self, topic, value):
        self.topic = topic
        self.value = value

class MockConsumer:
    def __init__(self, msg):
        self.msg = msg
        self.committed = False

    async def __aiter__(self):
        yield self.msg

    async def commit(self):
        self.committed = True

@pytest.mark.anyio
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_processes_title_generated(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class
):
    # Set up mocks
    mock_inbox_repo = MagicMock()
    mock_inbox_repo.exists.return_value = False
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_conv_repo = MagicMock()
    mock_conv_repo.update_title.return_value = True
    mock_conv_repo_class.return_value = mock_conv_repo

    mock_msg_repo = MagicMock()
    mock_msg_repo_class.return_value = mock_msg_repo

    mock_cache_service = AsyncMock()
    mock_cache_service_class.return_value = mock_cache_service

    # Setup mock consumer iteration
    event_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    
    event_payload = {
        "event_id": str(event_id),
        "event_type": KafkaTopics.TITLE_GENERATED,
        "payload": {
            "conversation_id": str(conv_id),
            "title": "Quantum AI Title"
        }
    }
    
    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.TITLE_GENERATED,
        value=event_payload
    )
    
    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    # Run loop
    await start_kafka_consumer()

    # Assertions
    mock_inbox_repo.exists.assert_called_once_with(event_id)
    mock_conv_repo.update_title.assert_called_once_with(conv_id, "Quantum AI Title")
    mock_cache_service.delete_conversation.assert_called_once_with(conv_id)
    mock_inbox_repo.save.assert_called_once_with(event_id)
    assert mock_consumer.committed is True

@pytest.mark.anyio
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_processes_response_completed(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class
):
    mock_inbox_repo = MagicMock()
    mock_inbox_repo.exists.return_value = False
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_conv_repo = MagicMock()
    mock_conv_repo_class.return_value = mock_conv_repo

    mock_msg_repo = MagicMock()
    mock_msg_repo.create_message_direct.return_value = MagicMock()
    mock_msg_repo_class.return_value = mock_msg_repo

    mock_cache_service = AsyncMock()
    mock_cache_service_class.return_value = mock_cache_service

    event_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    event_payload = {
        "event_id": str(event_id),
        "event_type": KafkaTopics.CHAT_RESPONSE_COMPLETED,
        "payload": {
            "conversation_id": str(conv_id),
            "message_id": str(msg_id),
            "full_content": "Final AI response text content"
        }
    }
    
    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.CHAT_RESPONSE_COMPLETED,
        value=event_payload
    )
    
    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    await start_kafka_consumer()

    mock_inbox_repo.exists.assert_called_once_with(event_id)
    mock_msg_repo.create_message_direct.assert_called_once_with(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="assistant",
        content="Final AI response text content",
        status="sent"
    )
    mock_cache_service.delete_last_50_messages.assert_called_once_with(conv_id)
    mock_inbox_repo.save.assert_called_once_with(event_id)
    assert mock_consumer.committed is True


@pytest.mark.anyio
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_deduplicates_events(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class
):
    mock_inbox_repo = MagicMock()
    # Mock deduplication: exists returns True!
    mock_inbox_repo.exists.return_value = True
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_conv_repo = MagicMock()
    mock_conv_repo_class.return_value = mock_conv_repo

    event_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    
    event_payload = {
        "event_id": str(event_id),
        "event_type": KafkaTopics.TITLE_GENERATED,
        "payload": {
            "conversation_id": str(conv_id),
            "title": "Dup Title"
        }
    }
    
    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.TITLE_GENERATED,
        value=event_payload
    )
    
    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    await start_kafka_consumer()

    mock_inbox_repo.exists.assert_called_once_with(event_id)
    # The handler should skip updating DB/cache/saving to inbox because it was deduplicated
    mock_conv_repo.update_title.assert_not_called()
    mock_inbox_repo.save.assert_not_called()
    # But it must commit the offset to advance the consumer
    assert mock_consumer.committed is True
