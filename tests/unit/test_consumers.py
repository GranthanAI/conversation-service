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
@patch("app.events.consumers.StreamService")
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_processes_chat_response_generated_success(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class,
    mock_stream_service_class,
):
    mock_inbox_repo = MagicMock()
    mock_inbox_repo.exists.return_value = False
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_conv_repo = MagicMock()
    mock_conv_repo_class.return_value = mock_conv_repo

    mock_msg_repo = MagicMock()
    mock_msg_repo.create_message_direct.return_value = MagicMock()
    mock_msg_repo.get_message.return_value = None
    mock_msg_repo_class.return_value = mock_msg_repo

    mock_cache_service = AsyncMock()
    mock_cache_service_class.return_value = mock_cache_service

    mock_stream_service = AsyncMock()
    mock_stream_service_class.return_value = mock_stream_service

    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()  # request / user message id
    resp_id = uuid.uuid4()  # pending assistant message id

    # Flat, envelope-less shape matching llm-service's ChatResponseGeneratedEvent
    event_payload = {
        "event_type": "chat.response.generated",
        "response_id": "resp_1",
        "conversation_id": str(conv_id),
        "user_id": str(uuid.uuid4()),
        "request_message_id": str(msg_id),
        "response_message_id": str(resp_id),
        "full_content": "Final AI response text content",
        "status": "success",
    }

    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.CHAT_RESPONSE_GENERATED,
        value=event_payload
    )

    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    await start_kafka_consumer()

    mock_msg_repo.create_message_direct.assert_called_once_with(
        conversation_id=conv_id,
        message_id=resp_id,
        sender="assistant",
        content="Final AI response text content",
        status="sent"
    )
    mock_cache_service.delete_last_50_messages.assert_called_once_with(conv_id)
    mock_stream_service.publish_token.assert_called_once()
    assert mock_consumer.committed is True


@pytest.mark.anyio
@patch("app.events.consumers.StreamService")
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_processes_chat_response_generated_error(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class,
    mock_stream_service_class,
):
    mock_inbox_repo = MagicMock()
    mock_inbox_repo.exists.return_value = False
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_msg_repo = MagicMock()
    mock_msg_repo.get_message.return_value = None
    mock_msg_repo_class.return_value = mock_msg_repo

    mock_cache_service = AsyncMock()
    mock_cache_service_class.return_value = mock_cache_service

    mock_stream_service = AsyncMock()
    mock_stream_service_class.return_value = mock_stream_service

    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()  # request / user message id
    resp_id = uuid.uuid4()  # pending assistant message id

    event_payload = {
        "event_type": "chat.response.generated",
        "conversation_id": str(conv_id),
        "request_message_id": str(msg_id),
        "response_message_id": str(resp_id),
        "status": "error",
        "error_message": "Upstream provider failure",
    }

    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.CHAT_RESPONSE_GENERATED,
        value=event_payload
    )

    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    await start_kafka_consumer()

    mock_msg_repo.create_message_direct.assert_called_once_with(
        conversation_id=conv_id,
        message_id=resp_id,
        sender="assistant",
        content="Upstream provider failure",
        status="failed"
    )
    published_payload = mock_stream_service.publish_token.call_args.args[1]
    assert published_payload["is_final"] is True
    assert published_payload["status"] == "failed"
    assert mock_consumer.committed is True


@pytest.mark.anyio
@patch("app.events.consumers.StreamService")
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_forwards_chat_response_chunk(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class,
    mock_stream_service_class,
):
    mock_inbox_repo = MagicMock()
    mock_inbox_repo.exists.return_value = False
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_cache_service = AsyncMock()
    mock_cache_service_class.return_value = mock_cache_service

    mock_stream_service = AsyncMock()
    mock_stream_service_class.return_value = mock_stream_service

    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()  # request / user message id
    resp_id = uuid.uuid4()  # pending assistant message id

    event_payload = {
        "event_type": "chat.response.chunk",
        "conversation_id": str(conv_id),
        "message_id": str(msg_id),
        "response_message_id": str(resp_id),
        "chunk_index": 0,
        "sequence_number": 1,
        "content": "Hello",
        "is_last": False,
    }

    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.CHAT_RESPONSE_CHUNK,
        value=event_payload
    )

    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    await start_kafka_consumer()

    mock_stream_service.publish_token.assert_called_once()
    published_conv_id, published_payload = mock_stream_service.publish_token.call_args.args
    assert published_conv_id == conv_id
    assert published_payload["content"] == "Hello"
    assert published_payload["message_id"] == str(resp_id)
    assert published_payload["request_message_id"] == str(msg_id)
    assert published_payload["is_final"] is False
    assert mock_consumer.committed is True


@pytest.mark.anyio
@patch("app.events.consumers.StreamService")
@patch("app.events.consumers.CassandraInboxRepository")
@patch("app.events.consumers.CassandraConversationRepository")
@patch("app.events.consumers.CassandraMessageRepository")
@patch("app.events.consumers.CacheService")
@patch("app.events.consumers.kafka_manager")
async def test_consumer_loop_deduplicates_chat_response_chunk(
    mock_kafka_manager_class,
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_inbox_repo_class,
    mock_stream_service_class,
):
    mock_inbox_repo = MagicMock()
    # Already processed — dedup hit
    mock_inbox_repo.exists.return_value = True
    mock_inbox_repo_class.return_value = mock_inbox_repo

    mock_stream_service = AsyncMock()
    mock_stream_service_class.return_value = mock_stream_service

    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()

    event_payload = {
        "event_type": "chat.response.chunk",
        "conversation_id": str(conv_id),
        "message_id": str(msg_id),
        "sequence_number": 1,
        "content": "Hello",
        "is_last": False,
    }

    mock_msg = MockKafkaMessage(
        topic=KafkaTopics.CHAT_RESPONSE_CHUNK,
        value=event_payload
    )

    mock_consumer = MockConsumer(mock_msg)
    mock_kafka_manager_class.consumer = mock_consumer

    await start_kafka_consumer()

    mock_stream_service.publish_token.assert_not_called()
    mock_inbox_repo.save.assert_not_called()
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
