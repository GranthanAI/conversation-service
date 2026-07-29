"""
Background Workers Unit Tests.
Verifies the execution of cleanup, outbox retry, and summary workers.
"""

import json
import uuid
import asyncio
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch

from app.events.topics import KafkaTopics
from app.models.outbox import OutboxEvent
from app.workers.cleanup_worker import start_cleanup_worker
from app.workers.retry_worker import start_retry_worker
from app.workers.summary_worker import start_summary_worker

@pytest.mark.anyio
@patch("app.workers.cleanup_worker.AIOKafkaConsumer")
@patch("app.workers.cleanup_worker.CassandraConversationRepository")
@patch("app.workers.cleanup_worker.CassandraMessageRepository")
@patch("app.workers.cleanup_worker.CacheService")
async def test_cleanup_worker_purges_data(
    mock_cache_service_class,
    mock_msg_repo_class,
    mock_conv_repo_class,
    mock_consumer_class
):
    mock_consumer = AsyncMock()
    mock_consumer_class.return_value = mock_consumer
    
    mock_conv_repo = MagicMock()
    mock_conv_repo_class.return_value = mock_conv_repo
    
    mock_msg_repo = MagicMock()
    mock_msg_repo_class.return_value = mock_msg_repo
    
    mock_cache_service = AsyncMock()
    mock_cache_service_class.return_value = mock_cache_service
    
    conv_id = uuid.uuid4()
    event_payload = {
        "event_id": str(uuid.uuid4()),
        "event_type": KafkaTopics.CONVERSATION_DELETED,
        "payload": {
            "conversation_id": str(conv_id)
        }
    }
    
    # Mock message retrieval envelope
    mock_msg = MagicMock()
    mock_msg.value = json.dumps(event_payload).encode("utf-8")
    
    # Mock consumer iteration to yield one message and then raise CancelledError
    async def mock_iter():
        yield mock_msg
        raise asyncio.CancelledError()
        
    mock_consumer.__aiter__.side_effect = mock_iter
    
    await start_cleanup_worker()
        
    # Assert DB hard purges and cache invalidations were invoked
    mock_cache_service.delete_conversation.assert_called_once_with(conv_id)
    mock_cache_service.delete_last_50_messages.assert_called_once_with(conv_id)
    mock_msg_repo.delete_all_for_conversation.assert_called_once_with(conv_id)
    mock_conv_repo.hard_delete.assert_called_once_with(conv_id)
    mock_consumer.commit.assert_called_once()

@pytest.mark.anyio
@patch("app.workers.retry_worker.CassandraOutboxRepository")
@patch("app.workers.retry_worker.kafka_producer_client")
async def test_retry_worker_reconciles_stale_events(
    mock_producer_client,
    mock_outbox_repo_class
):
    mock_repo = MagicMock()
    mock_outbox_repo_class.return_value = mock_repo
    
    stale_event_id = uuid.uuid1()
    new_event_id = uuid.uuid1()
    conv_id = uuid.uuid4()
    
    payload_str = json.dumps({"conversation_id": str(conv_id)})
    
    stale_time = datetime.now(timezone.utc) - timedelta(seconds=45)
    new_time = datetime.now(timezone.utc) - timedelta(seconds=5)
    
    # Bucket 0 has two events: one stale, one new
    stale_event = OutboxEvent(
        bucket=0,
        event_id=stale_event_id,
        event_type=KafkaTopics.CONVERSATION_CREATED,
        payload=payload_str,
        published=False,
        created_at=stale_time
    )
    new_event = OutboxEvent(
        bucket=0,
        event_id=new_event_id,
        event_type=KafkaTopics.CONVERSATION_CREATED,
        payload=payload_str,
        published=False,
        created_at=new_time
    )
    
    mock_repo.fetch_unpublished.side_effect = lambda bucket, limit: (
        [stale_event, new_event] if bucket == 0 else []
    )
    
    mock_producer_client.publish = AsyncMock()
    
    # Interrupt infinite loop on sleep
    with patch("asyncio.sleep", side_effect=asyncio.CancelledError):
        await start_retry_worker()
            
    # Verify ONLY the stale event was re-published and marked
    mock_producer_client.publish.assert_called_once_with(
        topic=KafkaTopics.CONVERSATION_CREATED,
        key=str(conv_id),
        value={"conversation_id": str(conv_id)}
    )
    mock_repo.mark_published.assert_called_once_with(0, stale_event_id)

@pytest.mark.anyio
@patch("app.workers.summary_worker.AIOKafkaConsumer")
@patch("app.workers.summary_worker.CassandraInboxRepository")
@patch("app.workers.summary_worker.MessageService")
async def test_summary_worker_attaches_summary(
    mock_msg_service_class,
    mock_inbox_repo_class,
    mock_consumer_class
):
    mock_consumer = AsyncMock()
    mock_consumer_class.return_value = mock_consumer
    
    mock_inbox = MagicMock()
    mock_inbox_repo_class.return_value = mock_inbox
    mock_inbox.exists.return_value = False # Not processed yet
    
    mock_msg_service = AsyncMock()
    mock_msg_service_class.return_value = mock_msg_service
    
    event_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    event_payload = {
        "event_id": str(event_id),
        "event_type": KafkaTopics.SUMMARY_GENERATED,
        "payload": {
            "conversation_id": str(conv_id),
            "summary": "This is a summary."
        }
    }
    
    mock_msg = MagicMock()
    mock_msg.value = json.dumps(event_payload).encode("utf-8")
    
    async def mock_iter():
        yield mock_msg
        raise asyncio.CancelledError()
        
    mock_consumer.__aiter__.side_effect = mock_iter
    
    await start_summary_worker()
        
    # Verify summary save, inbox registration, and offsets commit
    mock_msg_service.attach_summary.assert_called_once_with(conv_id, "This is a summary.")
    mock_inbox.save.assert_called_once_with(event_id)
    mock_consumer.commit.assert_called_once()
