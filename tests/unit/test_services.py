"""
Service Layer Unit Tests.
Mocks repository queries and Redis Cache-Aside loops to verify orchestration.
"""

# --- Python 3.12 Compatibility Patch for Cassandra Driver ---
import sys
import types
asyncore_mock = types.ModuleType("asyncore")
class DummyDispatcher:
    pass
asyncore_mock.dispatcher = DummyDispatcher
sys.modules['asyncore'] = asyncore_mock
# -------------------------------------------------------------

import json
import pytest
from unittest.mock import MagicMock, AsyncMock, ANY, patch
import uuid
from datetime import datetime, timezone

from app.services.conversation_service import ConversationService
from app.services.message_service import MessageService
from app.services.cache_service import CacheService
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus

@pytest.fixture
def mock_repos():
    """
    Creates mock repositories for service DI.
    """
    mock_conv_repo = MagicMock()
    mock_msg_repo = MagicMock()
    return mock_conv_repo, mock_msg_repo

@pytest.fixture
def mock_redis_client():
    """
    Creates mock async Redis client.
    """
    mock_client = AsyncMock()
    return mock_client

@pytest.mark.anyio
async def test_conversation_service_create(mock_repos):
    mock_conv_repo, _ = mock_repos
    service = ConversationService(repo=mock_conv_repo)
    
    user_id = uuid.uuid4()
    mock_conv_repo.create_with_outbox.return_value = Conversation(
        conversation_id=uuid.uuid4(),
        user_id=user_id,
        title="Test title",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    )
    
    conv = await service.create(user_id, "Test title")
    
    assert conv.user_id == user_id
    mock_conv_repo.create_with_outbox.assert_called_once()

@pytest.mark.anyio
async def test_conversation_service_rename(mock_repos):
    mock_conv_repo, _ = mock_repos
    service = ConversationService(repo=mock_conv_repo)
    
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    # Mock lookup get
    mock_conv_repo.get.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="Old Title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    # Mock update_with_outbox
    mock_conv_repo.update_with_outbox.return_value = Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="New Title",
        created_at=now,
        updated_at=now,
        status=ConversationStatus.ACTIVE
    )
    
    updated = await service.rename(conv_id, "New Title")
    assert updated is not None
    assert updated.title == "New Title"
    mock_conv_repo.get.assert_called_once_with(conv_id)
    mock_conv_repo.update_with_outbox.assert_called_once()

@pytest.mark.anyio
async def test_message_service_send(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))
    
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Test content",
        created_at=datetime.now(timezone.utc),
        status=MessageStatus.SENT
    )
    
    msg = await service.send(conv_id, msg_id, "user", "Test content")

    assert msg.message_id == msg_id
    mock_msg_repo.create_with_outbox.assert_called_once()
    mock_redis_client.delete.assert_called_once_with(f"conversation:{conv_id}:last50")

@pytest.mark.anyio
async def test_message_service_send_with_file_ids(mock_repos, mock_redis_client):
    """
    file_ids must reach the repository call AND the outbox Kafka payload
    (as strings, since json.dumps can't serialize UUID objects directly).
    """
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))

    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    file_id_1, file_id_2 = uuid.uuid4(), uuid.uuid4()

    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Explain this PDF",
        created_at=datetime.now(timezone.utc),
        status=MessageStatus.SENT,
        file_ids=[file_id_1, file_id_2]
    )

    msg = await service.send(conv_id, msg_id, "user", "Explain this PDF", file_ids=[file_id_1, file_id_2])

    assert msg.file_ids == [file_id_1, file_id_2]

    _, call_kwargs = mock_msg_repo.create_with_outbox.call_args
    assert call_kwargs["file_ids"] == [file_id_1, file_id_2]

    published_payload = json.loads(call_kwargs["outbox_payload"])
    assert published_payload["file_ids"] == [str(file_id_1), str(file_id_2)]


@pytest.mark.anyio
async def test_message_service_send_without_file_ids_defaults_to_empty(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))

    conv_id, msg_id = uuid.uuid4(), uuid.uuid4()
    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id, message_id=msg_id, sender="user", content="Hello",
        created_at=datetime.now(timezone.utc), status=MessageStatus.SENT
    )

    await service.send(conv_id, msg_id, "user", "Hello")

    _, call_kwargs = mock_msg_repo.create_with_outbox.call_args
    assert call_kwargs["file_ids"] is None
    published_payload = json.loads(call_kwargs["outbox_payload"])
    assert published_payload["file_ids"] == []


@pytest.mark.anyio
async def test_message_service_regenerate_forwards_prompt_file_ids(mock_repos, mock_redis_client):
    """
    regenerate() re-derives the response from the prior prompt message —
    that prompt's file_ids must carry forward into the new chat.message.created
    payload so the regenerated response is grounded on the same documents.
    """
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))

    conv_id = uuid.uuid4()
    assistant_msg_id = uuid.uuid4()
    prompt_file_id = uuid.uuid4()
    now = datetime.now(timezone.utc)

    assistant_msg = Message(
        conversation_id=conv_id, message_id=assistant_msg_id, sender="assistant",
        content="Old response", created_at=now, status=MessageStatus.SENT
    )
    prompt_msg = Message(
        conversation_id=conv_id, message_id=uuid.uuid4(), sender="user",
        content="Explain this PDF", created_at=now, status=MessageStatus.SENT,
        file_ids=[prompt_file_id]
    )
    mock_msg_repo.history.return_value = [assistant_msg, prompt_msg]
    mock_msg_repo.create_with_outbox.return_value = Message(
        conversation_id=conv_id, message_id=uuid.uuid4(), sender="assistant",
        content="", created_at=now, status=MessageStatus.PENDING
    )

    await service.regenerate(conv_id, assistant_msg_id)

    _, call_kwargs = mock_msg_repo.create_with_outbox.call_args
    published_payload = json.loads(call_kwargs["outbox_payload"])
    assert published_payload["file_ids"] == [str(prompt_file_id)]


@pytest.mark.anyio
async def test_message_service_history_cache_hit(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))
    
    conv_id = uuid.uuid4()
    
    # Mock cache hit returning serialized items
    mock_redis_client.lrange.return_value = [
        '{"conversation_id": "' + str(conv_id) + '", "message_id": "' + str(uuid.uuid4()) + '", "sender": "user", "content": "Cache Hello", "created_at": "2026-07-27T00:00:00Z", "status": "sent"}'
    ]
    
    history = await service.history(conv_id)
    assert len(history) == 1
    assert history[0].content == "Cache Hello"
    assert history[0].file_ids == []  # a cached blob written before file_ids existed must still parse
    mock_redis_client.lrange.assert_called_once()
    mock_msg_repo.history.assert_not_called()

@pytest.mark.anyio
async def test_message_service_history_cache_hit_with_file_ids(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))

    conv_id = uuid.uuid4()
    file_id = uuid.uuid4()

    mock_redis_client.lrange.return_value = [
        json.dumps({
            "conversation_id": str(conv_id), "message_id": str(uuid.uuid4()), "sender": "user",
            "content": "Explain this PDF", "created_at": "2026-07-27T00:00:00Z", "status": "sent",
            "file_ids": [str(file_id)]
        })
    ]

    history = await service.history(conv_id)
    assert history[0].file_ids == [file_id]

@pytest.mark.anyio
async def test_message_service_history_cache_miss(mock_repos, mock_redis_client):
    _, mock_msg_repo = mock_repos
    service = MessageService(repo=mock_msg_repo, cache_service=CacheService(redis_client=mock_redis_client))
    
    conv_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    # Mock cache miss
    mock_redis_client.lrange.return_value = []
    # Mock DB return
    mock_msg_repo.history.return_value = [
        Message(conversation_id=conv_id, message_id=uuid.uuid4(), sender="user", content="DB Hello", created_at=now, status=MessageStatus.SENT)
    ]
    
    history = await service.history(conv_id)
    assert len(history) == 1
    assert history[0].content == "DB Hello"
    mock_redis_client.lrange.assert_called_once()
    mock_msg_repo.history.assert_called_once_with(conv_id, 50, None)
    mock_redis_client.rpush.assert_called_once()
    mock_redis_client.expire.assert_called_once()
