import pytest
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID
from core.enums import ConversationStatus
from domain.entities.conversation import ConversationEntity
from domain.exceptions import NotFoundError, OwnershipError
from services.conversation_service import ConversationService

@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.create_conversation = AsyncMock()
    repo.get_conversation = AsyncMock()
    repo.list_conversations_by_user = AsyncMock()
    repo.update_conversation_status = AsyncMock()
    repo.update_conversation_title = AsyncMock()
    return repo

@pytest.fixture
def mock_cache():
    cache = MagicMock()
    cache.get_conversation = AsyncMock(return_value=None)
    cache.set_conversation = AsyncMock()
    cache.invalidate_conversation = AsyncMock()
    return cache

@pytest.fixture
def service(mock_repo, mock_cache):
    return ConversationService(repo=mock_repo, cache=mock_cache)

@pytest.mark.asyncio
async def test_create_conversation(service, mock_repo, mock_cache):
    user_id = uuid.uuid4()
    title = "Test Conversation"
    
    # Mock repo response
    mock_entity = ConversationEntity(
        conversation_id=uuid.uuid4(),
        user_id=user_id,
        title=title,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        status=ConversationStatus.ACTIVE
    )
    mock_repo.create_conversation.return_value = mock_entity

    result = await service.create_conversation(user_id, title)

    assert result == mock_entity
    mock_repo.create_conversation.assert_called_once()
    mock_cache.set_conversation.assert_called_once_with(mock_entity)

@pytest.mark.asyncio
async def test_get_conversation_cache_hit(service, mock_cache):
    user_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    mock_entity = ConversationEntity(
        conversation_id=conv_id,
        user_id=user_id,
        title="Cached Conv",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        status=ConversationStatus.ACTIVE
    )
    mock_cache.get_conversation.return_value = mock_entity

    result = await service.get_conversation(conv_id, user_id)

    assert result == mock_entity
    mock_cache.get_conversation.assert_called_once_with(conv_id)

@pytest.mark.asyncio
async def test_get_conversation_ownership_error(service, mock_cache):
    user_id = uuid.uuid4()
    other_user_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    mock_entity = ConversationEntity(
        conversation_id=conv_id,
        user_id=other_user_id, # Ownership mismatch
        title="Other User Conv",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        status=ConversationStatus.ACTIVE
    )
    mock_cache.get_conversation.return_value = mock_entity

    with pytest.raises(OwnershipError):
        await service.get_conversation(conv_id, user_id)

@pytest.mark.asyncio
async def test_get_conversation_not_found(service, mock_repo, mock_cache):
    user_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    mock_repo.get_conversation.return_value = None

    with pytest.raises(NotFoundError):
        await service.get_conversation(conv_id, user_id)
