"""
Repository Layer Unit Tests.
Mocks the Cassandra session connection to verify correct statement compilation,
binding parameter parsing, and model deserialization.
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

import pytest
from unittest.mock import MagicMock, patch, ANY
import uuid
from datetime import datetime, timezone

from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.repositories.inbox_repository import CassandraInboxRepository
from app.models.conversation import ConversationStatus
from app.models.message import MessageStatus

@pytest.fixture
def mock_cassandra_manager():
    """
    Mocks CassandraConnectionManager session driver hook.
    """
    with patch("app.repositories.conversation_repository.cassandra_manager") as mock_mgr_conv, \
         patch("app.repositories.message_repository.cassandra_manager") as mock_mgr_msg, \
         patch("app.repositories.outbox_repository.cassandra_manager") as mock_mgr_out, \
         patch("app.repositories.inbox_repository.cassandra_manager") as mock_mgr_in:
        
        # Share the same mock session
        mock_session = MagicMock()
        mock_mgr_conv.session = mock_session
        mock_mgr_msg.session = mock_session
        mock_mgr_out.session = mock_session
        mock_mgr_in.session = mock_session
        
        yield mock_session

def test_conversation_repository_create_with_outbox(mock_cassandra_manager):
    repo = CassandraConversationRepository()
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    event_id = uuid.uuid1()
    
    mock_prepared = MagicMock()
    mock_cassandra_manager.prepare.return_value = mock_prepared
    
    conv = repo.create_with_outbox(
        conversation_id=conv_id,
        user_id=user_id,
        title="Hello Test",
        status="active",
        event_id=event_id,
        event_type="conversation.created",
        outbox_payload="{}"
    )
    
    assert conv.conversation_id == conv_id
    assert conv.user_id == user_id
    assert conv.title == "Hello Test"
    assert conv.status == ConversationStatus.ACTIVE
    mock_cassandra_manager.prepare.assert_called_once()
    mock_cassandra_manager.execute.assert_called_once_with(mock_prepared, ANY)

def test_conversation_repository_get(mock_cassandra_manager):
    repo = CassandraConversationRepository()
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    mock_row = MagicMock()
    mock_row.conversation_id = conv_id
    mock_row.user_id = user_id
    mock_row.title = "Hello Test"
    mock_row.created_at = now
    mock_row.updated_at = now
    mock_row.status = "active"
    
    mock_cassandra_manager.execute.return_value.one.return_value = mock_row
    
    conv = repo.get(conv_id)
    assert conv is not None
    assert conv.conversation_id == conv_id
    assert conv.title == "Hello Test"

def test_conversation_repository_update_with_outbox(mock_cassandra_manager):
    repo = CassandraConversationRepository()
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    event_id = uuid.uuid1()
    
    # Mock row for initial get() query
    mock_row = MagicMock()
    mock_row.conversation_id = conv_id
    mock_row.user_id = user_id
    mock_row.title = "Old Title"
    mock_row.created_at = now
    mock_row.updated_at = now
    mock_row.status = "active"
    
    mock_cassandra_manager.execute.return_value.one.return_value = mock_row
    
    # Execute update
    updated_conv = repo.update_with_outbox(
        conversation_id=conv_id,
        title="New Title",
        status="archived",
        event_id=event_id,
        event_type="conversation.updated",
        outbox_payload="{}"
    )
    assert updated_conv is not None
    assert updated_conv.title == "New Title"
    assert updated_conv.status == ConversationStatus.ARCHIVED

def test_message_repository_create_with_outbox(mock_cassandra_manager):
    repo = CassandraMessageRepository()
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    event_id = uuid.uuid1()
    
    msg = repo.create_with_outbox(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Message content",
        status="sent",
        event_id=event_id,
        event_type="chat.message.created",
        outbox_payload="{}"
    )
    assert msg.conversation_id == conv_id
    assert msg.message_id == msg_id
    assert msg.content == "Message content"
    assert msg.status == MessageStatus.SENT

def test_outbox_repository_save(mock_cassandra_manager):
    repo = CassandraOutboxRepository()
    event_id = uuid.uuid1()
    
    event = repo.save(0, event_id, "chat.message.created", '{"data": 1}')
    assert event.bucket == 0
    assert event.event_id == event_id
    assert event.event_type == "chat.message.created"
    assert event.published is False

def test_inbox_repository_exists(mock_cassandra_manager):
    repo = CassandraInboxRepository()
    event_id = uuid.uuid4()
    
    mock_cassandra_manager.execute.return_value.one.return_value = None
    assert repo.exists(event_id) is False
    
    mock_cassandra_manager.execute.return_value.one.return_value = MagicMock()
    assert repo.exists(event_id) is True
