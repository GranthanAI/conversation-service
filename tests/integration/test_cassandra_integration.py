"""
Cassandra Database Integration Tests.
Verifies real writes, reads, batch updates, and hard deletions against the local Cassandra container.
"""

import uuid
import pytest
from datetime import datetime, timezone

from app.db.cassandra import cassandra_manager
from app.repositories.conversation_repository import CassandraConversationRepository
from app.repositories.message_repository import CassandraMessageRepository
from app.repositories.outbox_repository import CassandraOutboxRepository
from app.models.conversation import ConversationStatus

@pytest.fixture(scope="module")
def setup_cassandra():
    cassandra_manager.initialize()
    yield
    # Keep initialized for other tests if needed, shutdown handled by main runner

@pytest.mark.anyio
async def test_conversation_repository_crud(setup_cassandra):
    repo = CassandraConversationRepository()
    user_id = uuid.uuid4()
    conv_id = uuid.uuid4()
    
    # 1. Create with outbox BATCH
    conv = repo.create_with_outbox(
        conversation_id=conv_id,
        user_id=user_id,
        title="Integration Test Conversation",
        status="active",
        event_id=uuid.uuid1(),
        event_type="test.event",
        outbox_payload="{}"
    )
    
    assert conv is not None
    assert conv.conversation_id == conv_id
    assert conv.title == "Integration Test Conversation"
    assert conv.status.value == "active"
    
    # 2. Get/Query metadata
    fetched = repo.get(conv_id)
    assert fetched is not None
    assert fetched.title == "Integration Test Conversation"
    
    # 3. Update Title
    success = repo.update_title(conv_id, "Updated Title")
    assert success is True
    
    fetched_updated = repo.get(conv_id)
    assert fetched_updated.title == "Updated Title"
    
    # 4. Hard Delete
    delete_success = repo.hard_delete(conv_id)
    assert delete_success is True
    
    assert repo.get(conv_id) is None

@pytest.mark.anyio
async def test_message_repository_crud(setup_cassandra):
    repo = CassandraMessageRepository()
    conv_id = uuid.uuid4()
    msg_id = uuid.uuid4()
    
    # 1. Insert message direct
    msg = repo.create_message_direct(
        conversation_id=conv_id,
        message_id=msg_id,
        sender="user",
        content="Integration message",
        status="sent"
    )
    
    assert msg.message_id == msg_id
    assert msg.content == "Integration message"
    
    # 2. Query history
    history = repo.history(conv_id, limit=10)
    assert len(history) == 1
    assert history[0].content == "Integration message"
    
    # 3. Delete messages partition
    repo.delete_all_for_conversation(conv_id)
    assert len(repo.history(conv_id, limit=10)) == 0
