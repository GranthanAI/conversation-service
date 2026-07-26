"""
Cassandra Schema CRUD Verification Test.
Executes inserts, updates, queries, and deletions against all 5 schema tables
to confirm persistence health.
"""

# --- Python 3.12 Compatibility Patch for Cassandra Driver ---
import sys
import os
sys.path.append(os.getcwd())
import types
asyncore_mock = types.ModuleType("asyncore")
class DummyDispatcher:
    pass
asyncore_mock.dispatcher = DummyDispatcher
sys.modules['asyncore'] = asyncore_mock
# -------------------------------------------------------------

import uuid
from datetime import datetime, timezone
from app.db.cassandra import cassandra_manager
from app.core.logging import logger

def test_cassandra_crud():
    logger.info("Initializing Cassandra Connection Manager...")
    cassandra_manager.initialize()
    
    session = cassandra_manager.session
    if not session:
        logger.error("Failed to acquire active Cassandra session. Exiting.")
        sys.exit(1)
        
    try:
        conversation_id = uuid.uuid4()
        user_id = uuid.uuid4()
        message_id = uuid.uuid4()
        event_id = uuid.uuid1() # TimeUUID for outbox
        inbox_event_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        
        logger.info("Starting Table Persistency Verification...")

        # 1. Verify 'conversations' Table CRUD
        logger.info("Testing 'conversations' table...")
        insert_conv = session.prepare(
            "INSERT INTO conversations (conversation_id, user_id, title, created_at, updated_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        session.execute(insert_conv, (conversation_id, user_id, "Sample Title", now, now, "active"))
        
        select_conv = session.prepare("SELECT title, status FROM conversations WHERE conversation_id = ?")
        row = session.execute(select_conv, (conversation_id,)).one()
        assert row is not None, "Conversations insert failed"
        assert row.title == "Sample Title", "Conversations title mismatch"
        logger.info("Conversations Table: INSERT & SELECT [SUCCESS]")

        # Update Conversation
        update_conv = session.prepare("UPDATE conversations SET title = ? WHERE conversation_id = ?")
        session.execute(update_conv, ("Updated Title", conversation_id))
        row = session.execute(select_conv, (conversation_id,)).one()
        assert row.title == "Updated Title", "Conversations update failed"
        logger.info("Conversations Table: UPDATE [SUCCESS]")

        # 2. Verify 'conversations_by_user' Table CRUD
        logger.info("Testing 'conversations_by_user' table...")
        insert_conv_user = session.prepare(
            "INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        session.execute(insert_conv_user, (user_id, now, conversation_id, "Updated Title", now, "active"))
        
        select_conv_user = session.prepare("SELECT title FROM conversations_by_user WHERE user_id = ?")
        rows = list(session.execute(select_conv_user, (user_id,)))
        assert len(rows) > 0, "Conversations_by_user insert failed"
        assert rows[0].title == "Updated Title", "Conversations_by_user title mismatch"
        logger.info("Conversations_by_user Table: INSERT & SELECT [SUCCESS]")

        # 3. Verify 'messages_by_conversation' Table CRUD
        logger.info("Testing 'messages_by_conversation' table...")
        insert_msg = session.prepare(
            "INSERT INTO messages_by_conversation (conversation_id, message_id, sender, content, created_at, status) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        session.execute(insert_msg, (conversation_id, message_id, "user", "Hello world!", now, "sent"))
        
        select_msg = session.prepare(
            "SELECT content, sender FROM messages_by_conversation WHERE conversation_id = ? AND message_id = ?"
        )
        row = session.execute(select_msg, (conversation_id, message_id)).one()
        assert row is not None, "Messages insert failed"
        assert row.content == "Hello world!", "Messages content mismatch"
        logger.info("Messages Table: INSERT & SELECT [SUCCESS]")

        # 4. Verify 'transactional_outbox' Table CRUD
        logger.info("Testing 'transactional_outbox' table...")
        insert_outbox = session.prepare(
            "INSERT INTO transactional_outbox (bucket, event_id, event_type, payload, published, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        session.execute(insert_outbox, (0, event_id, "chat.message.created", '{"data": "val"}', False, now))
        
        select_outbox = session.prepare(
            "SELECT event_type, published FROM transactional_outbox WHERE bucket = ? AND event_id = ?"
        )
        row = session.execute(select_outbox, (0, event_id)).one()
        assert row is not None, "Outbox insert failed"
        assert row.published is False, "Outbox publish state mismatch"
        logger.info("Outbox Table: INSERT & SELECT [SUCCESS]")

        # 5. Verify 'inbox_events' Table CRUD
        logger.info("Testing 'inbox_events' table...")
        insert_inbox = session.prepare("INSERT INTO inbox_events (event_id, processed_at) VALUES (?, ?)")
        session.execute(insert_inbox, (inbox_event_id, now))
        
        select_inbox = session.prepare("SELECT processed_at FROM inbox_events WHERE event_id = ?")
        row = session.execute(select_inbox, (inbox_event_id,)).one()
        assert row is not None, "Inbox insert failed"
        logger.info("Inbox Table: INSERT & SELECT [SUCCESS]")

        # Clean up test rows
        logger.info("Cleaning up test data rows...")
        session.execute(session.prepare("DELETE FROM conversations WHERE conversation_id = ?"), (conversation_id,))
        session.execute(session.prepare("DELETE FROM conversations_by_user WHERE user_id = ? AND updated_at = ? AND conversation_id = ?"), (user_id, now, conversation_id))
        session.execute(session.prepare("DELETE FROM messages_by_conversation WHERE conversation_id = ? AND message_id = ?"), (conversation_id, message_id))
        session.execute(session.prepare("DELETE FROM transactional_outbox WHERE bucket = ? AND event_id = ?"), (0, event_id))
        session.execute(session.prepare("DELETE FROM inbox_events WHERE event_id = ?"), (inbox_event_id,))
        logger.info("Cleanup completed successfully.")
        
        print("\n==================================================")
        print("CASSANDRA CRUD SCHEMA VERIFICATION: ALL PASSED")
        print("==================================================\n")

    except Exception as e:
        logger.critical("CRUD assertion test failed with exception", error=str(e))
        raise e
    finally:
        cassandra_manager.close()

if __name__ == "__main__":
    test_cassandra_crud()
