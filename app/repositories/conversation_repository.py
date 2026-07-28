"""
Conversation Repository Adapter.
Handles prepared query statements mapping to Apache Cassandra.
"""

from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID
from app.db.cassandra import cassandra_manager
from app.models.conversation import Conversation, ConversationStatus

class CassandraConversationRepository:
    """
    Cassandra repository adapter handling persistence and lookup for conversations.
    """
    def __init__(self):
        self.manager = cassandra_manager
        self._statements = {}

    def _get_prepared(self, name: str, cql: str):
        """
        Lazily prepares statements to handle connection lag robustly.
        """
        if name not in self._statements:
            session = self.manager.session
            if not session:
                raise RuntimeError("Cassandra database session not available.")
            self._statements[name] = session.prepare(cql)
        return self._statements[name]

    def create_with_outbox(
        self,
        conversation_id: UUID,
        user_id: UUID,
        title: str,
        status: str,
        event_id: UUID,
        event_type: str,
        outbox_payload: str
    ) -> Conversation:
        """
        Atomic transactional write saving conversation data and outbox task inside a single LOGGED BATCH.
        """
        now = datetime.now(timezone.utc)
        bucket = conversation_id.int % 32
        
        cql = """
            BEGIN BATCH
                INSERT INTO conversations (conversation_id, user_id, title, created_at, updated_at, status)
                VALUES (?, ?, ?, ?, ?, ?);
                INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?);
                INSERT INTO transactional_outbox (bucket, event_id, event_type, payload, published, created_at)
                VALUES (?, ?, ?, ?, ?, ?);
            APPLY BATCH;
        """
        stmt = self._get_prepared("create_conv_outbox", cql)
        
        self.manager.session.execute(stmt, (
            conversation_id, user_id, title, now, now, status,
            user_id, now, conversation_id, title, now, status,
            bucket, event_id, event_type, outbox_payload, False, now
        ))
        
        return Conversation(
            conversation_id=conversation_id,
            user_id=user_id,
            title=title,
            created_at=now,
            updated_at=now,
            status=ConversationStatus(status)
        )

    def get(self, conversation_id: UUID) -> Optional[Conversation]:
        """
        Fetches conversation metadata by UUID.
        """
        cql = """
            SELECT conversation_id, user_id, title, created_at, updated_at, status
            FROM conversations
            WHERE conversation_id = ?
        """
        stmt = self._get_prepared("get_conv", cql)
        row = self.manager.session.execute(stmt, (conversation_id,)).one()
        
        if not row:
            return None
            
        return Conversation(
            conversation_id=row.conversation_id,
            user_id=row.user_id,
            title=row.title,
            created_at=row.created_at,
            updated_at=row.updated_at,
            status=ConversationStatus(row.status)
        )

    def update_with_outbox(
        self,
        conversation_id: UUID,
        title: str,
        status: str,
        event_id: UUID,
        event_type: str,
        outbox_payload: str
    ) -> Optional[Conversation]:
        """
        Atomic transactional update updating conversation metadata and staging outbox events in a single LOGGED BATCH.
        """
        conv = self.get(conversation_id)
        if not conv:
            return None

        new_updated_at = datetime.now(timezone.utc)
        bucket = conversation_id.int % 32

        cql = """
            BEGIN BATCH
                DELETE FROM conversations_by_user
                WHERE user_id = ? AND updated_at = ? AND conversation_id = ?;
                
                INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?);
                
                UPDATE conversations
                SET title = ?, updated_at = ?, status = ?
                WHERE conversation_id = ?;
                
                INSERT INTO transactional_outbox (bucket, event_id, event_type, payload, published, created_at)
                VALUES (?, ?, ?, ?, ?, ?);
            APPLY BATCH;
        """
        stmt = self._get_prepared("update_conv_outbox", cql)
        
        self.manager.session.execute(stmt, (
            conv.user_id, conv.updated_at, conversation_id,
            conv.user_id, new_updated_at, conversation_id, title, conv.created_at, status,
            title, new_updated_at, status, conversation_id,
            bucket, event_id, event_type, outbox_payload, False, new_updated_at
        ))

        return Conversation(
            conversation_id=conversation_id,
            user_id=conv.user_id,
            title=title,
            created_at=conv.created_at,
            updated_at=new_updated_at,
            status=ConversationStatus(status)
        )

    def delete_with_outbox(
        self,
        conversation_id: UUID,
        event_id: UUID,
        event_type: str,
        outbox_payload: str
    ) -> bool:
        """
        Soft deletes a conversation and stages the outbox delete event in a single LOGGED BATCH.
        """
        conv = self.get(conversation_id)
        if not conv:
            return False
        
        # Soft delete is an update statement setting status to deleted
        result = self.update_with_outbox(
            conversation_id=conversation_id,
            title=conv.title,
            status="deleted",
            event_id=event_id,
            event_type=event_type,
            outbox_payload=outbox_payload
        )
        return result is not None

    def list(self, user_id: UUID, limit: int = 20, cursor: Optional[datetime] = None) -> List[Conversation]:
        """
        Lists user conversation catalogs ordered by updated_at DESC.
        """
        if cursor:
            cql = """
                SELECT conversation_id, title, created_at, updated_at, status
                FROM conversations_by_user
                WHERE user_id = ? AND updated_at < ?
                LIMIT ?
            """
            stmt = self._get_prepared("list_conv_cursor", cql)
            rows = self.manager.session.execute(stmt, (user_id, cursor, limit))
        else:
            cql = """
                SELECT conversation_id, title, created_at, updated_at, status
                FROM conversations_by_user
                WHERE user_id = ?
                LIMIT ?
            """
            stmt = self._get_prepared("list_conv_no_cursor", cql)
            rows = self.manager.session.execute(stmt, (user_id, limit))

        conversations = []
        for row in rows:
            if row.status == "deleted":
                continue
            conversations.append(Conversation(
                conversation_id=row.conversation_id,
                user_id=user_id,
                title=row.title,
                created_at=row.created_at,
                updated_at=row.updated_at,
                status=ConversationStatus(row.status)
            ))
        return conversations

    def update_title(self, conversation_id: UUID, title: str) -> bool:
        """
        Updates the title of a conversation directly in Cassandra tables (no outbox staging).
        """
        conv = self.get(conversation_id)
        if not conv:
            return False

        new_updated_at = datetime.now(timezone.utc)
        cql = """
            BEGIN BATCH
                DELETE FROM conversations_by_user
                WHERE user_id = ? AND updated_at = ? AND conversation_id = ?;
                
                INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status)
                VALUES (?, ?, ?, ?, ?, ?);
                
                UPDATE conversations
                SET title = ?, updated_at = ?
                WHERE conversation_id = ?;
            APPLY BATCH;
        """
        stmt = self._get_prepared("update_conv_title_direct", cql)
        self.manager.session.execute(stmt, (
            conv.user_id, conv.updated_at, conversation_id,
            conv.user_id, new_updated_at, conversation_id, title, conv.created_at, conv.status.value,
            title, new_updated_at, conversation_id
        ))
        return True
