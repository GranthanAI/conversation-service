"""
Message Repository Adapter.
Handles prepared query statements mapping to Cassandra messages tables.
"""

import time
from typing import List, Optional
from datetime import datetime, timezone
from uuid import UUID
from app.db.cassandra import cassandra_manager
from app.models.message import Message, MessageStatus

class CassandraMessageRepository:
    """
    Cassandra repository adapter handling messages CRUD.
    """
    def __init__(self):
        self.manager = cassandra_manager
        self._statements = {}

    def _get_prepared(self, name: str, cql: str):
        """
        Lazily prepares statements.
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
        message_id: UUID,
        sender: str,
        content: str,
        status: str,
        event_id: UUID,
        event_type: str,
        outbox_payload: str,
        file_ids: Optional[List[UUID]] = None
    ) -> Message:
        """
        Atomic transactional write saving message data and outbox task in a single LOGGED BATCH.
        Uses client-side microsecond timestamps for LWW correctness.
        """
        now = datetime.now(timezone.utc)
        timestamp_micros = int(time.time() * 1000000)
        bucket = conversation_id.int % 32
        file_ids = file_ids or []

        cql = """
            BEGIN BATCH
                INSERT INTO messages_by_conversation (conversation_id, message_id, sender, content, created_at, status, file_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TIMESTAMP ?;

                INSERT INTO transactional_outbox (bucket, event_id, event_type, payload, published, created_at)
                VALUES (?, ?, ?, ?, ?, ?);
            APPLY BATCH;
        """
        stmt = self._get_prepared("create_msg_outbox", cql)

        self.manager.session.execute(stmt, (
            conversation_id, message_id, sender, content, now, status, file_ids, timestamp_micros,
            bucket, event_id, event_type, outbox_payload, False, now
        ))

        return Message(
            conversation_id=conversation_id,
            message_id=message_id,
            sender=sender,
            content=content,
            created_at=now,
            status=MessageStatus(status),
            file_ids=file_ids
        )

    def history(self, conversation_id: UUID, limit: int = 50, cursor: Optional[UUID] = None) -> List[Message]:
        """
        Fetches conversation message history.
        """
        if cursor:
            cql = """
                SELECT message_id, sender, content, created_at, status, file_ids
                FROM messages_by_conversation
                WHERE conversation_id = ? AND message_id < ?
                LIMIT ?
            """
            stmt = self._get_prepared("history_cursor", cql)
            rows = self.manager.session.execute(stmt, (conversation_id, cursor, limit))
        else:
            cql = """
                SELECT message_id, sender, content, created_at, status, file_ids
                FROM messages_by_conversation
                WHERE conversation_id = ?
                LIMIT ?
            """
            stmt = self._get_prepared("history_no_cursor", cql)
            rows = self.manager.session.execute(stmt, (conversation_id, limit))

        messages = []
        for row in rows:
            if row.status == "deleted":
                continue
            messages.append(Message(
                conversation_id=conversation_id,
                message_id=row.message_id,
                sender=row.sender,
                content=row.content,
                created_at=row.created_at,
                status=MessageStatus(row.status),
                file_ids=list(row.file_ids) if row.file_ids else []
            ))
        return messages

    def history_upto(self, conversation_id: UUID, upto_message_id: UUID, limit: int = 50) -> List[Message]:
        """Newest-first messages up to and INCLUDING `upto_message_id` (a branch point)."""
        cql = """
            SELECT message_id, sender, content, created_at, status, file_ids
            FROM messages_by_conversation
            WHERE conversation_id = ? AND message_id <= ?
            LIMIT ?
        """
        stmt = self._get_prepared("history_upto", cql)
        rows = self.manager.session.execute(stmt, (conversation_id, upto_message_id, limit))
        out: List[Message] = []
        for row in rows:
            try:
                status = MessageStatus(row.status)
            except ValueError:  # 'deleted'
                continue
            out.append(Message(
                conversation_id=conversation_id, message_id=row.message_id, sender=row.sender,
                content=row.content, created_at=row.created_at, status=status,
                file_ids=list(row.file_ids) if row.file_ids else [],
            ))
        return out

    def get_message(self, conversation_id: UUID, message_id: UUID) -> Optional[Message]:
        """Fetches a single message row, or None."""
        cql = """
            SELECT sender, content, created_at, status, file_ids
            FROM messages_by_conversation
            WHERE conversation_id = ? AND message_id = ?
        """
        stmt = self._get_prepared("get_msg", cql)
        row = self.manager.session.execute(stmt, (conversation_id, message_id)).one()
        if row is None:
            return None
        try:
            status = MessageStatus(row.status)
        except ValueError:  # e.g. 'deleted'
            status = MessageStatus.FAILED
        return Message(
            conversation_id=conversation_id,
            message_id=message_id,
            sender=row.sender,
            content=row.content,
            created_at=row.created_at,
            status=status,
            file_ids=list(row.file_ids) if row.file_ids else [],
        )

    def delete(self, conversation_id: UUID, message_id: UUID) -> bool:
        """
        Soft deletes a message.
        """
        cql = """
            UPDATE messages_by_conversation
            SET status = 'deleted'
            WHERE conversation_id = ? AND message_id = ?
        """
        stmt = self._get_prepared("delete_msg", cql)
        self.manager.session.execute(stmt, (conversation_id, message_id))
        return True

    def create_message_direct(
        self,
        conversation_id: UUID,
        message_id: UUID,
        sender: str,
        content: str,
        status: str
    ) -> Message:
        """
        Inserts/updates a message directly in Cassandra without staging outbox event.
        """
        now = datetime.now(timezone.utc)
        cql = """
            INSERT INTO messages_by_conversation (conversation_id, message_id, sender, content, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        stmt = self._get_prepared("create_msg_direct", cql)
        self.manager.session.execute(stmt, (
            conversation_id, message_id, sender, content, now, status
        ))
        return Message(
            conversation_id=conversation_id,
            message_id=message_id,
            sender=sender,
            content=content,
            created_at=now,
            status=MessageStatus(status)
        )

    def delete_all_for_conversation(self, conversation_id: UUID) -> bool:
        """
        Hard-deletes all messages in a conversation partition.
        """
        cql = """
            DELETE FROM messages_by_conversation
            WHERE conversation_id = ?
        """
        stmt = self._get_prepared("delete_all_msg_for_conv", cql)
        self.manager.session.execute(stmt, (conversation_id,))
        return True
