"""
Infrastructure Cassandra Repository - Conversation.
Concrete implementation of IConversationRepository using prepared queries,
thread pool isolation for non-blocking I/O, and atomic logged batches.
"""

import asyncio
import logging
from uuid import UUID
from datetime import datetime
from typing import List, Optional
from domain.entities.conversation import ConversationEntity
from domain.repositories.conversation import IConversationRepository
from core.enums import ConversationStatus
from infrastructure.cassandra.client import CassandraClientManager

logger = logging.getLogger(__name__)

class CassandraConversationRepository(IConversationRepository):
    """
    Cassandra concrete data adapter implementing the IConversationRepository port.
    """
    def __init__(self, client_manager: CassandraClientManager):
        self.manager = client_manager
        self.prepared_insert_conv = None
        self.prepared_insert_conv_by_user = None
        self.prepared_get_conv = None
        self.prepared_list_convs_first = None
        self.prepared_list_convs_cursor = None
        self.prepared_update_status = None
        self.prepared_update_status_by_user = None
        self.prepared_update_title = None
        self.prepared_update_title_by_user = None
        self.prepared_get_conv_by_user = None
        
        self._prepare_statements()

    def _prepare_statements(self) -> None:
        """
        Compiles driver PreparedStatements for performance.
        """
        if not self.manager.session:
            logger.warning("Cassandra session not available, skipping prepared statements initialization.")
            return

        session = self.manager.session
        try:
            self.prepared_insert_conv = session.prepare(
                "INSERT INTO conversations (conversation_id, user_id, title, created_at, updated_at, status) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            self.prepared_insert_conv_by_user = session.prepare(
                "INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            self.prepared_get_conv = session.prepare(
                "SELECT conversation_id, user_id, title, created_at, updated_at, status "
                "FROM conversations WHERE conversation_id = ?"
            )
            self.prepared_list_convs_first = session.prepare(
                "SELECT conversation_id, user_id, title, created_at, updated_at, status "
                "FROM conversations_by_user WHERE user_id = ? LIMIT ?"
            )
            self.prepared_list_convs_cursor = session.prepare(
                "SELECT conversation_id, user_id, title, created_at, updated_at, status "
                "FROM conversations_by_user WHERE user_id = ? AND updated_at < ? LIMIT ?"
            )
            self.prepared_update_status = session.prepare(
                "UPDATE conversations SET status = ?, updated_at = ? WHERE conversation_id = ?"
            )
            self.prepared_update_status_by_user = session.prepare(
                "UPDATE conversations_by_user SET status = ? WHERE user_id = ? AND updated_at = ? AND conversation_id = ?"
            )
            self.prepared_update_title = session.prepare(
                "UPDATE conversations SET title = ?, updated_at = ? WHERE conversation_id = ?"
            )
            self.prepared_update_title_by_user = session.prepare(
                "INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status) "
                "VALUES (?, ?, ?, ?, ?, ?)"
            )
            self.prepared_get_conv_by_user = session.prepare(
                "SELECT user_id, updated_at, conversation_id, title, created_at, status "
                "FROM conversations_by_user WHERE user_id = ? AND conversation_id = ? LIMIT 1 ALLOW FILTERING"
            )
        except Exception as e:
            logger.error(f"Failed to prepare statements in CassandraConversationRepository: {e}")

    async def create_conversation(
        self, conversation_id: UUID, user_id: UUID, title: str, created_at: datetime
    ) -> ConversationEntity:
        """
        Creates a conversation atomically across conversations and conversations_by_user using BATCH.
        """
        if not self.manager.session:
            raise RuntimeError("Cassandra session not available")

        entity = ConversationEntity(
            conversation_id=conversation_id,
            user_id=user_id,
            title=title,
            created_at=created_at,
            updated_at=created_at,
            status=ConversationStatus.ACTIVE
        )

        def _execute():
            batch = "BEGIN BATCH\n" \
                    f"  INSERT INTO conversations (conversation_id, user_id, title, created_at, updated_at, status) VALUES ({conversation_id}, {user_id}, '{title}', {int(created_at.timestamp()*1000)}, {int(created_at.timestamp()*1000)}, 'active');\n" \
                    f"  INSERT INTO conversations_by_user (user_id, updated_at, conversation_id, title, created_at, status) VALUES ({user_id}, {int(created_at.timestamp()*1000)}, {conversation_id}, '{title}', {int(created_at.timestamp()*1000)}, 'active');\n" \
                    "APPLY BATCH;"
            self.manager.session.execute(batch)

        await asyncio.to_thread(_execute)
        return entity

    async def get_conversation(self, conversation_id: UUID) -> Optional[ConversationEntity]:
        """
        Fetches a conversation by UUID, mapping the Cassandra driver row to entity schema.
        """
        if not self.manager.session:
            raise RuntimeError("Cassandra session not available")

        def _execute():
            bound = self.prepared_get_conv.bind([conversation_id])
            return self.manager.session.execute(bound).one()

        row = await asyncio.to_thread(_execute)
        if not row:
            return None

        return ConversationEntity(
            conversation_id=row.conversation_id,
            user_id=row.user_id,
            title=row.title,
            created_at=row.created_at,
            updated_at=row.updated_at,
            status=ConversationStatus(row.status)
        )

    async def list_conversations_by_user(
        self, user_id: UUID, limit: int, cursor: Optional[datetime] = None
    ) -> List[ConversationEntity]:
        """
        Queries conversation catalog for user with pagination limits.
        """
        if not self.manager.session:
            raise RuntimeError("Cassandra session not available")

        def _execute():
            if cursor:
                bound = self.prepared_list_convs_cursor.bind([user_id, cursor, limit])
            else:
                bound = self.prepared_list_convs_first.bind([user_id, limit])
            return self.manager.session.execute(bound)

        rows = await asyncio.to_thread(_execute)
        results = []
        for row in rows:
            results.append(
                ConversationEntity(
                    conversation_id=row.conversation_id,
                    user_id=row.user_id,
                    title=row.title,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                    status=ConversationStatus(row.status)
                )
            )
        return results

    async def update_conversation_status(self, conversation_id: UUID, status: str) -> None:
        """
        Updates the status state flag for soft deleting or archiving.
        """
        if not self.manager.session:
            raise RuntimeError("Cassandra session not available")

        conv = await self.get_conversation(conversation_id)
        if not conv:
            return

        now = datetime.utcnow()

        def _execute():
            self.manager.session.execute(self.prepared_update_status.bind([status, now, conversation_id]))
            self.manager.session.execute(self.prepared_update_status_by_user.bind([status, conv.user_id, conv.updated_at, conversation_id]))

        await asyncio.to_thread(_execute)

    async def update_conversation_title(self, conversation_id: UUID, title: str) -> None:
        """
        Renames a conversation title, maintaining catalog sort indexes.
        """
        if not self.manager.session:
            raise RuntimeError("Cassandra session not available")

        conv = await self.get_conversation(conversation_id)
        if not conv:
            return

        now = datetime.utcnow()

        def _execute():
            # Update main conversations table
            self.manager.session.execute(self.prepared_update_title.bind([title, now, conversation_id]))
            
            # Delete old row and insert new row in user catalog (since updated_at is a clustering key)
            delete_stmt = f"DELETE FROM conversations_by_user WHERE user_id = {conv.user_id} AND updated_at = {int(conv.updated_at.timestamp()*1000)} AND conversation_id = {conversation_id};"
            self.manager.session.execute(delete_stmt)
            
            # Write new user catalog index
            self.manager.session.execute(
                self.prepared_update_title_by_user.bind([conv.user_id, now, conversation_id, title, conv.created_at, conv.status.value])
            )

        await asyncio.to_thread(_execute)
