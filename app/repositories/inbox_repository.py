"""
Inbox Repository Adapter.
Handles prepared query statements mapping to Cassandra inbox deduplication tables.
"""

from datetime import datetime, timezone
from uuid import UUID
from app.db.cassandra import cassandra_manager
from app.models.inbox import InboxEvent

class CassandraInboxRepository:
    """
    Cassandra repository adapter handling Inbox deduplication records.
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

    def save(self, event_id: UUID) -> InboxEvent:
        """
        Records processed event ID to block redelivery.
        """
        now = datetime.now(timezone.utc)
        cql = """
            INSERT INTO inbox_events (event_id, processed_at)
            VALUES (?, ?)
        """
        stmt = self._get_prepared("save_inbox", cql)
        self.manager.session.execute(stmt, (event_id, now))
        
        return InboxEvent(
            event_id=event_id,
            processed_at=now
        )

    def exists(self, event_id: UUID) -> bool:
        """
        Verifies if an event has already been processed.
        """
        cql = """
            SELECT event_id
            FROM inbox_events
            WHERE event_id = ?
        """
        stmt = self._get_prepared("exists_inbox", cql)
        row = self.manager.session.execute(stmt, (event_id,)).one()
        return row is not None
