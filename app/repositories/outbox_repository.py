"""
Outbox Repository Adapter.
Handles prepared query statements mapping to Cassandra transactional outbox tables.
"""

from typing import List
from datetime import datetime, timezone
from uuid import UUID
from app.db.cassandra import cassandra_manager
from app.models.outbox import OutboxEvent

class CassandraOutboxRepository:
    """
    Cassandra repository adapter handling Outbox logs persistence.
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

    def save(self, bucket: int, event_id: UUID, event_type: str, payload: str) -> OutboxEvent:
        """
        Inserts new event task log into transactional outbox.
        """
        now = datetime.now(timezone.utc)
        cql = """
            INSERT INTO transactional_outbox (bucket, event_id, event_type, payload, published, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        stmt = self._get_prepared("save_outbox", cql)
        self.manager.session.execute(stmt, (bucket, event_id, event_type, payload, False, now))
        
        return OutboxEvent(
            bucket=bucket,
            event_id=event_id,
            event_type=event_type,
            payload=payload,
            published=False,
            created_at=now
        )

    def fetch_unpublished(self, bucket: int, limit: int = 200) -> List[OutboxEvent]:
        """
        Queries outbox records that haven't been published yet.
        """
        cql = """
            SELECT event_id, event_type, payload, published, created_at
            FROM transactional_outbox
            WHERE bucket = ? AND published = ?
            LIMIT ?
            ALLOW FILTERING
        """
        stmt = self._get_prepared("fetch_unpublished", cql)
        rows = self.manager.session.execute(stmt, (bucket, False, limit))
        
        events = []
        for row in rows:
            events.append(OutboxEvent(
                bucket=bucket,
                event_id=row.event_id,
                event_type=row.event_type,
                payload=row.payload,
                published=row.published,
                created_at=row.created_at
            ))
        return events

    def mark_published(self, bucket: int, event_id: UUID) -> bool:
        """
        Marks event as successfully published to broker.
        """
        cql = """
            UPDATE transactional_outbox
            SET published = true
            WHERE bucket = ? AND event_id = ?
        """
        stmt = self._get_prepared("mark_published", cql)
        self.manager.session.execute(stmt, (bucket, event_id))
        return True
