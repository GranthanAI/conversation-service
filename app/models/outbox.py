"""
Outbox Event Domain Entity.
Maps transaction records queue for async message publishing brokers.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

@dataclass
class OutboxEvent:
    """
    Plain model tracking outbox queue tasks.
    """
    bucket: int
    event_id: UUID
    event_type: str
    payload: str  # JSON-serialized string representation of metadata
    published: bool
    created_at: datetime
