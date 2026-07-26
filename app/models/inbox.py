"""
Inbox Event Domain Entity.
Handles records of processed events to execute idempotency deduplications.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

@dataclass
class InboxEvent:
    """
    Plain model tracking processed event registers.
    """
    event_id: UUID
    processed_at: datetime
