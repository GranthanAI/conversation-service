"""
Canonical Event Envelope Builder.
All services must use this shared builder to produce consistently structured
Kafka event payloads. The canonical schema is frozen at version 1.

Schema:
    {
        "event_id":       str  (UUIDv1 — time-ordered, unique per event),
        "event_type":     str  (e.g. "conversation.created"),
        "event_version":  int  (schema version; bump when payload shape changes),
        "occurred_at":    str  (ISO-8601 UTC timestamp),
        "source_service": str  (originating service name),
        "correlation_id": str  (X-Correlation-ID from the HTTP request, "" if unavailable),
        "causation_id":   str  (ID of the event or request that caused this one),
        "payload":        dict (event-specific data — never mix with top-level keys)
    }
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

SOURCE_SERVICE = "conversation-service"


def build_event_envelope(
    event_type: str,
    payload: Dict[str, Any],
    event_version: int = 1,
    causation_id: Optional[str] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Constructs a canonical versioned event envelope dict ready to be stored in the
    Cassandra Outbox and subsequently published to Kafka.

    Args:
        event_type:     Kafka topic / event type name (e.g. "conversation.created").
        payload:        Event-specific domain data. Never mix with top-level fields.
        event_version:  Schema version — increment only when payload shape changes.
        causation_id:   ID of the request/event that directly caused this event.
        correlation_id: X-Correlation-ID inherited from the originating HTTP request.
                        If not supplied, the middleware ContextVar is read automatically.

    Returns:
        A fully populated canonical envelope dict.
    """
    # Auto-read correlation ID from middleware ContextVar if not explicitly given
    if correlation_id is None:
        try:
            from app.middleware.correlation import get_correlation_id
            correlation_id = get_correlation_id()
        except Exception:
            correlation_id = ""

    event_id = str(uuid.uuid1())  # UUIDv1: time-ordered for Kafka partition locality

    return {
        "event_id":       event_id,
        "event_type":     event_type,
        "event_version":  event_version,
        "occurred_at":    datetime.now(timezone.utc).isoformat(),
        "source_service": SOURCE_SERVICE,
        "correlation_id": correlation_id or "",
        "causation_id":   causation_id or event_id,  # self-referential if no parent
        "payload":        payload,
    }
