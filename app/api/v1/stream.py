"""
SSE Stream API Router.
Exposes real-time Server-Sent Events stream for conversation replies.
"""

from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import StreamingResponse

from app.security import require_conversation_owner
from app.models.conversation import Conversation
from app.api.deps import get_stream_service
from app.services.stream_service import StreamService

router = APIRouter()

@router.get("/{conversation_id}", summary="Get conversation Server-Sent Events stream")
async def get_conversation_stream(
    conversation_id: UUID,
    message_id: Optional[UUID] = Query(None, description="Assistant response id: replay + follow only this turn"),
    since: Optional[str] = Query(None, pattern=r"^\d+(-\d+)?$", description="Resume after this event_id (0 = everything retained)"),
    conv: Conversation = Depends(require_conversation_owner),
    stream_service: StreamService = Depends(get_stream_service)
):
    """
    Establishes a Server-Sent Events (SSE) stream yielding live token generation events.
    Verifies authentication via standard Bearer authorization header or '?token=XYZ' query string.
    """
    generator = stream_service.subscribe(
        conversation_id,
        message_id=str(message_id) if message_id else None,
        since=since,
    )
    return StreamingResponse(
        generator,
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
