"""
Message API Router.
Handles protected message history and generation streaming trigger endpoints with JWT authentication and ownership validation.
"""

from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, status, Query, Response, HTTPException

from app.security import (
    require_conversation_owner,
    CurrentUser
)
from app.api.deps import get_message_service
from app.services.message_service import MessageService
from app.models.conversation import Conversation
from app.schemas.message import (
    CreateMessageRequest,
    MessageResponse,
    MessageListResponse
)
from app.utils.helpers import uuidv7
from app.utils.pagination import encode_cursor, decode_cursor

router = APIRouter()

@router.post("/{conversation_id}/messages", response_model=MessageResponse, status_code=status.HTTP_202_ACCEPTED, summary="Send user message")
async def create_message(
    conversation_id: UUID,
    payload: CreateMessageRequest,
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service)
):
    """
    Appends a new user message to the conversation log and triggers streaming outbox events (requires ownership).
    """
    message_id = uuidv7()
    msg = await service.send(
        conversation_id=conversation_id,
        message_id=message_id,
        sender="user",
        content=payload.content
    )
    return msg

@router.get("/{conversation_id}/messages", response_model=MessageListResponse, status_code=status.HTTP_200_OK, summary="Get conversation message history")
async def get_message_history(
    conversation_id: UUID,
    limit: int = Query(50, ge=1, le=100),
    cursor: Optional[str] = Query(None),
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service)
):
    """
    Fetches paginated message logs using Cache-Aside caching and opaque cursor pagination (requires ownership).
    """
    cursor_message_id = None
    if cursor:
        cursor_payload = decode_cursor(cursor)
        msg_id_str = cursor_payload.get("message_id")
        if msg_id_str:
            cursor_message_id = UUID(msg_id_str)

    items = await service.history(conversation_id=conversation_id, limit=limit, cursor=cursor_message_id)
    
    next_cursor = None
    has_more = False
    if len(items) == limit:
        last_item = items[-1]
        next_cursor = encode_cursor({"message_id": str(last_item.message_id)})
        has_more = True

    return MessageListResponse(items=items, next_cursor=next_cursor, has_more=has_more)

@router.post("/{conversation_id}/messages/{message_id}/regenerate", response_model=MessageResponse, status_code=status.HTTP_202_ACCEPTED, summary="Regenerate message")
async def regenerate_message(
    conversation_id: UUID,
    message_id: UUID,
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service)
):
    """
    Triggers regeneration of an assistant message (requires ownership).
    """
    msg = await service.regenerate(conversation_id=conversation_id, message_id=message_id)
    if not msg:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Target assistant message not found in history"
        )
    return msg

@router.delete("/{conversation_id}/messages/{message_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Soft delete message")
async def delete_message(
    conversation_id: UUID,
    message_id: UUID,
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service)
):
    """
    Soft-deletes a message from history (requires ownership).
    """
    success = await service.delete(conversation_id=conversation_id, message_id=message_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Message not found"
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
