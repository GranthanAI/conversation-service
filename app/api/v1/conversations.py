"""
Conversation API Router.
Handles protected conversation catalog endpoints with JWT authentication and fine-grained ownership validation.
"""

from typing import Optional
from datetime import datetime
from uuid import UUID
from fastapi import APIRouter, Depends, status, Query, Response

from app.security import (
    get_current_user,
    require_conversation_owner,
    CurrentUser
)
from app.api.deps import get_conversation_service
from app.services.conversation_service import ConversationService
from app.models.conversation import Conversation
from app.schemas.conversation import (
    CreateConversationRequest,
    RenameConversationRequest,
    ConversationResponse,
    ConversationListResponse
)
from app.utils.pagination import encode_cursor, decode_cursor

router = APIRouter()

@router.post("", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED, summary="Create conversation")
async def create_conversation(
    payload: CreateConversationRequest,
    current_user: CurrentUser = Depends(get_current_user),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Creates a new conversation catalog for the authenticated user.
    """
    conv = service.create(user_id=current_user.id, title=payload.title)
    return conv

@router.get("", response_model=ConversationListResponse, status_code=status.HTTP_200_OK, summary="List user conversations")
async def list_conversations(
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None),
    current_user: CurrentUser = Depends(get_current_user),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Lists conversation catalogs for the authenticated user ordered by updated_at DESC using opaque cursor pagination.
    """
    cursor_updated_at = None
    if cursor:
        cursor_payload = decode_cursor(cursor)
        iso_str = cursor_payload.get("updated_at")
        if iso_str:
            cursor_updated_at = datetime.fromisoformat(iso_str)

    items = service.list(user_id=current_user.id, limit=limit, cursor=cursor_updated_at)
    
    next_cursor = None
    has_more = False
    if len(items) == limit:
        last_item = items[-1]
        next_cursor = encode_cursor({
            "updated_at": last_item.updated_at.isoformat(),
            "conversation_id": str(last_item.conversation_id)
        })
        has_more = True

    return ConversationListResponse(items=items, next_cursor=next_cursor, has_more=has_more)

@router.get("/{conversation_id}", response_model=ConversationResponse, status_code=status.HTTP_200_OK, summary="Get conversation")
async def get_conversation(
    conversation_id: UUID,
    conv: Conversation = Depends(require_conversation_owner)
):
    """
    Fetches a specific conversation by ID (requires ownership).
    """
    return conv

@router.patch("/{conversation_id}/rename", response_model=ConversationResponse, status_code=status.HTTP_200_OK, summary="Rename conversation")
async def rename_conversation(
    conversation_id: UUID,
    payload: RenameConversationRequest,
    conv: Conversation = Depends(require_conversation_owner),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Renames a conversation title (requires ownership).
    """
    updated = service.rename(conversation_id=conversation_id, new_title=payload.title)
    return updated

@router.post("/{conversation_id}/archive", response_model=ConversationResponse, status_code=status.HTTP_200_OK, summary="Archive conversation")
async def archive_conversation(
    conversation_id: UUID,
    conv: Conversation = Depends(require_conversation_owner),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Archives a conversation (requires ownership).
    """
    archived = service.archive(conversation_id=conversation_id)
    return archived

@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Soft delete conversation")
async def delete_conversation(
    conversation_id: UUID,
    conv: Conversation = Depends(require_conversation_owner),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Soft-deletes a conversation (requires ownership).
    """
    service.delete(conversation_id=conversation_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
