"""
API Routers File - Conversations.
Exposes REST endpoint path routers for creating, list querying, retrieving, renaming,
and soft-deleting conversations, delegating operations to the ConversationService.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from uuid import UUID
from typing import Optional
from api.deps import get_current_user_id
from dependencies.services import get_conversation_service
from services.conversation_service import ConversationService
from schemas.requests.conversation import CreateConversationRequest, RenameConversationRequest
from schemas.responses.conversation import ConversationResponse, ConversationListResponse
from domain.exceptions import NotFoundError, OwnershipError

router = APIRouter(prefix="/v1/conversations", tags=["Conversations"])

@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ConversationResponse,
    summary="Create a new conversation"
)
async def create_conversation(
    req: CreateConversationRequest,
    user_id: UUID = Depends(get_current_user_id),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Creates and records a new conversation with the specified title.
    """
    try:
        conv = await service.create_conversation(user_id=user_id, title=req.title)
        return conv
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create conversation: {str(e)}"
        )

@router.get(
    "",
    status_code=status.HTTP_200_OK,
    response_model=ConversationListResponse,
    summary="List conversations for the authenticated user"
)
async def list_conversations(
    cursor: Optional[str] = Query(None, description="Cursor for pagination (ISO datetime string)"),
    limit: int = Query(20, ge=1, le=100, description="Page limit (max 100)"),
    user_id: UUID = Depends(get_current_user_id),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Lists conversation records belonging to the authenticated user with limit/cursor pagination.
    """
    try:
        conversations, next_cursor = await service.list_conversations(
            user_id=user_id, limit=limit, cursor=cursor
        )
        return ConversationListResponse(conversations=conversations, next_cursor=next_cursor)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list conversations: {str(e)}"
        )

@router.get(
    "/{conversation_id}",
    status_code=status.HTTP_200_OK,
    response_model=ConversationResponse,
    summary="Retrieve details of a single conversation"
)
async def get_conversation(
    conversation_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Queries details for a specific conversation UUID, enforcing ownership verification.
    """
    try:
        conv = await service.get_conversation(conversation_id=conversation_id, user_id=user_id)
        return conv
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except OwnershipError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve conversation: {str(e)}"
        )

@router.patch(
    "/{conversation_id}/title",
    status_code=status.HTTP_200_OK,
    response_model=ConversationResponse,
    summary="Rename a conversation"
)
async def rename_conversation(
    conversation_id: UUID,
    req: RenameConversationRequest,
    user_id: UUID = Depends(get_current_user_id),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Renames the title string attribute of a conversation UUID.
    """
    try:
        conv = await service.rename_conversation(
            conversation_id=conversation_id, user_id=user_id, title=req.title
        )
        return conv
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except OwnershipError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to rename conversation: {str(e)}"
        )

@router.delete(
    "/{conversation_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Soft-delete a conversation"
)
async def delete_conversation(
    conversation_id: UUID,
    user_id: UUID = Depends(get_current_user_id),
    service: ConversationService = Depends(get_conversation_service)
):
    """
    Soft-deletes a conversation UUID from listing outputs by flagging its status.
    """
    try:
        await service.delete_conversation(conversation_id=conversation_id, user_id=user_id)
        return
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except OwnershipError as e:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete conversation: {str(e)}"
        )
