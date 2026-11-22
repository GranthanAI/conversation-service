"""
Message API Router.
Handles protected message history, generation triggers, and idempotency request handling.
"""

import uuid
import json
from datetime import datetime
from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, status, Query, Response, HTTPException, Header
from pydantic import BaseModel

from app.services.custom_mode_service import CustomModeUnavailableError
from app.security import (
    require_conversation_owner,
    CurrentUser
)
from app.api.deps import get_message_service, get_idempotency_service, get_edit_service, get_feedback_service
from app.security import get_current_user
from app.services.edit_service import EditRefused, EditService, MessageNotFound
from app.services.feedback_service import FeedbackService
from app.services.message_service import MessageService, PendingResponseRegistrationError
from app.services.idempotency_service import IdempotencyService
from app.models.conversation import Conversation
from app.schemas.message import (
    CreateMessageRequest,
    MessageResponse,
    MessageListResponse,
    RegenerateRequest,
    EditMessageRequest,
)
from app.schemas.conversation import ConversationResponse
from app.utils.helpers import uuidv7
from app.utils.pagination import encode_cursor, decode_cursor
from app.core.config import settings

router = APIRouter()

@router.post("/{conversation_id}/messages", response_model=MessageResponse, status_code=status.HTTP_202_ACCEPTED, summary="Send user message")
async def create_message(
    conversation_id: UUID,
    payload: CreateMessageRequest,
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service),
    idempotency_service: IdempotencyService = Depends(get_idempotency_service)
):
    """
    Appends a new user message to the conversation log and triggers streaming outbox events (requires ownership).
    Supports safe retries using client-supplied X-Idempotency-Key.
    """
    if x_idempotency_key:
        try:
            uuid.UUID(x_idempotency_key)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="X-Idempotency-Key must be a valid UUIDv4"
            )

        lock_status = await idempotency_service.claim_key(x_idempotency_key)
        if lock_status == "processing":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A concurrent request with this idempotency key is already in progress"
            )
        elif lock_status is not None:
            # Found cached response payload
            cached_data = json.loads(lock_status)
            return MessageResponse(
                message_id=UUID(cached_data["message_id"]),
                conversation_id=UUID(cached_data["conversation_id"]),
                sender=cached_data["sender"],
                content=cached_data["content"],
                created_at=datetime.fromisoformat(cached_data["created_at"]),
                status=cached_data["status"],
                file_ids=[UUID(fid) for fid in cached_data.get("file_ids", [])],
                response_message_id=UUID(cached_data["response_message_id"]) if cached_data.get("response_message_id") else None,
                billing=cached_data.get("billing"),
                defaults_applied=cached_data.get("defaults_applied"),
                think=bool(cached_data.get("think")),
            )

    # Process normally
    message_id = uuidv7()
    try:
        msg = await service.send(
            conversation_id=conversation_id,
            message_id=message_id,
            sender="user",
            content=payload.content,
            user_id=conv.user_id,
            mode=payload.mode,
            selected_tools=payload.selected_tools,
            include_diagrams=payload.include_diagrams,
            file_ids=payload.file_ids,
            model=payload.model,
            think=payload.think,
            temporary=conv.temporary,
        )
    except Exception as e:
        if x_idempotency_key:
            await idempotency_service.remove_lock(x_idempotency_key)
        if isinstance(e, CustomModeUnavailableError):
            # An explicitly chosen custom mode is not usable: nothing ran, and no other mode is substituted.
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"code": "custom_mode_unavailable",
                        "message": "That custom mode is no longer available. Choose another mode and try again."},
            )
        if isinstance(e, PendingResponseRegistrationError):
            # Nothing was dispatched (placeholder rolled back); the client may retry.
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Could not register the response for tracking; message not sent. Please retry.",
            )
        raise e

    if x_idempotency_key:
        response_data = {
            "message_id": str(msg.message_id),
            "conversation_id": str(msg.conversation_id),
            "sender": msg.sender,
            "content": msg.content,
            "created_at": msg.created_at.isoformat(),
            "status": msg.status.value,
            "file_ids": [str(fid) for fid in msg.file_ids],
            "response_message_id": str(msg.response_message_id) if msg.response_message_id else None,
            "billing": msg.billing,
            "defaults_applied": msg.defaults_applied,
            "think": msg.think,
        }
        await idempotency_service.save_response(x_idempotency_key, response_data)

    return msg

@router.get("/{conversation_id}/messages", response_model=MessageListResponse, status_code=status.HTTP_200_OK, summary="Get conversation message history")
async def get_message_history(
    conversation_id: UUID,
    limit: int = Query(settings.MESSAGE_HISTORY_DEFAULT_LIMIT, ge=settings.MESSAGE_HISTORY_MIN_LIMIT, le=settings.MESSAGE_HISTORY_MAX_LIMIT),
    cursor: Optional[str] = Query(None),
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service),
    feedback: FeedbackService = Depends(get_feedback_service),
):
    """
    Fetches paginated message logs using Cache-Aside caching and opaque cursor pagination (requires ownership).
    Assistant rows carry `suggestions`, `think` and the caller's own `feedback` (one query for the whole page).
    """
    cursor_message_id = None
    if cursor:
        cursor_payload = decode_cursor(cursor)
        msg_id_str = cursor_payload.get("message_id")
        if msg_id_str:
            cursor_message_id = UUID(msg_id_str)

    items = await service.history(conversation_id=conversation_id, limit=limit, cursor=cursor_message_id)
    await feedback.attach(conv.user_id, items)
    
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
    payload: Optional[RegenerateRequest] = None,
    conv: Conversation = Depends(require_conversation_owner),
    service: MessageService = Depends(get_message_service)
):
    """
    Triggers regeneration of an assistant message (requires ownership). The optional body may carry `model`
    ("auto" or a catalog model id); without it the saved default_model applies. A regenerate counts as a new message.
    """
    try:
        msg = await service.regenerate(conversation_id=conversation_id, message_id=message_id, user_id=conv.user_id,
                                       model=payload.model if payload else None,
                                       think=bool(payload and payload.think), temporary=conv.temporary)
    except PendingResponseRegistrationError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not register the response for tracking; regeneration not started. Please retry.",
        )
    if not msg:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Target assistant message not found in history"
        )
    return msg

class EditResponse(BaseModel):
    conversation: ConversationResponse
    message: MessageResponse


def _problem(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail={"code": code, "message": message})


@router.post("/{conversation_id}/messages/{message_id}/edit", response_model=EditResponse,
             status_code=status.HTTP_202_ACCEPTED, summary="Edit a sent message into a new branch")
async def edit_message(
    conversation_id: UUID,
    message_id: UUID,
    payload: EditMessageRequest,
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    conv: Conversation = Depends(require_conversation_owner),
    service: EditService = Depends(get_edit_service),
    idempotency_service: IdempotencyService = Depends(get_idempotency_service),
):
    """
    The edited question becomes a NEW sibling branch (the original and its answer stay). Same idempotency and replay
    behaviour as send: a retry with the same key returns the same conversation and message. 409 problem codes:
    not_user_message, message_not_sent, temporary_conversation, conversation_busy.
    """
    key = x_idempotency_key or idempotency_key
    if key:
        try:
            uuid.UUID(key)
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Idempotency key must be a valid UUID")
        lock = await idempotency_service.claim_key(f"edit:{key}")
        if lock == "processing":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail="A concurrent request with this idempotency key is already in progress")
        if lock is not None:
            return EditResponse.model_validate_json(lock)
    try:
        new_conv, msg = await service.edit(
            conv.user_id, conv, message_id, content=payload.content, mode=payload.mode,
            selected_tools=payload.selected_tools, include_diagrams=payload.include_diagrams,
            file_ids=payload.file_ids, model=payload.model, think=payload.think)
    except Exception as e:
        if key:
            await idempotency_service.remove_lock(f"edit:{key}")
        if isinstance(e, MessageNotFound):
            raise _problem(404, "message_not_found", "Message not found")
        if isinstance(e, EditRefused):
            raise _problem(409, e.code, str(e))
        if isinstance(e, CustomModeUnavailableError):
            raise _problem(422, "custom_mode_unavailable",
                           "That custom mode is no longer available. Choose another mode and try again.")
        if isinstance(e, PendingResponseRegistrationError):
            raise _problem(503, "pending_registration_failed",
                           "Could not register the response for tracking; message not sent. Please retry.")
        raise
    response = EditResponse(conversation=ConversationResponse.model_validate(new_conv),
                            message=MessageResponse.model_validate(msg))
    if key:
        await idempotency_service.save_response(f"edit:{key}", json.loads(response.model_dump_json()))
    return response


@router.get("/{conversation_id}/messages/{message_id}/edits", summary="Edited versions of a message")
async def list_edits(
    conversation_id: UUID,
    message_id: UUID,
    conv: Conversation = Depends(require_conversation_owner),
    service: EditService = Depends(get_edit_service),
):
    versions = await service.versions(conv.user_id, message_id)
    return [{"conversation_id": str(v["conversation_id"]), "title": v["title"], "created_at": v["created_at"].isoformat()}
            for v in versions]


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
