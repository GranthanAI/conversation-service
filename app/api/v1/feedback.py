"""Thumbs up / down (Q2) and the staff review list.

PUT/DELETE /v1/conversations/{cid}/messages/{mid}/feedback   (owner only)
GET        /v1/feedback/admin                                  (roles support or admin)
"""

from datetime import date, datetime
from typing import List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from pydantic import BaseModel, Field, field_validator

from app.api.deps import get_feedback_service
from app.core.config import settings
from app.models.conversation import Conversation
from app.security import CurrentUser, get_current_user, require_conversation_owner
from app.services.feedback_service import REASONS, FeedbackNotAllowed, FeedbackService
from app.utils.pagination import decode_cursor, encode_cursor

router = APIRouter()
admin_router = APIRouter()


class FeedbackRequest(BaseModel):
    rating: Literal["up", "down"]
    reason: Optional[str] = None
    comment: Optional[str] = Field(None, max_length=1000)

    @field_validator("reason")
    @classmethod
    def _reason(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in REASONS:
            raise ValueError("Unknown reason.")
        return v

    @field_validator("comment")
    @classmethod
    def _comment(cls, v: Optional[str]) -> Optional[str]:
        return (v.strip() or None) if v is not None else None


class FeedbackResponse(BaseModel):
    rating: str
    reason: Optional[str] = None


class FeedbackItem(BaseModel):
    created_at: datetime
    rating: str
    reason: Optional[str] = None
    comment: Optional[str] = None
    model: Optional[str] = None
    model_group: Optional[str] = None
    conversation_id: UUID
    message_id: UUID
    answer: Optional[str] = None
    question: Optional[str] = None


class FeedbackAdminList(BaseModel):
    items: List[FeedbackItem]
    next_cursor: Optional[str] = None
    has_more: bool = False


@router.put("/{conversation_id}/messages/{message_id}/feedback", response_model=FeedbackResponse,
            summary="Rate an answer (thumbs up / down)")
async def put_feedback(
    conversation_id: UUID, message_id: UUID, body: FeedbackRequest,
    conv: Conversation = Depends(require_conversation_owner),
    user: CurrentUser = Depends(get_current_user),
    service: FeedbackService = Depends(get_feedback_service),
):
    if body.rating == "up" and (body.reason or body.comment):
        raise HTTPException(status_code=422, detail={"code": "invalid_feedback",
                                                     "message": "A reason or comment goes with a thumbs down only."})
    try:
        return await service.put(user.id, conversation_id, message_id, body.rating, body.reason, body.comment)
    except FeedbackNotAllowed:
        raise HTTPException(status_code=409, detail={"code": "not_assistant_message",
                                                     "message": "Only a finished answer can be rated."})


@router.delete("/{conversation_id}/messages/{message_id}/feedback", status_code=status.HTTP_204_NO_CONTENT,
               summary="Remove my rating of an answer")
async def delete_feedback(
    conversation_id: UUID, message_id: UUID,
    conv: Conversation = Depends(require_conversation_owner),
    user: CurrentUser = Depends(get_current_user),
    service: FeedbackService = Depends(get_feedback_service),
):
    await service.delete(user.id, message_id)               # 204 also when there was none
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def require_staff(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    allowed = {r.strip() for r in settings.FEEDBACK_ADMIN_ROLES.split(",") if r.strip()}
    if not allowed.intersection(user.roles):
        raise HTTPException(status_code=403, detail="Forbidden: staff only")
    return user


@admin_router.get("/admin", response_model=FeedbackAdminList, summary="Staff: review rated answers")
async def admin_feedback(
    rating: Literal["up", "down"] = Query("down"),
    date_from: Optional[date] = Query(None, alias="from"),
    date_to: Optional[date] = Query(None, alias="to"),
    model_group: Optional[str] = Query(None, max_length=16),
    reason: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=50),
    _staff: CurrentUser = Depends(require_staff),
    service: FeedbackService = Depends(get_feedback_service),
):
    if reason is not None and reason not in REASONS:
        raise HTTPException(status_code=422, detail="Unknown reason.")
    after = None
    if cursor:
        c = decode_cursor(cursor)
        try:
            after = (datetime.fromisoformat(c["created_at"]), UUID(c["message_id"]))
        except (KeyError, ValueError):
            raise HTTPException(status_code=400, detail="Invalid cursor")
    items, has_more = await service.admin_list(rating=rating, date_from=date_from, date_to=date_to,
                                               model_group=model_group, reason=reason, cursor=after, limit=limit)
    next_cursor = None
    if has_more and items:
        last = items[-1]
        next_cursor = encode_cursor({"created_at": last["created_at"].isoformat(), "message_id": str(last["message_id"])})
    return FeedbackAdminList(items=items, next_cursor=next_cursor, has_more=has_more)
