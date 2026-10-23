"""Conversation notes (gateway: /api/v1/conversations/{conversation_id}/notes).

  GET   the user's notes for this conversation (a missing document is empty: version 0).
  PUT   {"content": "...", "version": n} saves; `version` is the version the client last saw (0 = none yet). A stale
        version is a 409 that carries the CURRENT document, so the client can resolve it without losing anything.

Each conversation, including a branch, has its own independent document; nothing is inherited from or shared with a
parent or sibling. Notes are the user's own text: they are not sent to the Memory Service, Kafka, a prompt or any index.

Security
  * The owner is the verified JWT subject; there is no owner field in any request.
  * The conversation is looked up in the authoritative catalog on every call. A conversation that is missing, belongs to
    someone else, or has been (soft-)deleted all answer the same 404, so ids cannot be probed and deleted conversations'
    notes are not reachable.
  * If the catalog cannot be read, the answer is 503: access is never granted without the authorization check, and no
    partial state exists (a save is a single PostgreSQL statement).
"""

import time
from datetime import datetime
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from app.api.deps import get_conversation_service, get_notes_repository
from app.core.config import settings
from app.core.logging import logger
from app.models.conversation import Conversation, ConversationStatus
from app.repositories.notes_repository import NotesRepository, NotesRow, NotesVersionConflict
from app.security import CurrentUser, get_current_user

router = APIRouter()


class NotesDocument(BaseModel):
    conversation_id: UUID
    content: str
    version: int
    updated_at: Optional[datetime]
    max_chars: int


class NotesUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(max_length=settings.NOTES_MAX_CHARS)
    version: StrictInt = Field(ge=0)

    @field_validator("content")
    @classmethod
    def _no_nul(cls, value: str) -> str:
        if "\x00" in value:                       # PostgreSQL text cannot hold NUL; say so instead of a 500
            raise ValueError("notes cannot contain NUL characters")
        return value


def _doc(conversation_id: UUID, row: Optional[NotesRow]) -> NotesDocument:
    return NotesDocument(conversation_id=conversation_id, content=row.content if row else "",
                         version=row.version if row else 0, updated_at=row.updated_at if row else None,
                         max_chars=settings.NOTES_MAX_CHARS)


async def active_owned_conversation(conversation_id: UUID, user: CurrentUser = Depends(get_current_user),
                                    conversations=Depends(get_conversation_service)) -> Conversation:
    try:
        conv = await conversations.get(conversation_id)
    except Exception as exc:                                   # catalog unavailable: never grant access unchecked
        logger.error("notes_authorization_unavailable", error_type=type(exc).__name__)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, {
            "code": "conversation_unavailable", "message": "Could not verify access to this conversation. Please retry."}) from None
    if conv is None or conv.user_id != user.id or conv.status == ConversationStatus.DELETED:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Conversation not found")
    return conv


@router.get("/{conversation_id}/notes", response_model=NotesDocument, summary="Get my notes for this conversation")
async def get_notes(conv: Conversation = Depends(active_owned_conversation), user: CurrentUser = Depends(get_current_user),
                    repo: NotesRepository = Depends(get_notes_repository)):
    started = time.perf_counter()
    row = await repo.get(user.id, conv.conversation_id)
    logger.info("notes_read", exists=row is not None, version=row.version if row else 0,
                chars=len(row.content) if row else 0, duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return _doc(conv.conversation_id, row)


@router.put("/{conversation_id}/notes", response_model=NotesDocument, summary="Save my notes for this conversation")
async def put_notes(body: NotesUpdate, conv: Conversation = Depends(active_owned_conversation),
                    user: CurrentUser = Depends(get_current_user), repo: NotesRepository = Depends(get_notes_repository)):
    started = time.perf_counter()
    try:
        row, written = await repo.save(user.id, conv.conversation_id, body.content, body.version)
    except NotesVersionConflict as exc:
        logger.info("notes_conflict", sent_version=body.version, current_version=exc.current.version if exc.current else 0)
        current = _doc(conv.conversation_id, exc.current)
        raise HTTPException(status.HTTP_409_CONFLICT, {
            "code": "version_conflict", "message": "Your notes were changed elsewhere.",
            "current": current.model_dump(mode="json")}) from None
    logger.info("notes_write", written=written, version=row.version, chars=len(body.content),
                duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return _doc(conv.conversation_id, row)
