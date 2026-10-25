"""Edited copies of code / text / Markdown blocks of assistant answers.

  GET    /conversations/{cid}/source-edits[?message_id=]        the caller's edits in this conversation
  PUT    /conversations/{cid}/source-edits/{message_id}/{block_id}
         {"content", "version", "language", "source_format", "original_sha256"}   create or update (compare-and-set)
  DELETE /conversations/{cid}/source-edits/{message_id}/{block_id}?version=n       discard the edit (back to the original)

The assistant message is never modified: an edit is a separate, owner-scoped, versioned row (see source_edit_models). `version`
is the version the client last saw (0 = no edit yet); a stale version is a 409 carrying the CURRENT edit, so nothing is
overwritten. Saving text identical to what is stored writes nothing and succeeds, so a retry after a lost response is safe.

Security: the owner is the verified JWT subject (never a request field); the conversation is checked against the authoritative
catalog on every call (missing / someone else's / deleted all answer the same 404; catalog outage is 503, never an unchecked
grant). Content is stored and returned as inert text; nothing here executes it, sends it to a model, Kafka or an index.
"""

import re
import time
from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from app.api.deps import get_source_edit_repository
from app.api.v1.notes import active_owned_conversation
from app.core.config import settings
from app.core.logging import logger
from app.models.conversation import Conversation
from app.repositories.source_edit_repository import (SourceEditConflict, SourceEditLimitReached, SourceEditRepository,
                                                      SourceEditRow)
from app.security import CurrentUser, get_current_user

router = APIRouter()

BLOCK_ID = Path(pattern=r"^[A-Za-z0-9._:-]{1,80}$", description="Stable block id computed by the client")


class SourceEditDoc(BaseModel):
    conversation_id: UUID
    message_id: UUID
    block_id: str
    language: str
    source_format: str
    original_sha256: str
    content: str
    version: int
    created_at: datetime
    updated_at: datetime


class SourceEditList(BaseModel):
    conversation_id: UUID
    edits: list[SourceEditDoc]
    max_chars: int


class SourceEditUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(max_length=settings.SOURCE_EDIT_MAX_CHARS)
    version: StrictInt = Field(ge=0)
    language: str = Field(default="", pattern=r"^[a-z0-9+#._-]{0,32}$")
    source_format: Literal["code", "text", "markdown"]
    original_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("content")
    @classmethod
    def _no_nul(cls, value: str) -> str:
        if "\x00" in value:                       # PostgreSQL text cannot hold NUL; say so instead of a 500
            raise ValueError("source cannot contain NUL characters")
        return value


def _doc(conversation_id: UUID, r: SourceEditRow) -> SourceEditDoc:
    return SourceEditDoc(conversation_id=conversation_id, message_id=r.message_id, block_id=r.block_id, language=r.language,
                         source_format=r.source_format, original_sha256=r.original_sha256, content=r.content,
                         version=r.version, created_at=r.created_at, updated_at=r.updated_at)


def _conflict(conversation_id: UUID, current: Optional[SourceEditRow]) -> HTTPException:
    return HTTPException(status.HTTP_409_CONFLICT, {
        "code": "version_conflict", "message": "This block was changed elsewhere.",
        "current": _doc(conversation_id, current).model_dump(mode="json") if current else None})


@router.get("/{conversation_id}/source-edits", response_model=SourceEditList, summary="My edited code/text blocks in this conversation")
async def list_source_edits(message_id: Optional[UUID] = Query(default=None), conv: Conversation = Depends(active_owned_conversation),
                            user: CurrentUser = Depends(get_current_user),
                            repo: SourceEditRepository = Depends(get_source_edit_repository)):
    started = time.perf_counter()
    rows = await repo.list(user.id, conv.conversation_id, message_id)
    logger.info("source_edits_read", count=len(rows), duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return SourceEditList(conversation_id=conv.conversation_id, edits=[_doc(conv.conversation_id, r) for r in rows],
                          max_chars=settings.SOURCE_EDIT_MAX_CHARS)


@router.put("/{conversation_id}/source-edits/{message_id}/{block_id}", response_model=SourceEditDoc,
            summary="Save my edit of one block")
async def put_source_edit(body: SourceEditUpdate, message_id: UUID, block_id: str = BLOCK_ID,
                          conv: Conversation = Depends(active_owned_conversation), user: CurrentUser = Depends(get_current_user),
                          repo: SourceEditRepository = Depends(get_source_edit_repository)):
    started = time.perf_counter()
    try:
        row, written = await repo.save(
            user.id, conv.conversation_id, message_id, block_id, content=body.content, expected_version=body.version,
            language=body.language, source_format=body.source_format, original_sha256=body.original_sha256,
            max_per_conversation=settings.SOURCE_EDIT_MAX_PER_CONVERSATION)
    except SourceEditConflict as exc:
        logger.info("source_edit_conflict", sent_version=body.version, current_version=exc.current.version if exc.current else 0)
        raise _conflict(conv.conversation_id, exc.current) from None
    except SourceEditLimitReached:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, {
            "code": "too_many_edits",
            "message": f"A conversation can hold at most {settings.SOURCE_EDIT_MAX_PER_CONVERSATION} edited blocks."}) from None
    logger.info("source_edit_write", written=written, version=row.version, chars=len(body.content),
                duration_ms=round((time.perf_counter() - started) * 1000, 1))
    return _doc(conv.conversation_id, row)


@router.delete("/{conversation_id}/source-edits/{message_id}/{block_id}", status_code=status.HTTP_204_NO_CONTENT,
               summary="Discard my edit (back to the original answer)")
async def delete_source_edit(message_id: UUID, version: int = Query(ge=1), block_id: str = BLOCK_ID,
                             conv: Conversation = Depends(active_owned_conversation), user: CurrentUser = Depends(get_current_user),
                             repo: SourceEditRepository = Depends(get_source_edit_repository)):
    try:
        removed = await repo.delete(user.id, conv.conversation_id, message_id, block_id, version)
    except SourceEditConflict as exc:
        raise _conflict(conv.conversation_id, exc.current) from None
    logger.info("source_edit_discard", removed=removed)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
