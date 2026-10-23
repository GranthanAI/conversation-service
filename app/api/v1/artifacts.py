"""Artifacts API (Mermaid diagrams, Manim animations), routed through the gateway as /api/v1/artifacts.

  POST /artifacts                register-or-get (idempotent). Starts a render ONLY for a genuinely new Manim artifact.
  GET  /artifacts                list for a conversation or its hierarchy, grouped by originating conversation.
  GET  /artifacts/{id}           metadata/status (owner-scoped; someone else's id is a 404).
  GET  /artifacts/{id}/media     the stored MP4 (authenticated stream; `?download=1` forces a download).
  POST /artifacts/{id}/retry     explicit, bounded retry of a failed render.

Reading never renders. Opening, refreshing or revisiting a conversation only reads (or re-registers, which is a no-op for
an existing artifact).
"""

import asyncio
import re
import uuid
from datetime import datetime
from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from app.api.deps import get_conversation_service, get_message_repository
from app.artifacts.fingerprint import ArtifactSpecError
from app.artifacts.runtime import get_artifact_service, get_object_store
from app.artifacts.service import ACTIVE, ArtifactNotFound, ArtifactService, RetryLimitReached
from app.core.config import settings
from app.core.logging import logger
from app.models.artifact_models import ArtifactRecord, ArtifactRefRecord
from app.security import CurrentUser, get_current_user
from app.services.hierarchy_service import HierarchyService

router = APIRouter()

_background: set[asyncio.Task] = set()

FAILURE_MESSAGES = {
    "invalid_spec": "The animation specification was not valid.",
    "unsupported_template": "That kind of animation is not supported.",
    "disabled": "Animations are turned off.",
    "timeout": "The animation took too long to render.",
    "timed_out": "The animation took too long to render.",
    "resource_limit": "The animation was too large to render.",
    "invalid_output": "The rendered video could not be verified.",
    "render_error": "The animation could not be rendered.",
    "attempts_exhausted": "The animation could not be rendered after several attempts.",
    "render_job_lost": "The render was interrupted. You can retry it.",
    "budget_exhausted": "Too many animations in progress. Try again shortly.",
}


def _wake(service: ArtifactService, artifact_id: uuid.UUID) -> None:
    """Best-effort immediate advance after the response; the worker is the guaranteed path."""
    task = asyncio.create_task(service.advance(artifact_id))
    _background.add(task)
    task.add_done_callback(_background.discard)


class RegisterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    conversation_id: uuid.UUID
    source_message_id: uuid.UUID
    type: Literal["mermaid", "manim"]
    content: Any = Field(description="Mermaid source (string) or Manim specification (object)")
    block_id: str = Field(default="", max_length=64)
    title: Optional[str] = Field(default=None, max_length=200)


class ArtifactView(BaseModel):
    artifact_id: uuid.UUID
    type: str
    title: str
    status: str
    ready: bool
    active: bool
    failure_code: Optional[str] = None
    message: Optional[str] = None
    retryable: bool = False
    created_at: datetime
    updated_at: datetime
    conversation_id: Optional[uuid.UUID] = None
    source_message_id: Optional[uuid.UUID] = None
    block_id: Optional[str] = None
    mime_type: Optional[str] = None
    size_bytes: Optional[int] = None
    has_media: bool = False
    has_thumbnail: bool = False
    source: Optional[str] = None            # Mermaid source only (the browser renders it); Manim specs stay server-side


def view(a: ArtifactRecord, ref: Optional[ArtifactRefRecord] = None) -> ArtifactView:
    return ArtifactView(
        artifact_id=a.artifact_id, type=a.artifact_type, title=a.title, status=a.status, ready=a.status == "ready",
        active=a.status in ACTIVE, failure_code=a.failure_code if a.status == "failed" else None,
        message=(FAILURE_MESSAGES.get(a.failure_code or "", "The animation could not be rendered.")
                 if a.status == "failed" else None),
        retryable=a.status == "failed" and a.artifact_type == "manim" and a.retry_count < settings.ARTIFACT_MAX_MANUAL_RETRIES,
        created_at=a.created_at, updated_at=a.updated_at,
        conversation_id=ref.conversation_id if ref else a.origin_conversation_id,
        source_message_id=ref.source_message_id if ref else a.origin_message_id,
        block_id=ref.block_id if ref else None, mime_type=a.mime_type, size_bytes=a.size_bytes,
        has_media=a.artifact_type == "manim" and a.status == "ready" and bool(a.object_key),
        has_thumbnail=bool(a.thumbnail_key),
        source=a.spec_text if a.artifact_type == "mermaid" else None)


async def _owned_conversation(conversation_id: uuid.UUID, user: CurrentUser, conversations):
    conv = await conversations.get(conversation_id)
    if conv is None or conv.user_id != user.id:          # same answer for "missing" and "not yours"
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Conversation not found")
    return conv


@router.post("", response_model=ArtifactView)
async def register_artifact(
    body: RegisterRequest, response: Response,
    user: CurrentUser = Depends(get_current_user),
    service: ArtifactService = Depends(get_artifact_service),
    conversations=Depends(get_conversation_service),
    messages=Depends(get_message_repository),
):
    await _owned_conversation(body.conversation_id, user, conversations)
    msg = await asyncio.to_thread(messages.get_message, body.conversation_id, body.source_message_id)
    if msg is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Message not found")
    try:
        reg = await service.register(user.id, body.conversation_id, body.source_message_id, body.type, body.content,
                                     block_id=body.block_id, title=body.title)
    except ArtifactSpecError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from None
    response.status_code = status.HTTP_201_CREATED if reg.created else status.HTTP_200_OK
    if reg.artifact.status == "pending":
        _wake(service, reg.artifact.artifact_id)         # new, or left pending by an earlier crash
    return view(reg.artifact)


@router.get("")
async def list_artifacts(
    conversation_id: uuid.UUID,
    scope: Literal["self", "hierarchy", "auto"] = "auto",
    type: Optional[Literal["mermaid", "manim"]] = None,
    q: Optional[str] = Query(default=None, max_length=100),
    user: CurrentUser = Depends(get_current_user),
    service: ArtifactService = Depends(get_artifact_service),
    conversations=Depends(get_conversation_service),
):
    """Artifacts of this conversation, or of its whole authorized hierarchy, grouped by the conversation they came from.
    The tree comes from the conversation catalog on every call; the registry stores no hierarchy."""
    conv = await _owned_conversation(conversation_id, user, conversations)
    hierarchy = await HierarchyService(conversations, settings.EXPORT_MAX_CONVERSATIONS * 4).load(user.id)
    if conv.conversation_id not in hierarchy.nodes:       # archived/active only; defensive for a just-deleted row
        ids = [conv.conversation_id]
    else:
        ids = HierarchyService.scope_ids(hierarchy, conv.conversation_id, scope)
    rows = await service.list_for_conversations(user.id, ids, artifact_type=type, query=q)
    by_conv: dict[uuid.UUID, list[ArtifactView]] = {}
    for artifact, ref in rows:
        by_conv.setdefault(ref.conversation_id, []).append(view(artifact, ref))
    groups = []
    for cid in ids:
        if cid not in by_conv:
            continue
        node = hierarchy.nodes.get(cid)
        groups.append({"conversation_id": cid, "title": node.title if node else conv.title,
                       "parent_conversation_id": node.parent_conversation_id if node else None,
                       "is_current": cid == conv.conversation_id, "artifacts": by_conv[cid]})
    resolved = scope if scope != "auto" else ("self" if conv.parent_conversation_id else "hierarchy")
    root = hierarchy.root_of(conv.conversation_id)[0] if conv.conversation_id in hierarchy.nodes else conv.conversation_id
    return {"scope": resolved, "root_conversation_id": root, "conversation_id": conv.conversation_id,
            "is_root": conv.parent_conversation_id is None, "groups": groups,
            "total": sum(len(g["artifacts"]) for g in groups)}


async def _owned_artifact(service: ArtifactService, user: CurrentUser, artifact_id: uuid.UUID) -> ArtifactRecord:
    row = await service.get(user.id, artifact_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Artifact not found")
    return row


@router.get("/{artifact_id}", response_model=ArtifactView)
async def get_artifact(artifact_id: uuid.UUID, user: CurrentUser = Depends(get_current_user),
                       service: ArtifactService = Depends(get_artifact_service)):
    row = await _owned_artifact(service, user, artifact_id)
    if row.status in ACTIVE:
        _wake(service, row.artifact_id)                  # reading status may nudge a stalled artifact, never creates one
    return view(row)


@router.post("/{artifact_id}/retry", response_model=ArtifactView)
async def retry_artifact(artifact_id: uuid.UUID, user: CurrentUser = Depends(get_current_user),
                         service: ArtifactService = Depends(get_artifact_service)):
    try:
        row, restarted = await service.retry(user.id, artifact_id)
    except ArtifactNotFound:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Artifact not found") from None
    except RetryLimitReached:
        raise HTTPException(status.HTTP_409_CONFLICT, {"code": "retry_limit",
                            "message": "This animation has been retried too many times."}) from None
    if restarted:
        _wake(service, row.artifact_id)
    return view(row)


_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(title: str, fallback: str, ext: str) -> str:
    stem = _SAFE_NAME.sub("-", title).strip("-.")[:60] or fallback
    return f"{stem}{ext}"


@router.get("/{artifact_id}/media")
async def get_media(artifact_id: uuid.UUID, download: bool = False, user: CurrentUser = Depends(get_current_user),
                    service: ArtifactService = Depends(get_artifact_service)):
    row = await _owned_artifact(service, user, artifact_id)
    if row.artifact_type != "manim" or row.status != "ready" or not row.object_key:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Media not available")
    store = get_object_store()
    info = await store.stat(row.object_key)
    if info is None:
        await service.mark_object_missing(row.artifact_id)
        _wake(service, row.artifact_id)
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Media not available")
    headers = {"Cache-Control": "private, max-age=3600", "X-Content-Type-Options": "nosniff",
               "Content-Length": str(info.size)}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="{safe_filename(row.title, "animation", ".mp4")}"'
    logger.info("artifact_media_served", artifact_id=str(row.artifact_id))
    return StreamingResponse(store.stream(row.object_key), media_type="video/mp4", headers=headers)


@router.get("/{artifact_id}/thumbnail")
async def get_thumbnail(artifact_id: uuid.UUID, user: CurrentUser = Depends(get_current_user),
                        service: ArtifactService = Depends(get_artifact_service)):
    row = await _owned_artifact(service, user, artifact_id)
    if row.status != "ready" or not row.thumbnail_key:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Thumbnail not available")
    store = get_object_store()
    info = await store.stat(row.thumbnail_key)
    if info is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Thumbnail not available")
    return StreamingResponse(store.stream(row.thumbnail_key), media_type="image/png",
                             headers={"Cache-Control": "private, max-age=3600", "X-Content-Type-Options": "nosniff",
                                      "Content-Length": str(info.size)})
