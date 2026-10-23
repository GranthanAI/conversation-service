"""Conversation export API (gateway: /api/v1/exports).

  POST /exports                 create (or reuse) an export job. Idempotent over an unchanged source.
  GET  /exports/{id}            status and progress (owner-scoped).
  GET  /exports/{id}/download   the finished file (authenticated stream; never a public URL).

Formats: Markdown (.md) or PDF (.pdf) for one conversation; for a hierarchy a ZIP of Markdown files (with an optional
bundle of the referenced animation videos) or ONE combined PDF in tree order. A hierarchy export always resolves to the
root of the selected conversation's tree: root, the selected conversation, its siblings and all descendants.
"""

import asyncio
import uuid
from datetime import datetime
from typing import Literal, Optional
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict

from app.api.deps import get_conversation_service, get_message_repository
from app.artifacts.runtime import get_artifact_service, get_object_store
from app.core.config import settings
from app.exports.collector import SourceMissing
from app.exports.service import ExportLimitReached, ExportService, ExportTooLarge
from app.models.artifact_models import ExportJobRecord
from app.security import CurrentUser, get_current_user

router = APIRouter()
_background: set[asyncio.Task] = set()

FAILURE_MESSAGES = {
    "source_missing": "The conversation no longer exists.",
    "too_large": "This export is too large. Try exporting a smaller part of the hierarchy.",
    "pdf_unavailable": "PDF export is not available on this server.",
    "export_error": "The export could not be completed. Please try again.",
}


def get_export_service(conversations=Depends(get_conversation_service), messages=Depends(get_message_repository),
                       artifacts=Depends(get_artifact_service)) -> ExportService:
    from app.db.session import get_session_factory
    return ExportService(get_session_factory(), get_object_store(), conversations, messages, artifacts)


class ExportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    conversation_id: uuid.UUID
    scope: Literal["conversation", "hierarchy"] = "conversation"
    format: Literal["markdown", "pdf"] = "markdown"
    include_media: bool = False


class Progress(BaseModel):
    done: int
    total: int


class ExportView(BaseModel):
    job_id: uuid.UUID
    status: str
    conversation_id: uuid.UUID
    scope: str
    format: str
    include_media: bool
    progress: Progress
    filename: Optional[str] = None
    content_type: Optional[str] = None
    size_bytes: Optional[int] = None
    expires_at: Optional[datetime] = None
    failure_code: Optional[str] = None
    message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    download_ready: bool = False


def view(j: ExportJobRecord) -> ExportView:
    return ExportView(
        job_id=j.job_id, status=j.status, conversation_id=j.conversation_id, scope=j.scope, format=j.format,
        include_media=j.include_media, progress=Progress(done=j.progress_done, total=j.progress_total),
        filename=j.filename if j.status == "ready" else None, content_type=j.content_type if j.status == "ready" else None,
        size_bytes=j.size_bytes if j.status == "ready" else None, expires_at=j.expires_at,
        failure_code=j.failure_code if j.status == "failed" else None,
        message=FAILURE_MESSAGES.get(j.failure_code or "", "The export failed.") if j.status == "failed" else (
            "This export has expired; create it again." if j.status == "expired" else None),
        created_at=j.created_at, updated_at=j.updated_at, download_ready=j.status == "ready")


def _wake(service: ExportService, job_id: uuid.UUID) -> None:
    task = asyncio.create_task(service.process(job_id))
    _background.add(task)
    task.add_done_callback(_background.discard)


@router.post("", response_model=ExportView)
async def create_export(body: ExportRequest, response: Response, user: CurrentUser = Depends(get_current_user),
                        service: ExportService = Depends(get_export_service), conversations=Depends(get_conversation_service)):
    conv = await conversations.get(body.conversation_id)
    if conv is None or conv.user_id != user.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Conversation not found")
    try:
        job, created = await service.create(user.id, body.conversation_id, body.scope, body.format, body.include_media)
    except SourceMissing:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Conversation not found") from None
    except ExportTooLarge:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, {
            "code": "too_large", "message": FAILURE_MESSAGES["too_large"]}) from None
    except ExportLimitReached:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, {
            "code": "too_many_exports", "message": "Too many exports in progress. Wait for one to finish."}) from None
    response.status_code = status.HTTP_202_ACCEPTED if created else status.HTTP_200_OK
    if created and body.scope == "conversation" and body.format == "markdown":
        await service.process(job.job_id)                    # small and cheap: finished before the response
        job = await service.get(user.id, job.job_id) or job
    elif job.status in ("pending", "processing"):
        _wake(service, job.job_id)
    return view(job)


async def _owned(service: ExportService, user: CurrentUser, job_id: uuid.UUID) -> ExportJobRecord:
    job = await service.get(user.id, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Export not found")
    return job


@router.get("/{job_id}", response_model=ExportView)
async def get_export(job_id: uuid.UUID, user: CurrentUser = Depends(get_current_user),
                     service: ExportService = Depends(get_export_service)):
    job = await _owned(service, user, job_id)
    if job.status == "ready" and job.expires_at and job.expires_at.timestamp() < _now():
        job.status = "expired"                               # expired but not yet swept: never offer a download
    return view(job)


def _now() -> float:
    import time
    return time.time()


@router.get("/{job_id}/download")
async def download_export(job_id: uuid.UUID, user: CurrentUser = Depends(get_current_user),
                          service: ExportService = Depends(get_export_service)):
    job = await _owned(service, user, job_id)
    if job.status != "ready" or not job.object_key or (job.expires_at and job.expires_at.timestamp() < _now()):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Export not available")
    store = get_object_store()
    info = await store.stat(job.object_key)
    if info is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Export not available")
    name = job.filename or "export"
    headers = {"Content-Disposition": f"attachment; filename=\"{name}\"; filename*=UTF-8''{quote(name)}",
               "Content-Length": str(info.size), "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"}
    return StreamingResponse(store.stream(job.object_key), media_type=job.content_type or "application/octet-stream",
                             headers=headers)
