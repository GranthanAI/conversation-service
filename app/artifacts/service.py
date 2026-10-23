"""Artifact registry service: idempotent registration and the render lifecycle.

    pending -> queued -> rendering -> uploading -> ready          (Mermaid is registered directly as `ready`)
        any non-terminal -> failed        failed -> pending (explicit, bounded retry)

Guarantees
  * One row per (owner, type, fingerprint): enforced by a UNIQUE constraint, so concurrent identical registrations
    resolve to the same artifact without any application-level locking.
  * Registering something that already exists never renders: a `ready` artifact is returned as-is, an active one is
    shared. Only a genuinely new row starts a render, and only an explicit retry restarts a failed one.
  * Every state change is a compare-and-set on the expected current status, and each artifact is advanced by one worker
    at a time through a database lease, so duplicate deliveries / concurrent workers cannot double-render or regress.
  * A crash after the MP4 was uploaded but before the row was updated is reconciled from the deterministic object key:
    the object is validated and adopted instead of rendering again.
  * `ready` is only written after the MP4 passed structural validation, was stored, and its size was confirmed.
"""

import asyncio
import json
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.artifacts.fingerprint import (
    FINGERPRINT_VERSION, ArtifactSpecError, artifact_id_for, canonical_manim_spec, canonical_mermaid_source,
    compute_fingerprint, thumbnail_object_key, title_for, video_object_key,
)
from app.artifacts.media import InvalidMedia, validate_mp4
from app.artifacts.render_backend import MAX_THUMBNAIL_BYTES, RenderBackend, RenderRejected, RenderUnavailable
from app.artifacts.storage import ObjectStore
from app.core.config import Settings, settings as default_settings
from app.core.logging import logger
from app.models.artifact_models import ArtifactRecord, ArtifactRefRecord

TERMINAL = ("ready", "failed")
ACTIVE = ("pending", "queued", "rendering", "uploading")
_BACKOFF_SECONDS = 10.0
_PNG_MAGIC = bytes([0x89]) + b"PNG" + bytes([0x0D, 0x0A, 0x1A, 0x0A])


class ArtifactNotFound(LookupError):
    pass


class RetryLimitReached(Exception):
    pass


@dataclass(frozen=True)
class Registration:
    artifact: ArtifactRecord
    created: bool               # a new registry row was inserted (False = an existing artifact was reused)


def _lease(seconds: float):
    return func.now() + func.make_interval(0, 0, 0, 0, 0, 0, seconds)


class ArtifactService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], store: ObjectStore, backend: RenderBackend,
                 cfg: Settings = default_settings) -> None:
        self.sf = session_factory
        self.store = store
        self.backend = backend
        self.cfg = cfg

    # ------------------------------------------------------------------------------------------ registration

    async def register(self, owner: uuid.UUID, conversation_id: uuid.UUID, source_message_id: uuid.UUID,
                       artifact_type: str, content, block_id: str = "", title: Optional[str] = None) -> Registration:
        """Idempotent. Raises ArtifactSpecError for unusable content. Never renders by itself: the caller (API) wakes the
        advancer for a newly created `pending` artifact, and the worker picks up anything that was missed."""
        if artifact_type == "mermaid":
            canonical = canonical_mermaid_source(content, self.cfg.ARTIFACT_MAX_MERMAID_CHARS)
            renderer_version, quality = "", ""
        elif artifact_type == "manim":
            canonical = canonical_manim_spec(content, self.cfg.ARTIFACT_MAX_MANIM_SPEC_CHARS)
            renderer_version = await self.backend.renderer_version()
            quality = self.cfg.ARTIFACT_RENDER_QUALITY
        else:
            raise ArtifactSpecError("unsupported artifact type")
        fingerprint = compute_fingerprint(artifact_type, canonical, renderer_version, quality)
        artifact_id = artifact_id_for(owner, artifact_type, fingerprint)
        block_id = (block_id or "")[:64]

        async with self.sf() as session, session.begin():
            inserted = (await session.execute(
                pg_insert(ArtifactRecord).values(
                    artifact_id=artifact_id, owner_user_id=owner, artifact_type=artifact_type,
                    title=title_for(artifact_type, canonical, title), fingerprint=fingerprint,
                    fingerprint_version=FINGERPRINT_VERSION, spec_text=canonical,
                    status="ready" if artifact_type == "mermaid" else "pending",
                    renderer_version=f"{renderer_version}:{quality}" if renderer_version else "",
                    origin_conversation_id=conversation_id, origin_message_id=source_message_id,
                ).on_conflict_do_nothing().returning(ArtifactRecord.artifact_id))).scalar_one_or_none()
            await session.execute(
                pg_insert(ArtifactRefRecord).values(
                    artifact_id=artifact_id, conversation_id=conversation_id, source_message_id=source_message_id,
                    block_id=block_id, owner_user_id=owner).on_conflict_do_nothing())
            row = (await session.execute(select(ArtifactRecord).where(ArtifactRecord.artifact_id == artifact_id))).scalar_one()
        return Registration(row, created=inserted is not None)

    async def current_renderer(self) -> tuple[str, str]:
        """(renderer/template version, render quality): the render-affecting configuration part of every Manim fingerprint."""
        return await self.backend.renderer_version(), self.cfg.ARTIFACT_RENDER_QUALITY

    # ------------------------------------------------------------------------------------------ reads

    async def get(self, owner: uuid.UUID, artifact_id: uuid.UUID) -> Optional[ArtifactRecord]:
        """Owner-scoped: someone else's artifact is indistinguishable from a missing one."""
        async with self.sf() as session:
            return (await session.execute(select(ArtifactRecord).where(
                ArtifactRecord.artifact_id == artifact_id, ArtifactRecord.owner_user_id == owner))).scalar_one_or_none()

    async def lookup(self, owner: uuid.UUID, artifact_type: str, canonical_text: str,
                     renderer_version: str = "", quality: str = "") -> Optional[ArtifactRecord]:
        """Find without creating (used by exports)."""
        fp = compute_fingerprint(artifact_type, canonical_text, renderer_version, quality)
        async with self.sf() as session:
            return (await session.execute(select(ArtifactRecord).where(
                ArtifactRecord.owner_user_id == owner, ArtifactRecord.artifact_type == artifact_type,
                ArtifactRecord.fingerprint == fp))).scalar_one_or_none()

    async def list_for_conversations(
        self, owner: uuid.UUID, conversation_ids: Sequence[uuid.UUID], artifact_type: Optional[str] = None,
        query: Optional[str] = None, limit: int = 500,
    ) -> list[tuple[ArtifactRecord, ArtifactRefRecord]]:
        """One (artifact, first reference) per artifact and conversation, newest first. A single query: no N+1."""
        if not conversation_ids:
            return []
        stmt = (select(ArtifactRecord, ArtifactRefRecord)
                .join(ArtifactRefRecord, ArtifactRefRecord.artifact_id == ArtifactRecord.artifact_id)
                .where(ArtifactRecord.owner_user_id == owner, ArtifactRefRecord.owner_user_id == owner,
                       ArtifactRefRecord.conversation_id.in_(list(conversation_ids))))
        if artifact_type:
            stmt = stmt.where(ArtifactRecord.artifact_type == artifact_type)
        if query:
            escaped = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            stmt = stmt.where(ArtifactRecord.title.ilike(f"%{escaped}%", escape="\\"))
        stmt = (stmt.distinct(ArtifactRecord.artifact_id, ArtifactRefRecord.conversation_id)
                .order_by(ArtifactRecord.artifact_id, ArtifactRefRecord.conversation_id, ArtifactRefRecord.created_at,
                          ArtifactRefRecord.source_message_id))
        async with self.sf() as session:
            rows = (await session.execute(stmt)).all()
        rows.sort(key=lambda r: (r[0].created_at, str(r[0].artifact_id)), reverse=True)
        return [(a, r) for a, r in rows[:limit]]

    # ------------------------------------------------------------------------------------------ mutations (CAS)

    async def _cas(self, artifact_id: uuid.UUID, allowed_from: Iterable[str], **values) -> Optional[ArtifactRecord]:
        async with self.sf() as session, session.begin():
            res = await session.execute(
                update(ArtifactRecord)
                .where(ArtifactRecord.artifact_id == artifact_id, ArtifactRecord.status.in_(list(allowed_from)))
                .values(updated_at=func.now(), **values).returning(ArtifactRecord))
            return res.scalar_one_or_none()

    async def _release(self, artifact_id: uuid.UUID) -> None:
        async with self.sf() as session, session.begin():
            await session.execute(update(ArtifactRecord).where(ArtifactRecord.artifact_id == artifact_id)
                                  .values(lease_until=None))

    async def _backoff(self, artifact_id: uuid.UUID) -> None:
        async with self.sf() as session, session.begin():
            await session.execute(update(ArtifactRecord).where(ArtifactRecord.artifact_id == artifact_id)
                                  .values(lease_until=_lease(_BACKOFF_SECONDS)))

    async def _fail(self, artifact_id: uuid.UUID, code: str) -> None:
        logger.info("artifact_failed", artifact_id=str(artifact_id), code=code)
        await self._cas(artifact_id, ACTIVE, status="failed", failure_code=code[:48], lease_until=None)

    async def retry(self, owner: uuid.UUID, artifact_id: uuid.UUID) -> tuple[ArtifactRecord, bool]:
        """Explicit retry of a failed render. Idempotent (only `failed` moves), bounded, and it re-enters the normal
        lifecycle, so it can never create a second artifact."""
        row = await self.get(owner, artifact_id)
        if row is None:
            raise ArtifactNotFound(str(artifact_id))
        if row.status != "failed" or row.artifact_type != "manim":
            return row, False
        if row.retry_count >= self.cfg.ARTIFACT_MAX_MANUAL_RETRIES:
            raise RetryLimitReached()
        updated = await self._cas(artifact_id, ("failed",), status="pending", failure_code=None, attempt_count=0,
                                  retry_count=row.retry_count + 1, render_job_id=None, lease_until=None)
        return (updated or await self.get(owner, artifact_id)), updated is not None

    async def mark_object_missing(self, artifact_id: uuid.UUID) -> None:
        """The stored video disappeared: put the artifact back into the lifecycle so it is reconciled/re-rendered once."""
        await self._cas(artifact_id, ("ready",), status="pending", object_key=None, checksum_sha256=None, size_bytes=None,
                        mime_type=None, render_job_id=None, attempt_count=0, failure_code=None, lease_until=None)
        logger.warning("artifact_object_missing", artifact_id=str(artifact_id))

    # ------------------------------------------------------------------------------------------ lifecycle

    async def _claim(self, artifact_id: uuid.UUID) -> Optional[ArtifactRecord]:
        """Atomic lease: exactly one caller advances an artifact at a time; a crashed holder's lease simply expires."""
        async with self.sf() as session, session.begin():
            res = await session.execute(
                update(ArtifactRecord)
                .where(ArtifactRecord.artifact_id == artifact_id, ArtifactRecord.status.in_(ACTIVE),
                       (ArtifactRecord.lease_until.is_(None)) | (ArtifactRecord.lease_until < func.now()))
                .values(lease_until=_lease(self.cfg.ARTIFACT_LEASE_SECONDS)).returning(ArtifactRecord))
            return res.scalar_one_or_none()

    async def advance(self, artifact_id: uuid.UUID) -> None:
        """Move one artifact forward as far as it can go without waiting. Safe to call any number of times, from any
        process, in any order (duplicate queue deliveries, repeated wake-ups, worker restarts)."""
        row = await self._claim(artifact_id)
        if row is None:
            return
        keep_lease = False
        try:
            for _ in range(5):                          # pending -> queued -> uploading -> ready in one pass when possible
                before = row.status
                row = await self._step(row)
                if row is None or row.status in TERMINAL or row.status == before:
                    break                               # finished, failed, lost a race, or simply still waiting
        except (RenderUnavailable, OSError) as exc:
            logger.warning("artifact_step_unavailable", artifact_id=str(artifact_id), error_type=type(exc).__name__)
            await self._backoff(artifact_id)
            keep_lease = True
        except Exception as exc:                        # storage outage etc.: stay in state, retry after a pause
            logger.error("artifact_step_error", artifact_id=str(artifact_id), error_type=type(exc).__name__)
            await self._backoff(artifact_id)
            keep_lease = True
        finally:
            if not keep_lease:
                await self._release(artifact_id)

    async def _step(self, row: ArtifactRecord) -> Optional[ArtifactRecord]:
        if row.status == "pending":
            return await self._dispatch(row)
        if row.status in ("queued", "rendering"):
            return await self._poll(row)
        if row.status == "uploading":
            return await self._upload(row)
        return None

    async def _dispatch(self, row: ArtifactRecord) -> Optional[ArtifactRecord]:
        # Crash recovery first: a verified video may already exist at the deterministic key.
        adopted = await self._adopt_existing_object(row)
        if adopted is not None:
            return adopted
        if row.attempt_count >= self.cfg.ARTIFACT_MAX_AUTO_ATTEMPTS:
            await self._fail(row.artifact_id, "attempts_exhausted")
            return None
        try:
            job = await self.backend.submit(str(row.owner_user_id), json.loads(row.spec_text))
        except RenderRejected as exc:
            await self._fail(row.artifact_id, exc.code)
            return None
        return await self._cas(row.artifact_id, ("pending",), status="queued", render_job_id=job.job_id,
                               attempt_count=row.attempt_count + 1, failure_code=None)

    async def _poll(self, row: ArtifactRecord) -> Optional[ArtifactRecord]:
        age = (datetime.now(timezone.utc) - row.updated_at).total_seconds()
        job = await self.backend.status(str(row.owner_user_id), row.render_job_id or "")
        if job is None:                                 # llm-service lost the job (restart / expiry): resubmit, bounded
            if row.attempt_count >= self.cfg.ARTIFACT_MAX_AUTO_ATTEMPTS:
                await self._fail(row.artifact_id, "render_job_lost")
                return None
            return await self._cas(row.artifact_id, ("queued", "rendering"), status="pending", render_job_id=None)
        if job.state == "ready":
            return await self._cas(row.artifact_id, ("queued", "rendering"), status="uploading")
        if job.state in ("failed", "cancelled", "expired"):
            await self._fail(row.artifact_id, (job.failure or job.state))
            return None
        if age > self.cfg.ARTIFACT_MAX_ACTIVE_SECONDS:
            await self._fail(row.artifact_id, "timed_out")
            return None
        wanted = "queued" if job.state == "pending" else "rendering"
        if row.status != wanted:
            return await self._cas(row.artifact_id, ("queued", "rendering"), status=wanted)
        return row

    async def _upload(self, row: ArtifactRecord) -> Optional[ArtifactRecord]:
        adopted = await self._adopt_existing_object(row)
        if adopted is not None:
            return adopted
        key = video_object_key(row.artifact_id)
        with tempfile.TemporaryDirectory(prefix="artifact_") as tmp:
            dest = Path(tmp) / "video.mp4"
            try:
                await self.backend.download(str(row.owner_user_id), row.render_job_id or "", dest)
            except FileNotFoundError:                   # the rendered file is gone: render again (bounded)
                if row.attempt_count >= self.cfg.ARTIFACT_MAX_AUTO_ATTEMPTS:
                    await self._fail(row.artifact_id, "render_job_lost")
                    return None
                return await self._cas(row.artifact_id, ("uploading",), status="pending", render_job_id=None)
            except RenderRejected as exc:
                await self._fail(row.artifact_id, exc.code)
                return None
            try:
                info = validate_mp4(dest, self.cfg.ARTIFACT_MAX_VIDEO_BYTES)
            except InvalidMedia:
                await self._fail(row.artifact_id, "invalid_output")
                return None
            await self.store.put_file(key, dest, "video/mp4", {"sha256": info.sha256})
            stored = await self.store.stat(key)
            if stored is None or stored.size != info.size:
                raise OSError("uploaded object could not be confirmed")
            thumb_key = await self._store_thumbnail(row, Path(tmp) / "thumb.png")
        return await self._cas(row.artifact_id, ("uploading", "pending", "queued", "rendering"), status="ready",
                               object_key=key, thumbnail_key=thumb_key, mime_type="video/mp4", size_bytes=info.size,
                               checksum_sha256=info.sha256, failure_code=None, lease_until=None)

    async def _store_thumbnail(self, row: ArtifactRecord, local: Path) -> Optional[str]:
        """Best-effort preview frame: any problem simply means the artifact has no thumbnail."""
        try:
            if not await self.backend.download_thumbnail(str(row.owner_user_id), row.render_job_id or "", local):
                return None
            data = local.read_bytes()
            if not data.startswith(_PNG_MAGIC) or len(data) > MAX_THUMBNAIL_BYTES:
                return None
            key = thumbnail_object_key(row.artifact_id)
            await self.store.put_file(key, local, "image/png")
            return key
        except Exception as exc:
            logger.warning("artifact_thumbnail_skipped", error_type=type(exc).__name__)
            return None

    async def _adopt_existing_object(self, row: ArtifactRecord) -> Optional[ArtifactRecord]:
        """If a complete, valid video is already stored under the deterministic key, mark the artifact ready without
        rendering. An invalid leftover is deleted so it can never be served."""
        key = video_object_key(row.artifact_id)
        if await self.store.stat(key) is None:
            return None
        with tempfile.TemporaryDirectory(prefix="artifact_") as tmp:
            local = Path(tmp) / "video.mp4"
            await self.store.get_to_file(key, local)
            try:
                info = validate_mp4(local, self.cfg.ARTIFACT_MAX_VIDEO_BYTES)
            except InvalidMedia:
                await self.store.delete(key)
                logger.warning("artifact_invalid_object_deleted", artifact_id=str(row.artifact_id))
                return None
        logger.info("artifact_object_adopted", artifact_id=str(row.artifact_id))
        thumb = thumbnail_object_key(row.artifact_id)
        return await self._cas(row.artifact_id, ACTIVE, status="ready", object_key=key,
                               thumbnail_key=thumb if await self.store.stat(thumb) else None, mime_type="video/mp4",
                               size_bytes=info.size, checksum_sha256=info.sha256, failure_code=None, lease_until=None)

    # ------------------------------------------------------------------------------------------ worker / janitor

    async def active_ids(self, limit: int = 25) -> list[uuid.UUID]:
        async with self.sf() as session:
            res = await session.execute(
                select(ArtifactRecord.artifact_id)
                .where(ArtifactRecord.status.in_(ACTIVE),
                       (ArtifactRecord.lease_until.is_(None)) | (ArtifactRecord.lease_until < func.now()))
                .order_by(ArtifactRecord.updated_at).limit(limit))
            return list(res.scalars())

    async def run_once(self) -> int:
        ids = await self.active_ids()
        for artifact_id in ids:
            try:
                await self.advance(artifact_id)
            except Exception as exc:
                logger.error("artifact_worker_error", error_type=type(exc).__name__)
        return len(ids)

    async def delete_orphan_objects(self, grace_seconds: float = 86400.0) -> int:
        """Objects under artifacts/ with no registry row (an aborted registration, a manual delete) older than the grace
        period. Registered artifacts are never touched."""
        import time
        removed = 0
        cutoff = time.time() - grace_seconds
        for info in await self.store.list("artifacts/"):
            parts = info.key.split("/")
            if len(parts) != 3 or info.last_modified > cutoff:
                continue
            try:
                artifact_id = uuid.UUID(parts[1])
            except ValueError:
                continue
            async with self.sf() as session:
                known = (await session.execute(select(ArtifactRecord.artifact_id).where(
                    ArtifactRecord.artifact_id == artifact_id))).scalar_one_or_none()
            if known is None:
                await self.store.delete(info.key)
                removed += 1
        return removed


class ArtifactWorker:
    """Background loop: advances active artifacts (the safety net behind the request-time wake-up) and, rarely, removes
    orphaned objects."""

    def __init__(self, service: ArtifactService, interval: float, janitor_every: float = 3600.0) -> None:
        self.service, self.interval, self.janitor_every = service, interval, janitor_every

    async def run(self) -> None:
        import time
        last_janitor = time.monotonic()
        while True:
            try:
                await self.service.run_once()
                if time.monotonic() - last_janitor > self.janitor_every:
                    last_janitor = time.monotonic()
                    await self.service.delete_orphan_objects()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("artifact_worker_loop_error", error_type=type(exc).__name__)
            await asyncio.sleep(self.interval)
