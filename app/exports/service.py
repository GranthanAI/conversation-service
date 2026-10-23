"""Export jobs: idempotent creation keyed on the source snapshot, asynchronous processing with leases, retention.

    pending -> processing -> ready -> expired          processing -> failed (permanent) | pending (transient, bounded)

Why this is safe to retry and cache:
  * `request_key` = sha256(owner, selected conversation, scope, format, media option, the exact set of conversations and
    each one's latest message). The same request over an unchanged source reuses the live job (also for concurrent
    requests, via a partial UNIQUE index); any new message or branch changes the key, so a stale file is never served.
  * The job stores the snapshot and the processor never reads past it, so the archive matches the key it was created for.
  * Processing is claimed with a lease (one worker at a time; a crashed worker's lease expires and the job resumes) and
    attempts are bounded.
"""

import asyncio
import hashlib
import json
import tempfile
import uuid
from pathlib import Path
from typing import Optional
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.artifacts.service import ArtifactService
from app.artifacts.storage import ObjectStore, sha256_file
from app.core.config import Settings, settings as default_settings
from app.core.logging import logger
from app.exports.archive import build_hierarchy_zip
from app.exports.collector import ExportCollector, SourceMissing
from app.exports.naming import conversation_dir
from app.exports.render_markdown import AssetLinks, render_conversation_markdown
from app.exports.render_pdf import PdfUnavailable, render_pdf
from app.models.artifact_models import ExportJobRecord
from app.services.hierarchy_service import HierarchyService

LIVE = ("pending", "processing", "ready")
SCOPES = ("conversation", "hierarchy")
FORMATS = ("markdown", "pdf")
MAX_THUMB_BYTES = 2 * 1024 * 1024


class ExportTooLarge(Exception):
    pass


class ExportLimitReached(Exception):
    pass


def _secs(seconds: float):
    return func.now() + func.make_interval(0, 0, 0, 0, 0, 0, seconds)


class ExportService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], store: ObjectStore, conversations, messages,
                 artifacts: ArtifactService, cfg: Settings = default_settings) -> None:
        self.sf, self.store, self.cfg = session_factory, store, cfg
        self.artifacts = artifacts
        self.hierarchy = HierarchyService(conversations, cfg.EXPORT_MAX_CONVERSATIONS * 4)
        self.collector = ExportCollector(conversations, messages, artifacts, cfg.EXPORT_MAX_MESSAGES_PER_CONVERSATION)

    # ------------------------------------------------------------------------------------------ creation

    async def _scope_ids(self, owner: UUID, conversation_id: UUID, scope: str):
        h = await self.hierarchy.load(owner)
        if conversation_id not in h.nodes:
            raise SourceMissing(str(conversation_id))
        ids = [conversation_id] if scope == "conversation" else h.subtree(h.root_of(conversation_id)[0])
        if len(ids) > self.cfg.EXPORT_MAX_CONVERSATIONS:
            raise ExportTooLarge()
        return h, ids

    async def create(self, owner: UUID, conversation_id: UUID, scope: str, fmt: str,
                     include_media: bool = False) -> tuple[ExportJobRecord, bool]:
        """Returns (job, created). `created` is False when an equivalent live job already exists."""
        assert scope in SCOPES and fmt in FORMATS
        include_media = bool(include_media and scope == "hierarchy" and fmt == "markdown")   # media only travels in the ZIP
        _, ids = await self._scope_ids(owner, conversation_id, scope)
        snapshot = await self.collector.snapshot(ids)
        request_key = hashlib.sha256(json.dumps(
            {"o": str(owner), "c": str(conversation_id), "s": scope, "f": fmt, "m": include_media,
             "snap": sorted(snapshot.items())}, sort_keys=True).encode()).hexdigest()
        job_id = uuid.uuid4()
        async with self.sf() as session:
            async with session.begin():
                # a `ready` export past its expiry is not reusable
                await session.execute(update(ExportJobRecord).where(
                    ExportJobRecord.owner_user_id == owner, ExportJobRecord.request_key == request_key,
                    ExportJobRecord.status == "ready", ExportJobRecord.expires_at < func.now()).values(status="expired"))
                live = (await session.execute(select(func.count()).select_from(ExportJobRecord).where(
                    ExportJobRecord.owner_user_id == owner, ExportJobRecord.status.in_(("pending", "processing"))))).scalar_one()
                existing = (await session.execute(select(ExportJobRecord).where(
                    ExportJobRecord.owner_user_id == owner, ExportJobRecord.request_key == request_key,
                    ExportJobRecord.status.in_(LIVE)))).scalar_one_or_none()
                if existing is not None:
                    return existing, False
                if live >= self.cfg.EXPORT_MAX_LIVE_PER_USER:
                    raise ExportLimitReached()
                inserted = (await session.execute(
                    pg_insert(ExportJobRecord).values(
                        job_id=job_id, owner_user_id=owner, conversation_id=conversation_id, scope=scope, format=fmt,
                        include_media=include_media, request_key=request_key, snapshot=snapshot,
                        progress_total=len(ids)).on_conflict_do_nothing().returning(ExportJobRecord.job_id))
                    ).scalar_one_or_none()
                row = (await session.execute(select(ExportJobRecord).where(
                    ExportJobRecord.owner_user_id == owner, ExportJobRecord.request_key == request_key,
                    ExportJobRecord.status.in_(LIVE)))).scalar_one()
        return row, inserted is not None

    async def get(self, owner: UUID, job_id: UUID) -> Optional[ExportJobRecord]:
        async with self.sf() as session:
            row = (await session.execute(select(ExportJobRecord).where(
                ExportJobRecord.job_id == job_id, ExportJobRecord.owner_user_id == owner))).scalar_one_or_none()
        return row

    # ------------------------------------------------------------------------------------------ processing

    async def _claim(self, job_id: UUID) -> Optional[ExportJobRecord]:
        async with self.sf() as session, session.begin():
            res = await session.execute(
                update(ExportJobRecord)
                .where(ExportJobRecord.job_id == job_id, ExportJobRecord.status.in_(("pending", "processing")),
                       ExportJobRecord.attempt_count < self.cfg.EXPORT_MAX_ATTEMPTS,
                       (ExportJobRecord.lease_until.is_(None)) | (ExportJobRecord.lease_until < func.now()))
                .values(status="processing", attempt_count=ExportJobRecord.attempt_count + 1,
                        lease_until=_secs(self.cfg.EXPORT_LEASE_SECONDS), updated_at=func.now())
                .returning(ExportJobRecord))
            return res.scalar_one_or_none()

    async def _set(self, job_id: UUID, allowed: tuple[str, ...], **values) -> bool:
        async with self.sf() as session, session.begin():
            res = await session.execute(update(ExportJobRecord).where(
                ExportJobRecord.job_id == job_id, ExportJobRecord.status.in_(allowed))
                .values(updated_at=func.now(), **values).returning(ExportJobRecord.job_id))
            return res.scalar_one_or_none() is not None

    async def _fail(self, job_id: UUID, code: str) -> None:
        logger.info("export_failed", job_id=str(job_id), code=code)
        await self._set(job_id, ("pending", "processing"), status="failed", failure_code=code, lease_until=None)

    async def process(self, job_id: UUID) -> None:
        job = await self._claim(job_id)
        if job is None:
            return
        try:
            await self._produce(job)
        except SourceMissing:
            await self._fail(job_id, "source_missing")
        except ExportTooLarge:
            await self._fail(job_id, "too_large")
        except PdfUnavailable:
            await self._fail(job_id, "pdf_unavailable")
        except Exception as exc:
            logger.error("export_error", job_id=str(job_id), error_type=type(exc).__name__)
            if job.attempt_count >= self.cfg.EXPORT_MAX_ATTEMPTS:
                await self._fail(job_id, "export_error")
            else:                                   # transient: release and let the worker retry after the lease backoff
                await self._set(job_id, ("processing",), status="pending", lease_until=_secs(10.0))

    async def _produce(self, job: ExportJobRecord) -> None:
        owner, selected = job.owner_user_id, job.conversation_id
        h = await self.hierarchy.load(owner)
        if selected not in h.nodes:
            raise SourceMissing(str(selected))
        if job.scope == "conversation":
            ids = [selected]
        else:
            ids = [c for c in h.subtree(h.root_of(selected)[0]) if str(c) in job.snapshot]     # exactly the snapshot
        renderer, quality = await self.artifacts.current_renderer()

        async def progress(done: int, total: int) -> None:
            await self._set(job.job_id, ("processing",), progress_done=done, progress_total=total)

        bundle = await self.collector.collect(owner, h, selected, ids, job.snapshot, renderer, quality, progress)
        with tempfile.TemporaryDirectory(prefix="export_") as tmp:
            work = Path(tmp)
            out, filename, ctype = await self._render(job, bundle, work)
            size = out.stat().st_size
            if size > self.cfg.EXPORT_MAX_ARCHIVE_BYTES:
                raise ExportTooLarge()
            digest = sha256_file(out)
            key = f"exports/{job.job_id}/{filename}"
            await self.store.put_file(key, out, ctype, {"sha256": digest})
            stored = await self.store.stat(key)
            if stored is None or stored.size != size:
                raise OSError("uploaded export could not be confirmed")
        ok = await self._set(job.job_id, ("processing",), status="ready", object_key=key, filename=filename,
                             content_type=ctype, size_bytes=size, checksum_sha256=digest, failure_code=None,
                             lease_until=None, progress_done=len(bundle.conversations),
                             progress_total=len(bundle.conversations), expires_at=_secs(self.cfg.EXPORT_RETENTION_HOURS * 3600))
        if not ok:
            await self.store.delete(key)            # the job was cancelled/expired underneath us: leave nothing behind

    async def _render(self, job: ExportJobRecord, bundle, work: Path) -> tuple[Path, str, str]:
        first = bundle.conversations[0].conversation if bundle.conversations else None
        base = conversation_dir(first.title if first else "conversation", bundle.selected_id)
        if job.format == "pdf":
            thumbs = {}
            for art in bundle.artifacts.values():
                if art.thumbnail_key:
                    tmp = work / f"{art.artifact_id}.png"
                    try:
                        await self.store.get_to_file(art.thumbnail_key, tmp)
                        if tmp.stat().st_size <= MAX_THUMB_BYTES:
                            thumbs[art.artifact_id] = tmp.read_bytes()
                    except Exception:
                        pass
            pdf = await render_pdf(bundle, thumbs)
            out = work / "export.pdf"
            out.write_bytes(pdf)
            return out, f"{base}.pdf", "application/pdf"
        if job.scope == "conversation":
            md = render_conversation_markdown(bundle.conversations[0], bundle, AssetLinks())
            out = work / "export.md"
            out.write_bytes(md.encode("utf-8"))        # bytes, so line endings are LF on every platform
            return out, f"{base}.md", "text/markdown; charset=utf-8"
        result = await build_hierarchy_zip(
            bundle, self.store, job.include_media, work, self.cfg.EXPORT_MAX_ARCHIVE_BYTES,
            {"scope": job.scope, "format": job.format, "include_media": job.include_media}, dict(job.snapshot),
            self.cfg.ARTIFACT_MAX_VIDEO_BYTES)
        return result.path, result.filename, "application/zip"

    # ------------------------------------------------------------------------------------------ worker / retention

    async def run_once(self) -> int:
        async with self.sf() as session:
            ids = list((await session.execute(
                select(ExportJobRecord.job_id).where(
                    ExportJobRecord.status.in_(("pending", "processing")),
                    (ExportJobRecord.lease_until.is_(None)) | (ExportJobRecord.lease_until < func.now()))
                .order_by(ExportJobRecord.created_at).limit(10))).scalars())
        for job_id in ids:
            await self.process(job_id)
        await self._fail_exhausted()
        return len(ids)

    async def _fail_exhausted(self) -> None:
        async with self.sf() as session, session.begin():
            await session.execute(update(ExportJobRecord).where(
                ExportJobRecord.status.in_(("pending", "processing")),
                ExportJobRecord.attempt_count >= self.cfg.EXPORT_MAX_ATTEMPTS,
                (ExportJobRecord.lease_until.is_(None)) | (ExportJobRecord.lease_until < func.now()))
                .values(status="failed", failure_code="export_error", lease_until=None, updated_at=func.now()))

    async def cleanup(self) -> dict:
        """Expire ready exports past retention (object deleted first, then the row), prune old terminal rows, and remove
        objects no live job refers to."""
        expired = 0
        async with self.sf() as session:
            due = list((await session.execute(select(ExportJobRecord.job_id, ExportJobRecord.object_key).where(
                ExportJobRecord.status == "ready", ExportJobRecord.expires_at < func.now()).limit(100))).all())
        for job_id, key in due:
            if key:
                await self.store.delete(key)
            if await self._set(job_id, ("ready",), status="expired", object_key=None):
                expired += 1
        async with self.sf() as session, session.begin():
            pruned = (await session.execute(delete(ExportJobRecord).where(
                ExportJobRecord.status.in_(("failed", "expired")),
                ExportJobRecord.updated_at < func.now() - func.make_interval(0, 0, 0, 0, 0, 0,
                                                                              self.cfg.EXPORT_RETENTION_HOURS * 3600 * 7)
                ).returning(ExportJobRecord.job_id))).all()
        orphans = 0
        import time
        for info in await self.store.list("exports/"):
            parts = info.key.split("/")
            if len(parts) < 3 or info.last_modified > time.time() - 3600:
                continue
            try:
                job_id = UUID(parts[1])
            except ValueError:
                continue
            async with self.sf() as session:
                status = (await session.execute(select(ExportJobRecord.status).where(
                    ExportJobRecord.job_id == job_id))).scalar_one_or_none()
            if status not in ("ready", "processing"):
                await self.store.delete(info.key)
                orphans += 1
        return {"expired": expired, "pruned": len(pruned), "orphan_objects": orphans}


class ExportWorker:
    def __init__(self, service: ExportService, interval: float, cleanup_every: float = 300.0) -> None:
        self.service, self.interval, self.cleanup_every = service, interval, cleanup_every

    async def run(self) -> None:
        import time
        last = 0.0
        while True:
            try:
                await self.service.run_once()
                if time.monotonic() - last > self.cleanup_every:
                    last = time.monotonic()
                    await self.service.cleanup()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("export_worker_loop_error", error_type=type(exc).__name__)
            await asyncio.sleep(self.interval)
