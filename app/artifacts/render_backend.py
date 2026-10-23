"""The render backend: llm-service's isolated Manim job API.

conversation-service never renders. It submits a validated spec (llm-service re-validates it against the reviewed
templates), polls the job, and downloads the finished MP4 once, to verify it and store it durably. llm-service trusts
`X-User-Id` because it is only reachable on the internal network (the gateway strips client-supplied values).
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol

import httpx

from app.core.config import settings
from app.core.logging import logger


MAX_THUMBNAIL_BYTES = 2 * 1024 * 1024


class RenderUnavailable(Exception):
    """Transient: the renderer cannot be reached right now. The artifact stays in its state and is retried."""


class RenderRejected(Exception):
    """Permanent: the renderer will never accept this spec (invalid/unsupported/disabled)."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class RenderJob:
    job_id: str
    state: str                       # pending|validating|rendering|repairing|ready|failed|cancelled|expired
    failure: Optional[str] = None


class RenderBackend(Protocol):
    async def renderer_version(self) -> str: ...
    async def submit(self, owner: str, spec: dict) -> RenderJob: ...
    async def status(self, owner: str, job_id: str) -> Optional[RenderJob]: ...
    async def download(self, owner: str, job_id: str, dest: Path) -> int: ...
    async def download_thumbnail(self, owner: str, job_id: str, dest: Path) -> bool: ...


class LlmRenderBackend:
    def __init__(self, base_url: Optional[str] = None, timeout: Optional[float] = None) -> None:
        self.base = (base_url or settings.LLM_SERVICE_URL).rstrip("/") + "/api/v1/visualizations"
        self.timeout = timeout or settings.ARTIFACT_LLM_TIMEOUT_SECONDS
        self._version: Optional[str] = None
        self._version_at = 0.0

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self.timeout)

    async def renderer_version(self) -> str:
        """`<template version>/q<quality>` from llm-service, cached briefly; the configured fallback keeps registration
        working while llm-service is down (a wrong guess only costs one re-render after an upgrade)."""
        now = time.monotonic()
        if self._version and now - self._version_at < 60:
            return self._version
        try:
            async with self._client() as c:
                r = await c.get(f"{self.base}/config")
                r.raise_for_status()
                data = r.json()
            base = str(data.get("renderer_version") or settings.ARTIFACT_RENDERER_VERSION)
            version = f"{base}/q{data['quality']}" if data.get("quality") else base
            self._version, self._version_at = version, now
        except Exception as exc:
            logger.warning("renderer version unavailable; using configured value", error_type=type(exc).__name__)
            return self._version or settings.ARTIFACT_RENDERER_VERSION
        return self._version

    async def submit(self, owner, spec):
        try:
            async with self._client() as c:
                r = await c.post(f"{self.base}/manim/jobs", json={"spec": spec}, headers={"X-User-Id": owner})
        except httpx.HTTPError as exc:
            raise RenderUnavailable(type(exc).__name__) from exc
        if r.status_code in (200, 202):
            d = r.json()
            return RenderJob(d["id"], d["state"], d.get("failure"))
        if r.status_code == 429 or r.status_code >= 500:
            raise RenderUnavailable(f"http_{r.status_code}")
        code = "rejected"
        try:
            detail = r.json().get("detail")
            code = (detail.get("failure") if isinstance(detail, dict) else None) or code
        except ValueError:
            pass
        raise RenderRejected(str(code)[:48])

    async def status(self, owner, job_id):
        try:
            async with self._client() as c:
                r = await c.get(f"{self.base}/manim/jobs/{job_id}", headers={"X-User-Id": owner})
        except httpx.HTTPError as exc:
            raise RenderUnavailable(type(exc).__name__) from exc
        if r.status_code == 404:
            return None
        if r.status_code >= 400:
            raise RenderUnavailable(f"http_{r.status_code}")
        d = r.json()
        return RenderJob(d["id"], d["state"], d.get("failure"))

    async def download(self, owner, job_id, dest):
        total = 0
        try:
            async with self._client() as c:
                async with c.stream("GET", f"{self.base}/manim/jobs/{job_id}/media", headers={"X-User-Id": owner}) as r:
                    if r.status_code == 404:
                        raise FileNotFoundError(job_id)
                    if r.status_code >= 400:
                        raise RenderUnavailable(f"http_{r.status_code}")
                    with open(dest, "wb") as out:
                        async for chunk in r.aiter_bytes(256 * 1024):
                            total += len(chunk)
                            if total > settings.ARTIFACT_MAX_VIDEO_BYTES:
                                raise RenderRejected("resource_limit")
                            out.write(chunk)
        except httpx.HTTPError as exc:
            raise RenderUnavailable(type(exc).__name__) from exc
        return total

    async def download_thumbnail(self, owner, job_id, dest):
        """Optional preview PNG. False when none exists or it cannot be fetched; never raises (it is best-effort)."""
        try:
            async with self._client() as c:
                async with c.stream("GET", f"{self.base}/manim/jobs/{job_id}/thumbnail", headers={"X-User-Id": owner}) as r:
                    if r.status_code != 200:
                        return False
                    total = 0
                    with open(dest, "wb") as out:
                        async for chunk in r.aiter_bytes(64 * 1024):
                            total += len(chunk)
                            if total > MAX_THUMBNAIL_BYTES:
                                return False
                            out.write(chunk)
            return True
        except Exception:
            return False
