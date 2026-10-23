"""Test doubles for the artifact pipeline: a scriptable render backend (no llm-service, no Manim, no LLM provider) and a
helper that builds a structurally valid MP4."""

import hashlib
import json
import struct
from pathlib import Path
from typing import Optional

from app.artifacts.render_backend import RenderJob, RenderRejected, RenderUnavailable


def box(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I4s", 8 + len(payload), kind) + payload


def make_mp4(extra: int = 4000, marker: bytes = b"\x01") -> bytes:
    return box(b"ftyp", b"isom\x00\x00\x02\x00isomiso2mp41") + box(b"moov", b"\x00" * 200) + box(b"mdat", marker * extra)


PNG = bytes([0x89]) + b"PNG" + bytes([0x0D, 0x0A, 0x1A, 0x0A]) + b"0" * 64


class FakeBackend:
    """Behaves like llm-service's job API: job ids are derived from (owner, spec), so identical submissions share a job."""

    def __init__(self, version: str = "manim-templates-1") -> None:
        self.version = version
        self.submits: list[tuple[str, dict]] = []
        self.states: dict[str, str] = {}
        self.failures: dict[str, Optional[str]] = {}
        self.reject_with: Optional[str] = None
        self.unavailable = False
        self.forget_jobs = False
        self.media = make_mp4()
        self.downloads = 0
        self.auto_ready = True
        self.thumbnail: Optional[bytes] = PNG

    async def renderer_version(self) -> str:
        return self.version

    @staticmethod
    def job_id(owner: str, spec: dict) -> str:
        return "mj_" + hashlib.sha256(f"{owner}\x00{json.dumps(spec, sort_keys=True)}".encode()).hexdigest()[:32]

    async def submit(self, owner, spec):
        if self.unavailable:
            raise RenderUnavailable("down")
        if self.reject_with:
            raise RenderRejected(self.reject_with)
        self.submits.append((owner, spec))
        jid = self.job_id(owner, spec)
        self.states.setdefault(jid, "ready" if self.auto_ready else "rendering")
        return RenderJob(jid, self.states[jid])

    async def status(self, owner, job_id):
        if self.unavailable:
            raise RenderUnavailable("down")
        if self.forget_jobs or job_id not in self.states:
            return None
        return RenderJob(job_id, self.states[job_id], self.failures.get(job_id))

    async def download(self, owner, job_id, dest: Path):
        if self.unavailable:
            raise RenderUnavailable("down")
        if job_id not in self.states:
            raise FileNotFoundError(job_id)
        self.downloads += 1
        Path(dest).write_bytes(self.media)
        return len(self.media)

    async def download_thumbnail(self, owner, job_id, dest: Path):
        if self.thumbnail is None or job_id not in self.states:
            return False
        Path(dest).write_bytes(self.thumbnail)
        return True
