"""Object storage for rendered videos and export archives (S3-compatible: the existing MinIO; buckets stay private).

The interface is tiny on purpose so tests use `MemoryObjectStore` and production uses `MinioObjectStore`. All access is
authorised by the API layer; nothing here is ever exposed through a public URL.
"""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, Optional, Protocol

CHUNK = 256 * 1024


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    content_type: str = ""
    metadata: dict = field(default_factory=dict)
    last_modified: float = 0.0


class ObjectStore(Protocol):
    async def ensure_ready(self) -> None: ...
    async def put_file(self, key: str, path: Path, content_type: str, metadata: Optional[dict] = None) -> None: ...
    async def stat(self, key: str) -> Optional[ObjectInfo]: ...
    async def get_to_file(self, key: str, path: Path) -> None: ...
    def stream(self, key: str) -> AsyncIterator[bytes]: ...
    async def delete(self, key: str) -> None: ...
    async def list(self, prefix: str) -> list[ObjectInfo]: ...


class MemoryObjectStore:
    """In-process store for tests and local development without MinIO."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str, dict, float]] = {}
        self.puts = 0
        self.put_keys: list[str] = []

    async def ensure_ready(self) -> None:
        return None

    async def put_file(self, key, path, content_type, metadata=None):
        self.puts += 1
        self.put_keys.append(key)
        self.objects[key] = (Path(path).read_bytes(), content_type, dict(metadata or {}), time.time())

    def put_bytes(self, key: str, data: bytes, content_type: str = "video/mp4", metadata: Optional[dict] = None) -> None:
        self.objects[key] = (data, content_type, dict(metadata or {}), time.time())

    def puts_ending(self, suffix: str) -> int:
        return sum(k.endswith(suffix) for k in self.put_keys)

    async def stat(self, key):
        item = self.objects.get(key)
        return ObjectInfo(key, len(item[0]), item[1], item[2], item[3]) if item else None

    async def get_to_file(self, key, path):
        item = self.objects.get(key)
        if item is None:
            raise FileNotFoundError(key)
        Path(path).write_bytes(item[0])

    async def stream(self, key):
        item = self.objects.get(key)
        if item is None:
            raise FileNotFoundError(key)
        for i in range(0, len(item[0]), CHUNK):
            yield item[0][i:i + CHUNK]

    async def delete(self, key):
        self.objects.pop(key, None)

    async def list(self, prefix):
        return [ObjectInfo(k, len(v[0]), v[1], v[2], v[3]) for k, v in sorted(self.objects.items()) if k.startswith(prefix)]


class MinioObjectStore:
    """MinIO / S3 through the official client (synchronous), moved off the event loop with `asyncio.to_thread`."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str, secure: bool = False,
                 region: str = "us-east-1") -> None:
        from minio import Minio                     # imported lazily so unit tests need not have a server
        self.bucket = bucket
        self.region = region
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure, region=region)

    async def ensure_ready(self) -> None:
        """Creates the bucket if missing. It is never made public: no policy is attached."""
        def _ensure() -> None:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket, location=self.region)
        await asyncio.to_thread(_ensure)

    async def put_file(self, key, path, content_type, metadata=None):
        await asyncio.to_thread(self.client.fput_object, self.bucket, key, str(path), content_type=content_type,
                                metadata=metadata or None)

    async def stat(self, key):
        from minio.error import S3Error

        def _stat():
            try:
                st = self.client.stat_object(self.bucket, key)
            except S3Error as exc:
                if exc.code in ("NoSuchKey", "NoSuchObject", "NotFound"):
                    return None
                raise
            meta = {k[len("x-amz-meta-"):].lower(): v for k, v in (st.metadata or {}).items()
                    if k.lower().startswith("x-amz-meta-")}
            return ObjectInfo(key, st.size or 0, st.content_type or "", meta,
                              st.last_modified.timestamp() if st.last_modified else 0.0)
        return await asyncio.to_thread(_stat)

    async def get_to_file(self, key, path):
        await asyncio.to_thread(self.client.fget_object, self.bucket, key, str(path))

    async def stream(self, key):
        response = await asyncio.to_thread(self.client.get_object, self.bucket, key)
        try:
            while True:
                chunk = await asyncio.to_thread(response.read, CHUNK)
                if not chunk:
                    break
                yield chunk
        finally:
            response.close()
            response.release_conn()

    async def delete(self, key):
        await asyncio.to_thread(self.client.remove_object, self.bucket, key)

    async def list(self, prefix):
        def _list():
            return [ObjectInfo(o.object_name, o.size or 0, "", {}, o.last_modified.timestamp() if o.last_modified else 0.0)
                    for o in self.client.list_objects(self.bucket, prefix=prefix, recursive=True)]
        return await asyncio.to_thread(_list)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
