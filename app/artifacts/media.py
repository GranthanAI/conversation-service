"""Verification of a rendered MP4 before it is trusted (marked ready, stored, or served)."""

import hashlib
import struct
from dataclasses import dataclass
from pathlib import Path

MIN_MP4_BYTES = 1024
_CHUNK = 1024 * 1024


class InvalidMedia(ValueError):
    """The file is not a plausible, complete MP4 within the size limits."""


@dataclass(frozen=True)
class MediaInfo:
    size: int
    sha256: str


def _walk_boxes(f, size: int) -> set[bytes]:
    """Walk the top-level ISO-BMFF boxes. A truncated or garbage file fails because box sizes must tile the file exactly."""
    seen: list[bytes] = []
    pos = 0
    while pos < size:
        f.seek(pos)
        header = f.read(8)
        if len(header) < 8:
            raise InvalidMedia("truncated box header")
        box_size, box_type = struct.unpack(">I4s", header)
        if box_size == 1:
            ext = f.read(8)
            if len(ext) < 8:
                raise InvalidMedia("truncated box header")
            box_size = struct.unpack(">Q", ext)[0]
            min_size = 16
        elif box_size == 0:
            box_size, min_size = size - pos, 8
        else:
            min_size = 8
        if box_size < min_size or pos + box_size > size:
            raise InvalidMedia("box exceeds file bounds")
        seen.append(box_type)
        pos += box_size
    if not seen or seen[0] != b"ftyp":
        raise InvalidMedia("not an MP4 (missing ftyp)")
    return set(seen)


def validate_mp4(path: Path, max_bytes: int) -> MediaInfo:
    """Size bounds, ISO-BMFF structure (ftyp first, moov and mdat present, boxes tile the file) and the sha256 of the
    content. Raises InvalidMedia."""
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise InvalidMedia("unreadable") from exc
    if size < MIN_MP4_BYTES:
        raise InvalidMedia("too small")
    if size > max_bytes:
        raise InvalidMedia("too large")
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        boxes = _walk_boxes(f, size)
        if b"moov" not in boxes or b"mdat" not in boxes:
            raise InvalidMedia("missing moov/mdat")
        f.seek(0)
        for chunk in iter(lambda: f.read(_CHUNK), b""):
            digest.update(chunk)
    return MediaInfo(size=size, sha256=digest.hexdigest())
