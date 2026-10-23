"""Filesystem-safe names for export archives. Titles are user content: they never reach a path unsanitised, and every
archive member is validated again before it is written (defence in depth against traversal)."""

import re
import unicodedata
from uuid import UUID

_UNSAFE = re.compile(r"[^a-z0-9._-]+")
_WINDOWS_RESERVED = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}


def slug(text: str, max_len: int = 40, fallback: str = "conversation") -> str:
    ascii_text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii").lower()
    cleaned = _UNSAFE.sub("-", ascii_text).strip("-._")[:max_len].strip("-._")
    if not cleaned or cleaned in _WINDOWS_RESERVED:
        return fallback
    return cleaned


def short_id(value: UUID) -> str:
    return str(value).replace("-", "")[:8]


def conversation_dir(title: str, conversation_id: UUID) -> str:
    """`<slug>-<first 8 hex of the id>`: readable, and unique because it embeds the stable conversation id."""
    return f"{slug(title)}-{short_id(conversation_id)}"


class UnsafeArchivePath(ValueError):
    pass


def validate_archive_path(path: str) -> str:
    if not path or path.startswith("/") or "\\" in path or "\x00" in path or ":" in path:
        raise UnsafeArchivePath(path[:60])
    parts = path.split("/")
    for part in parts:
        if part in ("", ".", "..") or len(part) > 120 or any(ord(c) < 32 for c in part):
            raise UnsafeArchivePath(path[:60])
    return path
