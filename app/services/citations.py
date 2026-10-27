"""
Answer-source citations stored with a final assistant message.

llm-service sends them on `chat.response.generated`; they are persisted as deterministic JSON in
`messages_by_conversation.citations` so history can rebuild the sources after a reload. Only references
(File Service id, display name, page/section, video time range) are stored: no excerpts and no URLs. A video
link is built by the client from the validated video id of the referenced source, which File Service authorizes.
"""

import json
import re
from typing import Any, List, Optional

MAX_CITATIONS = 50
MAX_FIELD_CHARS = 300
MAX_JSON_BYTES = 32_000
MAX_SECONDS = 9_999_999  # matches the 7-digit `t=<start>-<end>` section encoding

_FILE_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _text(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    return value.strip()[:MAX_FIELD_CHARS] or None


def _int(value: Any, upper: int) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= upper else None


def _clean_one(raw: Any) -> Optional[dict]:
    """One whitelisted citation, or None if it cannot identify a source."""
    if not isinstance(raw, dict):
        return None
    file_id = raw.get("file_id")
    if not isinstance(file_id, str) or not _FILE_ID.match(file_id):
        return None
    out: dict = {
        "file_id": file_id,
        "source_name": _text(raw.get("source_name")) or "",
        "page_number": _int(raw.get("page_number"), 100_000),
        "section": _text(raw.get("section")),
    }
    start = _int(raw.get("start_seconds"), MAX_SECONDS)
    if start is not None:
        end = _int(raw.get("end_seconds"), MAX_SECONDS)
        out["start_seconds"] = start
        out["end_seconds"] = end if end is not None and end >= start else None
    return out


def sanitize_citations(raw: Any) -> List[dict]:
    """Whitelist, bound and de-duplicate citations from an event. Invalid entries are dropped, never repaired,
    so an event can't make a message carry a source it did not retrieve. Order is preserved (reference numbering)."""
    if not isinstance(raw, list):
        return []
    cleaned: List[dict] = []
    seen = set()
    for item in raw:
        c = _clean_one(item)
        if c is None:
            continue
        key = json.dumps(c, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(c)
        if len(cleaned) >= MAX_CITATIONS:
            break
    while cleaned and len(dumps_citations(cleaned).encode("utf-8")) > MAX_JSON_BYTES:
        cleaned.pop()
    return cleaned


def dumps_citations(citations: List[dict]) -> str:
    return json.dumps(citations, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def loads_citations(stored: Optional[str]) -> List[dict]:
    """Stored column -> citations. Null, empty or corrupt values (e.g. historical messages) are an empty list;
    the stored value is re-sanitized so a bad row can never break history."""
    if not stored:
        return []
    try:
        return sanitize_citations(json.loads(stored))
    except (ValueError, TypeError):
        return []
