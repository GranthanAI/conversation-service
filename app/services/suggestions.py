"""Follow-up questions stored with an assistant answer (chat.response.suggestions, schema 1.0).

Stored as a JSON list in messages_by_conversation.suggestions. Validation is the contract: 1..3 strings, each at most
120 characters after strip, no newlines; empty items and duplicates are dropped.
"""

import json
from typing import Any, List, Optional

MAX_SUGGESTIONS = 3
MAX_SUGGESTION_CHARS = 120


def sanitize_suggestions(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return []
    out: List[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        text = item.strip()
        if not text or len(text) > MAX_SUGGESTION_CHARS or "\n" in text or "\r" in text:
            continue
        if text in out:
            continue
        out.append(text)
        if len(out) >= MAX_SUGGESTIONS:
            break
    return out


def dumps_suggestions(suggestions: List[str]) -> str:
    return json.dumps(sanitize_suggestions(suggestions), ensure_ascii=False, separators=(",", ":"))


def loads_suggestions(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    try:
        return sanitize_suggestions(json.loads(raw))
    except (TypeError, ValueError):
        return []
