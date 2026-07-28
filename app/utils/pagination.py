"""
Opaque Cursor Pagination Utility.
Encodes and decodes URL-safe Base64 cursors to encapsulate Cassandra clustering keys without leaking database schema internals.
"""

import base64
import json
from typing import Dict, Any, List, Optional, Generic, TypeVar
from pydantic import BaseModel
from fastapi import HTTPException, status
from app.core.logging import logger

T = TypeVar("T")

def encode_cursor(payload: Dict[str, Any]) -> str:
    """
    Encodes a dictionary of clustering key attributes into a URL-safe Base64 cursor string.
    """
    json_bytes = json.dumps(payload).encode("utf-8")
    return base64.urlsafe_b64encode(json_bytes).decode("utf-8")

def decode_cursor(cursor_str: str) -> Dict[str, Any]:
    """
    Decodes a URL-safe Base64 cursor string back into a claims/attributes dictionary.
    Raises HTTPException(400) if the cursor string is malformed or corrupted.
    """
    try:
        json_bytes = base64.urlsafe_b64decode(cursor_str.encode("utf-8"))
        payload = json.loads(json_bytes.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Cursor payload must be a JSON object.")
        return payload
    except Exception as e:
        logger.warning("Failed to decode pagination cursor", cursor=cursor_str, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid pagination cursor format"
        )

class PageResponse(BaseModel, Generic[T]):
    """
    Generic paginated container for infinite scrolling API feeds.
    """
    items: List[T]
    next_cursor: Optional[str] = None
    has_more: bool = False
