"""
Pagination Unit Tests.
Verifies Base64 cursor encoding, decoding, invalid payload handling, and PageResponse data container logic.
"""

import uuid
from datetime import datetime, timezone
import pytest
from fastapi import HTTPException
from app.utils.pagination import encode_cursor, decode_cursor, PageResponse

def test_encode_and_decode_cursor():
    conv_id = uuid.uuid4()
    now_iso = datetime.now(timezone.utc).isoformat()
    
    payload = {"updated_at": now_iso, "conversation_id": str(conv_id)}
    cursor_str = encode_cursor(payload)
    
    assert isinstance(cursor_str, str)
    assert len(cursor_str) > 0
    
    decoded = decode_cursor(cursor_str)
    assert decoded["updated_at"] == now_iso
    assert decoded["conversation_id"] == str(conv_id)

def test_decode_invalid_cursor():
    invalid_cursor = "not-a-valid-base64-json-string"
    with pytest.raises(HTTPException) as exc_info:
        decode_cursor(invalid_cursor)
    assert exc_info.value.status_code == 400
    assert "invalid pagination cursor format" in exc_info.value.detail.lower()

def test_page_response_container():
    items = ["item1", "item2"]
    page = PageResponse[str](items=items, next_cursor="opaque_cursor", has_more=True)
    
    assert page.items == items
    assert page.next_cursor == "opaque_cursor"
    assert page.has_more is True
