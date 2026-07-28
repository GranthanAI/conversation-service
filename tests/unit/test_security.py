"""
Security & Authentication Unit Tests.
Verifies JWT verification, CurrentUser claim construction, expiration rules, and fine-grained conversation ownership validation.
"""

from datetime import datetime, timezone, timedelta
import uuid
from unittest.mock import MagicMock, AsyncMock
import jwt
import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.core.config import settings
from app.security import (
    CurrentUser,
    verify_jwt_token,
    get_current_user,
    require_conversation_owner
)
from app.models.conversation import Conversation, ConversationStatus

def make_test_jwt(data: dict, expires_delta: timedelta = None) -> str:
    """
    Test helper to generate JWT tokens for verification tests.
    """
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta if expires_delta is not None else timedelta(hours=24))
    to_encode.update({"iat": now, "exp": expire})
    return jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)

def test_verify_jwt_token_and_current_user():
    user_id = uuid.uuid4()
    token = make_test_jwt({
        "sub": str(user_id),
        "email": "user@example.com",
        "roles": ["user", "admin"],
        "scopes": "read write"
    })
    
    payload = verify_jwt_token(token)
    assert payload["sub"] == str(user_id)
    
    user = CurrentUser.from_jwt_payload(payload)
    assert user.id == user_id
    assert user.email == "user@example.com"
    assert user.roles == ["user", "admin"]
    assert user.scopes == ["read", "write"]

def test_verify_jwt_expired_token():
    user_id = uuid.uuid4()
    token = make_test_jwt({"sub": str(user_id)}, expires_delta=timedelta(seconds=-10))
    
    with pytest.raises(HTTPException) as exc_info:
        verify_jwt_token(token)
    assert exc_info.value.status_code == 401
    assert "expired" in exc_info.value.detail.lower()

def test_verify_jwt_invalid_signature():
    token = "invalid.jwt.token.string"
    with pytest.raises(HTTPException) as exc_info:
        verify_jwt_token(token)
    assert exc_info.value.status_code == 401

def test_current_user_missing_sub_claim():
    payload = {"email": "no_sub@example.com"}
    with pytest.raises(HTTPException) as exc_info:
        CurrentUser.from_jwt_payload(payload)
    assert exc_info.value.status_code == 401

def test_current_user_malformed_uuid():
    payload = {"sub": "not-a-valid-uuid"}
    with pytest.raises(HTTPException) as exc_info:
        CurrentUser.from_jwt_payload(payload)
    assert exc_info.value.status_code == 401

@pytest.mark.anyio
async def test_get_current_user_dependency():
    user_id = uuid.uuid4()
    token = make_test_jwt({"sub": str(user_id), "email": "test@example.com"})
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
    
    user = await get_current_user(credentials=creds)
    assert isinstance(user, CurrentUser)
    assert user.id == user_id
    assert user.email == "test@example.com"

@pytest.mark.anyio
async def test_require_conversation_owner_success():
    conv_id = uuid.uuid4()
    user_id = uuid.uuid4()
    user = CurrentUser(id=user_id)
    
    mock_service = MagicMock()
    mock_service.get = AsyncMock(return_value=Conversation(
        conversation_id=conv_id,
        user_id=user_id,
        title="My Conversation",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    ))
    
    conv = await require_conversation_owner(
        conversation_id=conv_id,
        current_user=user,
        service=mock_service
    )
    assert conv.conversation_id == conv_id
    assert conv.user_id == user_id

@pytest.mark.anyio
async def test_require_conversation_owner_forbidden():
    conv_id = uuid.uuid4()
    owner_user_id = uuid.uuid4()
    other_user_id = uuid.uuid4()
    user = CurrentUser(id=other_user_id)
    
    mock_service = MagicMock()
    mock_service.get = AsyncMock(return_value=Conversation(
        conversation_id=conv_id,
        user_id=owner_user_id,
        title="Owner Conversation",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
        status=ConversationStatus.ACTIVE
    ))
    
    with pytest.raises(HTTPException) as exc_info:
        await require_conversation_owner(
            conversation_id=conv_id,
            current_user=user,
            service=mock_service
        )
    assert exc_info.value.status_code == 403

@pytest.mark.anyio
async def test_require_conversation_owner_not_found():
    conv_id = uuid.uuid4()
    user = CurrentUser(id=uuid.uuid4())
    
    mock_service = MagicMock()
    mock_service.get = AsyncMock(return_value=None)
    
    with pytest.raises(HTTPException) as exc_info:
        await require_conversation_owner(
            conversation_id=conv_id,
            current_user=user,
            service=mock_service
        )
    assert exc_info.value.status_code == 404
