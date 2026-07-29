"""
FastAPI Security Dependencies.
Provides Bearer authentication token resolution and fine-grained resource ownership validation.
"""

from uuid import UUID
from typing import Optional
from fastapi import Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.security.models import CurrentUser
from app.security.jwt import verify_jwt_token
from app.models.conversation import Conversation
from app.api.deps import get_conversation_service
from app.services.conversation_service import ConversationService

oauth2_scheme = HTTPBearer(auto_error=False)

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(oauth2_scheme),
    token: Optional[str] = Query(None)
) -> CurrentUser:
    """
    FastAPI dependency reading the Bearer JWT token, verifying claims, and returning a strongly typed CurrentUser.
    Supports query parameter tokens for SSE streams.
    """
    resolved_token = None
    if credentials:
        resolved_token = credentials.credentials
    elif token:
        resolved_token = token
        
    if not resolved_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )
    payload = verify_jwt_token(resolved_token)
    return CurrentUser.from_jwt_payload(payload)

async def require_conversation_owner(
    conversation_id: UUID,
    current_user: CurrentUser = Depends(get_current_user),
    service: ConversationService = Depends(get_conversation_service)
) -> Conversation:
    """
    FastAPI dependency verifying conversation existence (404) and matching user ownership (403).
    """
    conv = await service.get(conversation_id)
    if not conv:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )
    if conv.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden: You do not own this conversation"
        )
    return conv
