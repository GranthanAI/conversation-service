"""
FastAPI Security Dependencies.
Provides Bearer authentication token resolution and fine-grained resource ownership validation.
"""

from uuid import UUID
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.security.models import CurrentUser
from app.security.jwt import verify_jwt_token
from app.models.conversation import Conversation
from app.api.deps import get_conversation_service
from app.services.conversation_service import ConversationService

oauth2_scheme = HTTPBearer(auto_error=True)

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme)
) -> CurrentUser:
    """
    FastAPI dependency reading the Bearer JWT token, verifying claims, and returning a strongly typed CurrentUser.
    """
    token = credentials.credentials
    payload = verify_jwt_token(token)
    return CurrentUser.from_jwt_payload(payload)

async def require_conversation_owner(
    conversation_id: UUID,
    current_user: CurrentUser = Depends(get_current_user),
    service: ConversationService = Depends(get_conversation_service)
) -> Conversation:
    """
    FastAPI dependency verifying conversation existence (404) and matching user ownership (403).
    """
    conv = service.repo.get(conversation_id)
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
