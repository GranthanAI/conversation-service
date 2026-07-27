"""
Security Domain Models.
Strongly typed model representing the authenticated user extracted from JWT claims.
"""

from typing import List, Optional, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field
from fastapi import HTTPException, status
from app.core.logging import logger

class CurrentUser(BaseModel):
    """
    Strongly typed representation of an authenticated user constructed directly from verified JWT claims.
    """
    id: UUID = Field(..., description="Unique UUID identifier of the authenticated user")
    email: Optional[str] = Field(None, description="User email address if present in claims")
    roles: List[str] = Field(default_factory=list, description="Assigned authorization roles")
    scopes: List[str] = Field(default_factory=list, description="Granted permission scopes")

    @classmethod
    def from_jwt_payload(cls, payload: Dict[str, Any]) -> "CurrentUser":
        """
        Constructs a CurrentUser instance from a decoded JWT claims dictionary.
        Raises HTTPException(401) if identity claims are missing or malformed.
        """
        raw_user_id = payload.get("sub") or payload.get("user_id")
        if not raw_user_id:
            logger.warning("JWT claims missing required 'sub' or 'user_id' identifier")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid user identity in token claims",
                headers={"WWW-Authenticate": "Bearer"}
            )

        try:
            user_id = UUID(str(raw_user_id))
        except ValueError:
            logger.warning("JWT claims contain malformed user UUID", raw_user_id=raw_user_id)
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Malformed user UUID in token claims",
                headers={"WWW-Authenticate": "Bearer"}
            )

        email = payload.get("email")
        roles = payload.get("roles") or []
        scopes = payload.get("scopes") or []

        if isinstance(roles, str):
            roles = [roles]
        if isinstance(scopes, str):
            scopes = scopes.split()

        return cls(
            id=user_id,
            email=email,
            roles=list(roles),
            scopes=list(scopes)
        )
