"""
JWT Verification Engine.
Handles signature verification, expiration, issuer, and audience claim checks for incoming JWTs.
"""

from typing import Dict, Any
import jwt
from fastapi import HTTPException, status
from app.core.config import settings
from app.core.logging import logger

def verify_jwt_token(token: str) -> Dict[str, Any]:
    """
    Verifies incoming JWT token signature, expiration, issuer, and audience claims.
    Returns the decoded token payload dictionary.
    """
    options = {
        "verify_signature": True,
        "verify_exp": True,
        "verify_iss": bool(settings.JWT_ISSUER),
        "verify_aud": bool(settings.JWT_AUDIENCE)
    }
    
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
            issuer=settings.JWT_ISSUER,
            audience=settings.JWT_AUDIENCE,
            options=options
        )
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("JWT token signature has expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token has expired",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except jwt.PyJWTError as e:
        logger.warning("JWT token verification failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token",
            headers={"WWW-Authenticate": "Bearer"}
        )
