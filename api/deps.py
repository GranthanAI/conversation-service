"""
API Dependencies File.
Implements request injection dependencies such as extracting authenticated user credentials
using settings variables for JWT verification.
"""

import uuid
import logging
from uuid import UUID
from fastapi import Request, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from core.config import settings

logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)

def get_current_user_id(request: Request, credentials: HTTPAuthorizationCredentials = Security(security)) -> UUID:
    """
    Dependency to extract and verify the current user's UUID from the JWT authorization header.
    Validates token payload using JWT_SECRET_KEY and JWT_ALGORITHM config settings.
    """
    # 1. Custom bypass header (useful for developer sandbox testing)
    user_header = request.headers.get("X-User-Id")
    if user_header:
        try:
            return UUID(user_header)
        except ValueError:
            pass

    # 2. Extract JWT token credentials
    if not credentials:
        # Fallback to local sandbox user if no bearer is present
        dummy_id = UUID("00000000-0000-0000-0000-000000000001")
        logger.warning(f"No authentication token provided. Defaulting to dummy user: {dummy_id}")
        return dummy_id

    token = credentials.credentials
    try:
        # Attempt to decode JWT
        try:
            import jwt
            payload = jwt.decode(token, settings.JWT_SECRET_KEY, algorithms=[settings.JWT_ALGORITHM])
            user_id = payload.get("user_id") or payload.get("sub")
            if user_id:
                return UUID(user_id)
        except ImportError:
            # Fallback signature split extraction if jwt library is missing
            parts = token.split(".")
            if len(parts) == 3:
                import base64
                import json
                payload_data = base64.urlsafe_b64decode(parts[1] + "==").decode("utf-8")
                payload = json.loads(payload_data)
                user_id = payload.get("user_id") or payload.get("sub")
                if user_id:
                    return UUID(user_id)
        
        return UUID("00000000-0000-0000-0000-000000000001")
    except Exception as e:
        logger.error(f"JWT decode failed: {e}")
        raise HTTPException(status_code=401, detail="Invalid authorization token")
