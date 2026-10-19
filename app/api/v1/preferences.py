"""
User preferences endpoints (gateway: /api/v1/preferences). Backed by PostgreSQL.

The authenticated user (verified JWT `sub`) is the only identity used: there is no user_id in the path or body, so
one user can never read or change another's settings.
"""

from fastapi import APIRouter, Depends, HTTPException, status

from app.api.deps import get_preferences_service
from app.models.preferences import PreferencesResponse, PreferencesUpdate
from app.repositories.pg_repositories import InvalidDefaultModeError, PreferencesConflictError
from app.security.dependencies import get_current_user
from app.security.models import CurrentUser
from app.services.preferences_service import PreferencesService, UnknownCatalogItemError

router = APIRouter()


@router.get("", response_model=PreferencesResponse, summary="Get my preferences (defaults filled in)")
async def get_preferences(
    current_user: CurrentUser = Depends(get_current_user),
    service: PreferencesService = Depends(get_preferences_service),
):
    return await service.get(current_user.id)


@router.patch("", response_model=PreferencesResponse, summary="Update some of my preferences")
async def update_preferences(
    patch: PreferencesUpdate,
    current_user: CurrentUser = Depends(get_current_user),
    service: PreferencesService = Depends(get_preferences_service),
):
    """Only the fields present are changed. Send the `version` from your last read to reject the write (409) if the
    settings were changed elsewhere in the meantime."""
    try:
        return await service.update(current_user.id, patch)
    except PreferencesConflictError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Your settings were changed elsewhere. Reload and try again.",
                    "current_version": e.current_version},
        )
    except (UnknownCatalogItemError, InvalidDefaultModeError) as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail={"code": "invalid_preference", "message": str(e)})


@router.delete("", response_model=PreferencesResponse, summary="Restore default preferences")
async def reset_preferences(
    current_user: CurrentUser = Depends(get_current_user),
    service: PreferencesService = Depends(get_preferences_service),
):
    return await service.reset(current_user.id)
