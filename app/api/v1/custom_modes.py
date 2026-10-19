"""
Custom-mode endpoints (gateway: /api/v1/custom-modes).

The owner is ALWAYS the authenticated user (verified JWT `sub`); the body schemas reject an owner field and the
queries are owner-scoped, so another user's mode id behaves exactly like a missing one (404, no existence leak).
Instructions are never logged.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.api.deps import get_custom_mode_service
from app.models.custom_modes import CustomModeCreate, CustomModeList, CustomModeResponse, CustomModeUpdate
from app.repositories.pg_repositories import (
    CustomModeConflictError, CustomModeLimitError, CustomModeNameTakenError, CustomModeNotFoundError,
)
from app.security.dependencies import get_current_user
from app.security.models import CurrentUser
from app.services.custom_mode_service import CustomModeService

router = APIRouter()


def _error(code: str, message: str, http_status: int, **extra) -> HTTPException:
    return HTTPException(status_code=http_status, detail={"code": code, "message": message, **extra})


@router.post("", response_model=CustomModeResponse, status_code=status.HTTP_201_CREATED, summary="Create a custom mode")
async def create_custom_mode(
    body: CustomModeCreate,
    user: CurrentUser = Depends(get_current_user),
    service: CustomModeService = Depends(get_custom_mode_service),
):
    try:
        return await service.create(user.id, body)
    except CustomModeNameTakenError:
        raise _error("name_taken", "You already have a custom mode with that name.", status.HTTP_409_CONFLICT)
    except CustomModeLimitError as e:
        raise _error("limit_reached", f"You can have at most {e.limit} custom modes. Archive one first.",
                     status.HTTP_409_CONFLICT)


@router.get("", response_model=CustomModeList, summary="List my custom modes")
async def list_custom_modes(
    include_archived: bool = Query(False),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0, le=10_000),
    user: CurrentUser = Depends(get_current_user),
    service: CustomModeService = Depends(get_custom_mode_service),
):
    return await service.list(user.id, include_archived=include_archived, limit=limit, offset=offset)


@router.get("/{mode_id}", response_model=CustomModeResponse, summary="Get one of my custom modes")
async def get_custom_mode(
    mode_id: UUID,
    user: CurrentUser = Depends(get_current_user),
    service: CustomModeService = Depends(get_custom_mode_service),
):
    try:
        return await service.get(user.id, mode_id)
    except CustomModeNotFoundError:
        raise _error("not_found", "Custom mode not found.", status.HTTP_404_NOT_FOUND)


@router.patch("/{mode_id}", response_model=CustomModeResponse, summary="Update a custom mode")
async def update_custom_mode(
    mode_id: UUID,
    body: CustomModeUpdate,
    user: CurrentUser = Depends(get_current_user),
    service: CustomModeService = Depends(get_custom_mode_service),
):
    """Only the fields present change. Send the `version` from your last read to get a 409 instead of overwriting
    an edit made elsewhere. Archived modes cannot be edited."""
    try:
        return await service.update(user.id, mode_id, body)
    except CustomModeNotFoundError:
        raise _error("not_found", "Custom mode not found.", status.HTTP_404_NOT_FOUND)
    except CustomModeConflictError as e:
        raise _error("version_conflict", "This mode was changed elsewhere. Reload and try again.",
                     status.HTTP_409_CONFLICT, current_version=e.current_version)
    except CustomModeNameTakenError:
        raise _error("name_taken", "You already have a custom mode with that name.", status.HTTP_409_CONFLICT)


@router.delete("/{mode_id}", response_model=CustomModeResponse, summary="Archive a custom mode")
async def archive_custom_mode(
    mode_id: UUID,
    user: CurrentUser = Depends(get_current_user),
    service: CustomModeService = Depends(get_custom_mode_service),
):
    """Soft delete (idempotent): the mode stops being selectable and a saved default pointing at it is ignored
    until changed. Requests already dispatched keep running with the version they were sent with."""
    try:
        return await service.archive(user.id, mode_id)
    except CustomModeNotFoundError:
        raise _error("not_found", "Custom mode not found.", status.HTTP_404_NOT_FOUND)
