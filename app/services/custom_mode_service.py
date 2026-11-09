"""Custom-mode lifecycle (create / list / get / update / archive) and resolution for a message. PostgreSQL only."""

import uuid
from typing import Optional

from app.core.config import settings
from app.core.logging import logger
from app.models.custom_modes import (
    CustomModeCreate, CustomModeList, CustomModeResponse, CustomModeSnapshot, CustomModeUpdate, ModeStatus,
)
from app.repositories.pg_repositories import CustomModeLimitError, CustomModeNotFoundError, CustomModeRepository


class CustomModeUnavailableError(Exception):
    """An EXPLICITLY chosen custom mode does not exist, is not the caller's, or is archived."""


class CustomModeService:
    def __init__(self, repo: CustomModeRepository, plan_limits=None):
        self.repo = repo
        self.plan_limits = plan_limits      # PlanLimitService; None = no plan limits

    async def create(self, owner_id: uuid.UUID, body: CustomModeCreate) -> CustomModeResponse:
        max_per_user, plan_ent = settings.CUSTOM_MODE_MAX_PER_USER, None
        if self.plan_limits is not None:
            await self.plan_limits.check_instruction_chars(owner_id, body.instructions)
            plan_cap, ent = await self.plan_limits.custom_mode_limit(owner_id)
            if plan_cap is not None and plan_cap <= max_per_user:
                max_per_user, plan_ent = plan_cap, ent
        kwargs = dict(name=body.name, description=body.description, instructions=body.instructions,
                      execution_style=body.execution_style.value)
        try:
            record = await self.repo.create(owner_id, max_per_user=max_per_user, **kwargs)
        except CustomModeLimitError:
            if plan_ent is None:
                raise
            self.plan_limits.refuse_custom_modes(plan_ent)       # enforce: 403 plan_limit; shadow: logged, then proceed
            record = await self.repo.create(owner_id, max_per_user=settings.CUSTOM_MODE_MAX_PER_USER, **kwargs)
        logger.info("custom_mode_created", status="ok")
        return CustomModeResponse.from_record(record)

    async def list(self, owner_id: uuid.UUID, *, include_archived: bool, limit: int, offset: int) -> CustomModeList:
        rows, total = await self.repo.list(owner_id, include_archived=include_archived, limit=limit, offset=offset)
        return CustomModeList(items=[CustomModeResponse.from_record(r) for r in rows], total=total, limit=limit,
                              offset=offset, max_per_user=settings.CUSTOM_MODE_MAX_PER_USER,
                              limits={"name": settings.CUSTOM_MODE_NAME_MAX_CHARS,
                                      "description": settings.CUSTOM_MODE_DESCRIPTION_MAX_CHARS,
                                      "instructions": settings.CUSTOM_MODE_INSTRUCTIONS_MAX_CHARS})

    async def get(self, owner_id: uuid.UUID, mode_id: uuid.UUID) -> CustomModeResponse:
        record = await self.repo.get(owner_id, mode_id)
        if record is None:
            raise CustomModeNotFoundError(str(mode_id))
        return CustomModeResponse.from_record(record)

    async def update(self, owner_id: uuid.UUID, mode_id: uuid.UUID, body: CustomModeUpdate) -> CustomModeResponse:
        changes = body.changes()
        if not changes:
            return await self.get(owner_id, mode_id)
        if self.plan_limits is not None and "instructions" in changes:
            await self.plan_limits.check_instruction_chars(owner_id, changes["instructions"])
        record = await self.repo.update(owner_id, mode_id, changes, body.version)
        return CustomModeResponse.from_record(record)

    async def archive(self, owner_id: uuid.UUID, mode_id: uuid.UUID) -> CustomModeResponse:
        return CustomModeResponse.from_record(await self.repo.archive(owner_id, mode_id))

    async def snapshot_for_message(self, owner_id: uuid.UUID, mode_id: uuid.UUID) -> CustomModeSnapshot:
        """The trusted, immutable definition used to run one request. Owner-scoped and active only: anything
        else is an unavailable mode (never silently replaced by a different one)."""
        record = await self.repo.get(owner_id, mode_id, include_archived=False)
        if record is None or record.status != ModeStatus.ACTIVE.value:
            raise CustomModeUnavailableError(str(mode_id))
        return CustomModeSnapshot.from_record(record)
