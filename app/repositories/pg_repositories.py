"""
PostgreSQL repositories for user preferences and custom modes (the only authoritative store for both).

Repositories only `flush`; the request-scoped session (app/db/session.py::get_db) commits or rolls back.
Every write is a single atomic statement (compare-and-set on `version`), so concurrent edits can never silently
overwrite each other and no read-then-write race exists.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.custom_modes import ModeStatus, custom_ref, normalize_name
from app.models.sql_models import CustomModeRecord, UserPreferenceRecord

_PREF_FIELDS = ("answer_length", "tone", "language", "personal_instructions", "citations", "document_context")


class PreferencesConflictError(Exception):
    """The stored version is not the one the client edited (someone saved in between)."""

    def __init__(self, current_version: int):
        self.current_version = current_version
        super().__init__(f"Preferences were changed elsewhere (current version {current_version}).")


class CustomModeNotFoundError(LookupError):
    pass


class CustomModeConflictError(Exception):
    def __init__(self, current_version: int):
        self.current_version = current_version
        super().__init__(f"Custom mode was changed elsewhere (current version {current_version}).")


class CustomModeNameTakenError(Exception):
    pass


class CustomModeLimitError(Exception):
    def __init__(self, limit: int):
        self.limit = limit
        super().__init__(f"At most {limit} custom modes are allowed.")


class InvalidDefaultModeError(ValueError):
    """The default custom mode does not exist or belongs to someone else."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


class PreferencesRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def _to_dict(row: UserPreferenceRecord) -> dict:
        data = {f: getattr(row, f) for f in _PREF_FIELDS}
        data["default_mode"] = custom_ref(row.default_custom_mode_id) if row.default_custom_mode_id else row.default_mode
        data["default_tools"] = list(row.default_tools or [])
        data["version"] = row.version
        data["updated_at"] = row.updated_at
        return data

    async def get(self, user_id: uuid.UUID) -> Optional[dict]:
        row = (await self.session.execute(
            select(UserPreferenceRecord).where(UserPreferenceRecord.user_id == user_id))).scalar_one_or_none()
        return self._to_dict(row) if row else None

    async def save(self, user_id: uuid.UUID, values: dict, expected_version: int) -> int:
        """Writes the full document if the stored version equals `expected_version` (0 = no row yet).
        Returns the new version; raises PreferencesConflictError otherwise."""
        custom_id = values.get("_default_custom_mode_id")
        columns = {f: values[f] for f in _PREF_FIELDS}
        columns.update(
            default_mode=None if custom_id else values.get("default_mode"),
            default_custom_mode_id=custom_id,
            default_tools=list(values.get("default_tools") or []),
            updated_at=_now(),
        )
        try:
            if expected_version == 0:
                stmt = (pg_insert(UserPreferenceRecord)
                        .values(user_id=user_id, version=1, **columns)
                        .on_conflict_do_nothing(index_elements=["user_id"])
                        .returning(UserPreferenceRecord.version))
            else:
                stmt = (update(UserPreferenceRecord)
                        .where(UserPreferenceRecord.user_id == user_id, UserPreferenceRecord.version == expected_version)
                        .values(version=expected_version + 1, **columns)
                        .returning(UserPreferenceRecord.version))
            new_version = (await self.session.execute(stmt)).scalar_one_or_none()
        except IntegrityError as exc:                       # FK / check violation
            raise InvalidDefaultModeError("Invalid default mode.") from exc
        if new_version is None:
            current = (await self.session.execute(
                select(UserPreferenceRecord.version).where(UserPreferenceRecord.user_id == user_id))).scalar_one_or_none()
            raise PreferencesConflictError(current or 0)
        return new_version

    async def delete(self, user_id: uuid.UUID) -> None:
        await self.session.execute(text("DELETE FROM user_preferences WHERE user_id = :u"), {"u": user_id})


class CustomModeRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, owner_id: uuid.UUID, *, name: str, description: str, instructions: str,
                     execution_style: str, max_per_user: int) -> CustomModeRecord:
        # Serialize concurrent creates for the same owner so the per-user limit holds under races
        # (the lock is released at the end of the surrounding transaction).
        await self.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
                                   {"k": f"custom_modes:{owner_id}"})
        active = (await self.session.execute(
            select(func.count()).select_from(CustomModeRecord).where(
                CustomModeRecord.owner_user_id == owner_id, CustomModeRecord.status == ModeStatus.ACTIVE.value)
        )).scalar_one()
        if active >= max_per_user:
            raise CustomModeLimitError(max_per_user)
        record = CustomModeRecord(
            mode_id=uuid.uuid4(), owner_user_id=owner_id, name=name, name_normalized=normalize_name(name),
            description=description, instructions=instructions, execution_style=execution_style)
        self.session.add(record)
        try:
            await self.session.flush()
        except IntegrityError as exc:
            raise CustomModeNameTakenError(name) from exc
        await self.session.refresh(record)
        return record

    async def get(self, owner_id: uuid.UUID, mode_id: uuid.UUID, *, include_archived: bool = True) -> Optional[CustomModeRecord]:
        """Owner-scoped: another user's id behaves exactly like a missing one."""
        stmt = select(CustomModeRecord).where(CustomModeRecord.mode_id == mode_id,
                                              CustomModeRecord.owner_user_id == owner_id)
        if not include_archived:
            stmt = stmt.where(CustomModeRecord.status == ModeStatus.ACTIVE.value)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list(self, owner_id: uuid.UUID, *, include_archived: bool, limit: int, offset: int):
        base = select(CustomModeRecord).where(CustomModeRecord.owner_user_id == owner_id)
        if not include_archived:
            base = base.where(CustomModeRecord.status == ModeStatus.ACTIVE.value)
        total = (await self.session.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
        rows = (await self.session.execute(
            base.order_by(CustomModeRecord.updated_at.desc(), CustomModeRecord.mode_id).limit(limit).offset(offset)
        )).scalars().all()
        return list(rows), total

    async def update(self, owner_id: uuid.UUID, mode_id: uuid.UUID, changes: dict,
                     expected_version: Optional[int]) -> CustomModeRecord:
        values = dict(changes)
        if "name" in values:
            values["name_normalized"] = normalize_name(values["name"])
        if "execution_style" in values:
            values["execution_style"] = getattr(values["execution_style"], "value", values["execution_style"])
        conditions = [CustomModeRecord.mode_id == mode_id, CustomModeRecord.owner_user_id == owner_id,
                      CustomModeRecord.status == ModeStatus.ACTIVE.value]
        if expected_version is not None:
            conditions.append(CustomModeRecord.version == expected_version)
        stmt = (update(CustomModeRecord).where(*conditions)
                .values(version=CustomModeRecord.version + 1, updated_at=_now(), **values)
                .returning(CustomModeRecord))
        try:
            row = (await self.session.execute(stmt)).scalar_one_or_none()
        except IntegrityError as exc:
            raise CustomModeNameTakenError(values.get("name", "")) from exc
        if row is None:
            current = await self.get(owner_id, mode_id, include_archived=False)
            if current is None:
                raise CustomModeNotFoundError(str(mode_id))
            raise CustomModeConflictError(current.version)
        return row

    async def archive(self, owner_id: uuid.UUID, mode_id: uuid.UUID) -> CustomModeRecord:
        """Soft delete (idempotent). Saved defaults / historical requests may still refer to it."""
        existing = await self.get(owner_id, mode_id)
        if existing is None:
            raise CustomModeNotFoundError(str(mode_id))
        if existing.status == ModeStatus.ARCHIVED.value:
            return existing
        stmt = (update(CustomModeRecord)
                .where(CustomModeRecord.mode_id == mode_id, CustomModeRecord.owner_user_id == owner_id,
                       CustomModeRecord.status == ModeStatus.ACTIVE.value)
                .values(status=ModeStatus.ARCHIVED.value, archived_at=_now(), updated_at=_now(),
                        version=CustomModeRecord.version + 1)
                .returning(CustomModeRecord))
        row = (await self.session.execute(stmt)).scalar_one_or_none()
        return row or (await self.get(owner_id, mode_id))
