"""
User preferences (PostgreSQL): read / partial update / reset, and resolution for a new message.

Resolution rules for a message (documented in docs/MODES_AND_PREFERENCES.md):
  * mode:   the message's own `mode` wins (including "auto"); if the message sends no mode, the saved
            `default_mode` is used. `custom:<uuid>` modes are resolved against PostgreSQL, owner-scoped.
  * tools:  the message's own `selected_tools` wins (including an explicit empty list); if none is sent, the saved
            `default_tools` are used.
  * Values that came from saved defaults are flagged in `defaults_applied`, so llm-service skips a default that is
    no longer available (including an archived custom mode) instead of rejecting the message. An EXPLICIT choice
    that cannot run is rejected, never replaced by another mode.
  * Generation preferences are taken from the saved document at send time.
  * PostgreSQL unavailable: reads for message resolution degrade to the built-in defaults (the message still
    goes out, logged); the settings API itself returns a controlled error and never pretends a save succeeded.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from uuid import UUID

from app.core.logging import logger
from app.models.custom_modes import CUSTOM_PREFIX, CustomModeSnapshot, custom_ref, is_custom_ref, parse_custom_ref
from app.models.preferences import (
    PreferencesResponse, PreferencesUpdate, UserPreferences, generation_preferences, preference_options,
)
from app.repositories.pg_repositories import (
    CustomModeRepository, InvalidDefaultModeError, PreferencesConflictError, PreferencesRepository,
)
from app.services.custom_mode_service import CustomModeUnavailableError
from app.services.mode_catalog import ModeCatalog


class UnknownCatalogItemError(ValueError):
    """A built-in mode or tool id that llm-service does not offer."""


@dataclass
class ResolvedMessagePreferences:
    mode: Optional[str]
    selected_tools: Optional[List[str]]
    defaults_applied: dict = field(default_factory=dict)
    generation: dict = field(default_factory=dict)
    version: int = 0
    custom_mode: Optional[CustomModeSnapshot] = None


class PreferencesService:
    def __init__(self, repo: PreferencesRepository, modes: CustomModeRepository, catalog: Optional[ModeCatalog] = None):
        self.repo = repo
        self.modes = modes
        self.catalog = catalog

    async def _load(self, user_id: UUID) -> tuple[UserPreferences, int, Optional[object]]:
        row = await self.repo.get(user_id)
        return UserPreferences.from_row(row), (row or {}).get("version", 0), (row or {}).get("updated_at")

    async def _default_mode_available(self, user_id: UUID, prefs: UserPreferences) -> bool:
        ref = parse_custom_ref(prefs.default_mode) if prefs.default_mode else None
        if ref is None:
            return True
        return await self.modes.get(user_id, ref, include_archived=False) is not None

    async def get(self, user_id: UUID) -> PreferencesResponse:
        prefs, version, updated_at = await self._load(user_id)
        return PreferencesResponse(**prefs.model_dump(), version=version, updated_at=updated_at,
                                   default_mode_available=await self._default_mode_available(user_id, prefs),
                                   options=preference_options())

    async def _validate_catalog(self, merged: UserPreferences, patch: PreferencesUpdate) -> None:
        """Built-in mode / tool ids must exist in llm-service's live registry (when it can be reached)."""
        if self.catalog is None:
            return
        needs_modes = patch.default_mode not in (None, "") and not is_custom_ref(merged.default_mode)
        needs_tools = patch.default_tools is not None
        if not (needs_modes or needs_tools):
            return
        catalog = await self.catalog.get()
        if catalog is None:
            return
        if needs_modes and merged.default_mode not in catalog.modes:
            raise UnknownCatalogItemError(f"Unknown or unavailable mode: {merged.default_mode}")
        if needs_tools:
            unknown = [t for t in merged.default_tools if t not in catalog.tools]
            if unknown:
                raise UnknownCatalogItemError(f"Unknown or unavailable tool: {unknown[0]}")

    async def update(self, user_id: UUID, patch: PreferencesUpdate) -> PreferencesResponse:
        """Applies only the sent fields. A given `version` must match (409 otherwise); without it the save is
        retried on top of the newest document. Either way the write is one atomic compare-and-set."""
        attempts = 1 if patch.version is not None else 3
        for attempt in range(attempts):
            prefs, stored_version, _ = await self._load(user_id)
            if patch.version is not None and patch.version != stored_version:
                raise PreferencesConflictError(stored_version)
            merged = UserPreferences.model_validate({**prefs.model_dump(), **patch.changes()})
            await self._validate_catalog(merged, patch)

            values = merged.model_dump(mode="json")
            ref = parse_custom_ref(merged.default_mode) if merged.default_mode else None
            if ref is not None:
                if await self.modes.get(user_id, ref, include_archived=False) is None:
                    raise InvalidDefaultModeError("That custom mode does not exist.")   # also covers other users' ids
                values["_default_custom_mode_id"] = ref
            try:
                await self.repo.save(user_id, values, stored_version)
                return await self.get(user_id)
            except PreferencesConflictError:
                if attempt == attempts - 1:
                    raise
        raise PreferencesConflictError(0)  # unreachable

    async def reset(self, user_id: UUID) -> PreferencesResponse:
        await self.repo.delete(user_id)
        return await self.get(user_id)

    async def resolve_for_message(
        self, user_id: Optional[UUID], mode: Optional[str], selected_tools: Optional[List[str]],
    ) -> ResolvedMessagePreferences:
        """Never fails because of saved defaults: if they cannot be read the message proceeds with built-in
        defaults and the explicit choices it carries. Raises CustomModeUnavailableError only for an EXPLICIT custom
        mode that cannot be used."""
        prefs, version = UserPreferences(), 0
        if user_id is not None:
            try:
                prefs, version, _ = await self._load(user_id)
            except Exception as e:  # availability over personalisation; logged without content
                logger.warning("Could not load user preferences; using defaults", error_type=type(e).__name__)

        defaults_applied = {"mode": False, "tools": False}
        resolved_mode, explicit = mode, mode is not None
        if mode is None and prefs.default_mode:
            resolved_mode, explicit = prefs.default_mode, False
            defaults_applied["mode"] = True
        resolved_tools = selected_tools
        if selected_tools is None and prefs.default_tools:
            resolved_tools = list(prefs.default_tools)
            defaults_applied["tools"] = True

        snapshot: Optional[CustomModeSnapshot] = None
        ref = parse_custom_ref(resolved_mode) if resolved_mode else None
        if resolved_mode and is_custom_ref(resolved_mode) and ref is None:
            raise CustomModeUnavailableError("invalid custom mode reference")
        if ref is not None:
            if user_id is None:
                raise CustomModeUnavailableError(str(ref))
            record = await self.modes.get(user_id, ref, include_archived=False)
            if record is None:
                if explicit:
                    raise CustomModeUnavailableError(str(ref))
                # a saved default that is gone/archived: Auto, as for any unavailable default
                logger.info("default_custom_mode_unavailable")
                resolved_mode, defaults_applied["mode"] = None, False
            else:
                snapshot = CustomModeSnapshot.from_record(record)
                # the event's mode_hint is the BUILT-IN path the custom mode runs on
                resolved_mode = snapshot.execution_style.value if snapshot.execution_style.value == "smart" else "default"

        return ResolvedMessagePreferences(
            mode=resolved_mode, selected_tools=resolved_tools, defaults_applied=defaults_applied,
            generation=generation_preferences(prefs), version=version, custom_mode=snapshot)
