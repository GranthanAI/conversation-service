"""
User preferences: read / partial update / reset, and resolution for a new message.

Resolution rules for a message (documented in docs/updates and the API docs):
  * mode:   the message's own `mode` wins (including "auto"); if the message sends no
            mode, the saved `default_mode` is used.
  * tools:  the message's own `selected_tools` wins (including an explicit empty list);
            if the message sends none, the saved `default_tools` are used.
  * Values that came from saved defaults are flagged in `defaults_applied`, so
    llm-service (owner of the live mode/tool registry) skips any default that is no
    longer available instead of rejecting the message. Explicit choices stay strict.
  * Generation preferences (length, tone, language, instructions, citations) are taken
    from the saved document at send time; a change affects messages sent afterwards.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from uuid import UUID

from app.core.logging import logger
from app.models.preferences import (
    PreferencesResponse,
    PreferencesUpdate,
    UserPreferences,
    generation_preferences,
    preference_options,
)
from app.repositories.preferences_repository import CassandraPreferencesRepository, PreferencesConflictError


@dataclass
class ResolvedMessagePreferences:
    mode: Optional[str]
    selected_tools: Optional[List[str]]
    defaults_applied: dict = field(default_factory=dict)
    generation: dict = field(default_factory=dict)
    version: int = 0


class PreferencesService:
    def __init__(self, repo: CassandraPreferencesRepository):
        self.repo = repo

    def _load(self, user_id: UUID) -> tuple[UserPreferences, int, Optional[object]]:
        row = self.repo.get(user_id)
        prefs = UserPreferences.from_row(row)
        return prefs, (row or {}).get("version", 0), (row or {}).get("updated_at")

    def get(self, user_id: UUID) -> PreferencesResponse:
        prefs, version, updated_at = self._load(user_id)
        return PreferencesResponse(**prefs.model_dump(), version=version, updated_at=updated_at, options=preference_options())

    def update(self, user_id: UUID, patch: PreferencesUpdate) -> PreferencesResponse:
        """Applies only the sent fields. If `patch.version` is given it must match the
        stored version (409 otherwise); without it the latest stored version is used,
        which still never loses a concurrent write because the save is conditional."""
        attempts = 1 if patch.version is not None else 3
        for attempt in range(attempts):
            prefs, stored_version, _ = self._load(user_id)
            if patch.version is not None and patch.version != stored_version:
                raise PreferencesConflictError(stored_version)
            merged = UserPreferences.model_validate({**prefs.model_dump(), **patch.changes()})
            try:
                self.repo.save(user_id, merged.model_dump(mode="json"), stored_version)
                return self.get(user_id)
            except PreferencesConflictError:
                # Without a client version the caller only cares about its own fields:
                # re-read and re-apply them on top of the newer document.
                if attempt == attempts - 1:
                    raise
        raise PreferencesConflictError(0)  # unreachable

    def reset(self, user_id: UUID) -> PreferencesResponse:
        self.repo.delete(user_id)
        return self.get(user_id)

    def resolve_for_message(
        self,
        user_id: Optional[UUID],
        mode: Optional[str],
        selected_tools: Optional[List[str]],
    ) -> ResolvedMessagePreferences:
        """Never raises: if preferences cannot be read the message proceeds with the
        built-in defaults (and the explicit choices it carries)."""
        prefs, version = UserPreferences(), 0
        if user_id is not None:
            try:
                prefs, version, _ = self._load(user_id)
            except Exception as e:  # availability over personalisation; logged
                logger.warning("Could not load user preferences; using defaults", user_id=str(user_id), error=str(e))

        defaults_applied = {"mode": False, "tools": False}
        resolved_mode = mode
        if mode is None and prefs.default_mode:
            resolved_mode = prefs.default_mode
            defaults_applied["mode"] = True
        resolved_tools = selected_tools
        if selected_tools is None and prefs.default_tools:
            resolved_tools = list(prefs.default_tools)
            defaults_applied["tools"] = True

        return ResolvedMessagePreferences(
            mode=resolved_mode,
            selected_tools=resolved_tools,
            defaults_applied=defaults_applied,
            generation=generation_preferences(prefs),
            version=version,
        )
