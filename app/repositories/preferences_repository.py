"""
User Preferences Repository (Cassandra).

One row per user. Writes are conditional (lightweight transactions) so two clients
editing settings at once cannot silently overwrite each other: the first write wins
and the second gets a version conflict.
"""

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from app.db.cassandra import cassandra_manager

_COLUMNS = (
    "default_mode", "default_tools", "answer_length", "tone", "language",
    "personal_instructions", "citations", "document_context",
)


class PreferencesConflictError(Exception):
    """The stored version is not the one the client edited (someone saved in between)."""

    def __init__(self, current_version: int):
        self.current_version = current_version
        super().__init__(f"Preferences were changed elsewhere (current version {current_version}).")


class CassandraPreferencesRepository:
    def __init__(self):
        self.manager = cassandra_manager
        self._statements = {}

    def _get_prepared(self, name: str, cql: str):
        if name not in self._statements:
            session = self.manager.session
            if not session:
                raise RuntimeError("Cassandra database session not available.")
            self._statements[name] = session.prepare(cql)
        return self._statements[name]

    def get(self, user_id: UUID) -> Optional[dict]:
        stmt = self._get_prepared(
            "prefs_get",
            f"SELECT {', '.join(_COLUMNS)}, version, updated_at FROM user_preferences WHERE user_id = ?",
        )
        row = self.manager.session.execute(stmt, (user_id,)).one()
        if row is None:
            return None
        data = {c: getattr(row, c) for c in _COLUMNS}
        data["default_tools"] = list(data["default_tools"] or [])
        data["version"] = row.version or 0
        data["updated_at"] = row.updated_at
        return data

    def save(self, user_id: UUID, values: dict, expected_version: int) -> int:
        """Writes the full document if the stored version equals `expected_version`
        (0 = no row yet). Returns the new version; raises PreferencesConflictError."""
        now = datetime.now(timezone.utc)
        new_version = expected_version + 1
        params = tuple(values.get(c) for c in _COLUMNS)
        if expected_version == 0:
            stmt = self._get_prepared(
                "prefs_insert",
                f"INSERT INTO user_preferences (user_id, {', '.join(_COLUMNS)}, version, updated_at) "
                f"VALUES (?, {', '.join('?' for _ in _COLUMNS)}, ?, ?) IF NOT EXISTS",
            )
            result = self.manager.session.execute(stmt, (user_id, *params, new_version, now))
        else:
            stmt = self._get_prepared(
                "prefs_update",
                f"UPDATE user_preferences SET {', '.join(f'{c} = ?' for c in _COLUMNS)}, version = ?, updated_at = ? "
                f"WHERE user_id = ? IF version = ?",
            )
            result = self.manager.session.execute(stmt, (*params, new_version, now, user_id, expected_version))
        if not result.was_applied:
            existing = result.one()
            raise PreferencesConflictError(getattr(existing, "version", 0) or 0)
        return new_version

    def delete(self, user_id: UUID) -> None:
        stmt = self._get_prepared("prefs_delete", "DELETE FROM user_preferences WHERE user_id = ?")
        self.manager.session.execute(stmt, (user_id,))
