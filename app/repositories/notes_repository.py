"""PostgreSQL access for conversation notes. Every statement is scoped by (owner, conversation); every write is one atomic
compare-and-set statement, so two writers (two tabs, a retried request, a request that was in flight when the user kept
typing) can never silently overwrite each other."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_COLUMNS = "content, version, updated_at"


@dataclass(frozen=True)
class NotesRow:
    content: str
    version: int
    updated_at: Optional[datetime]


class NotesVersionConflict(Exception):
    """The submitted version is not the stored one. `current` is the stored document (None = none exists)."""

    def __init__(self, current: Optional[NotesRow]):
        super().__init__("notes were changed elsewhere")
        self.current = current


class NotesRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, owner: uuid.UUID, conversation_id: uuid.UUID) -> Optional[NotesRow]:
        row = (await self.session.execute(
            text(f"SELECT {_COLUMNS} FROM conversation_notes WHERE owner_id = :o AND conversation_id = :c"),
            {"o": owner, "c": conversation_id})).first()
        return NotesRow(row.content, row.version, row.updated_at) if row else None

    async def save(self, owner: uuid.UUID, conversation_id: uuid.UUID, content: str,
                   expected_version: int) -> tuple[NotesRow, bool]:
        """Stores `content` if `expected_version` is the current version (0 = "no document yet").
        Returns (document, written). `written` is False when nothing had to change (the stored text is already exactly
        this, whatever its version): no write, no version bump, no spurious conflict. Raises NotesVersionConflict otherwise."""
        params = {"o": owner, "c": conversation_id, "content": content, "exp": expected_version, "id": uuid.uuid4()}
        row = None
        if expected_version == 0:
            row = (await self.session.execute(text(
                f"INSERT INTO conversation_notes (id, owner_id, conversation_id, content, version) "
                f"VALUES (:id, :o, :c, :content, 1) ON CONFLICT (owner_id, conversation_id) DO NOTHING RETURNING {_COLUMNS}"),
                params)).first()
        else:
            row = (await self.session.execute(text(
                f"UPDATE conversation_notes SET content = :content, version = version + 1, updated_at = now() "
                f"WHERE owner_id = :o AND conversation_id = :c AND version = :exp AND content IS DISTINCT FROM :content "
                f"RETURNING {_COLUMNS}"), params)).first()
        if row is not None:
            return NotesRow(row.content, row.version, row.updated_at), True
        current = await self.get(owner, conversation_id)
        if current is not None and current.content == content:
            return current, False                      # already stored exactly: success without a write
        raise NotesVersionConflict(current)
