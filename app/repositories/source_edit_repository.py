"""PostgreSQL access for source edits. Every statement is scoped by (owner, conversation); every write is one atomic
statement (compare-and-set on `version`), so two tabs or a retried request can never silently overwrite each other."""

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_COLUMNS = "message_id, block_id, language, source_format, original_sha256, content, version, created_at, updated_at"


@dataclass(frozen=True)
class SourceEditRow:
    message_id: uuid.UUID
    block_id: str
    language: str
    source_format: str
    original_sha256: str
    content: str
    version: int
    created_at: datetime
    updated_at: datetime


class SourceEditConflict(Exception):
    """The submitted version is not the stored one. `current` is the stored edit (None = none exists)."""

    def __init__(self, current: Optional[SourceEditRow]):
        super().__init__("source edit was changed elsewhere")
        self.current = current


class SourceEditLimitReached(Exception):
    """The conversation already holds the maximum number of edited blocks."""


def _row(r) -> SourceEditRow:
    return SourceEditRow(r.message_id, r.block_id, r.language, r.source_format, r.original_sha256, r.content,
                         r.version, r.created_at, r.updated_at)


class SourceEditRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list(self, owner: uuid.UUID, conversation_id: uuid.UUID, message_id: Optional[uuid.UUID] = None) -> list[SourceEditRow]:
        sql = f"SELECT {_COLUMNS} FROM source_edits WHERE owner_id = :o AND conversation_id = :c"
        params: dict = {"o": owner, "c": conversation_id}
        if message_id is not None:
            sql += " AND message_id = :m"
            params["m"] = message_id
        rows = (await self.session.execute(text(sql + " ORDER BY created_at, block_id"), params)).all()
        return [_row(r) for r in rows]

    async def get(self, owner: uuid.UUID, conversation_id: uuid.UUID, message_id: uuid.UUID, block_id: str) -> Optional[SourceEditRow]:
        r = (await self.session.execute(text(
            f"SELECT {_COLUMNS} FROM source_edits WHERE owner_id = :o AND conversation_id = :c AND message_id = :m AND block_id = :b"),
            {"o": owner, "c": conversation_id, "m": message_id, "b": block_id})).first()
        return _row(r) if r else None

    async def save(self, owner: uuid.UUID, conversation_id: uuid.UUID, message_id: uuid.UUID, block_id: str, *, content: str,
                   expected_version: int, language: str, source_format: str, original_sha256: str,
                   max_per_conversation: int) -> tuple[SourceEditRow, bool]:
        """Stores `content` if `expected_version` is current (0 = "no edit yet"). Returns (edit, written); `written` is False
        when the stored text already equals `content` (a retry of a save that did land): success, no write, no version bump.
        Language, format and the original fingerprint are fixed at creation. Raises SourceEditConflict / SourceEditLimitReached."""
        params = {"o": owner, "c": conversation_id, "m": message_id, "b": block_id, "content": content, "exp": expected_version,
                  "id": uuid.uuid4(), "lang": language, "fmt": source_format, "sha": original_sha256, "cap": max_per_conversation}
        if expected_version == 0:
            r = (await self.session.execute(text(
                "INSERT INTO source_edits (id, owner_id, conversation_id, message_id, block_id, language, source_format, "
                "original_sha256, content, version) "
                "SELECT :id, :o, :c, :m, :b, :lang, :fmt, :sha, :content, 1 "
                "WHERE (SELECT count(*) FROM source_edits WHERE owner_id = :o AND conversation_id = :c) < :cap "
                f"ON CONFLICT (owner_id, conversation_id, message_id, block_id) DO NOTHING RETURNING {_COLUMNS}"), params)).first()
        else:
            r = (await self.session.execute(text(
                "UPDATE source_edits SET content = :content, version = version + 1, updated_at = now() "
                "WHERE owner_id = :o AND conversation_id = :c AND message_id = :m AND block_id = :b AND version = :exp "
                f"AND content IS DISTINCT FROM :content RETURNING {_COLUMNS}"), params)).first()
        if r is not None:
            return _row(r), True
        current = await self.get(owner, conversation_id, message_id, block_id)
        if current is None:
            if expected_version == 0:
                raise SourceEditLimitReached()
            raise SourceEditConflict(None)             # the edit was discarded elsewhere
        if current.content == content:
            return current, False
        raise SourceEditConflict(current)

    async def delete(self, owner: uuid.UUID, conversation_id: uuid.UUID, message_id: uuid.UUID, block_id: str,
                     expected_version: int) -> bool:
        """Discards the edit (back to the original). True if a row was removed; False if none existed (idempotent retry).
        A stale version raises SourceEditConflict and removes nothing."""
        r = (await self.session.execute(text(
            "DELETE FROM source_edits WHERE owner_id = :o AND conversation_id = :c AND message_id = :m AND block_id = :b "
            "AND version = :exp RETURNING 1"),
            {"o": owner, "c": conversation_id, "m": message_id, "b": block_id, "exp": expected_version})).first()
        if r is not None:
            return True
        current = await self.get(owner, conversation_id, message_id, block_id)
        if current is None:
            return False
        raise SourceEditConflict(current)
