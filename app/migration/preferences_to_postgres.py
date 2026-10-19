"""
One-off, repeatable migration of legacy user preferences from Cassandra (`graphgpt_conversations.user_preferences`)
into PostgreSQL (`user_preferences`). The Cassandra table is only READ; nothing is deleted or modified there.

Safety rules (see docs/MODES_AND_PREFERENCES.md, "Migration runbook"):
  * Idempotent and resumable: re-running never duplicates rows and never loses progress.
  * Never overwrites newer PostgreSQL data: an existing PG row is only refreshed when the legacy row is strictly
    newer in BOTH version and updated_at (i.e. it was edited in Cassandra after the earlier copy). Rows edited
    through the new application (PostgreSQL) are never touched by stale legacy data.
  * Every record is validated with the same domain model the API uses; unsupported values fall back to the field's
    default (counted as `normalized_fields`), rows that cannot be migrated at all are rejected with a reason code.
  * Bounded batches (one transaction per batch) and an optional rate limit.
  * Dry-run mode computes the full report without writing.
  * The report holds counts and reason codes only, never preference contents.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.custom_modes import parse_custom_ref
from app.models.preferences import UserPreferences

FIELDS = ("default_mode", "default_tools", "answer_length", "tone", "language", "personal_instructions", "citations",
          "document_context")


@dataclass
class MigrationReport:
    examined: int = 0
    inserted: int = 0
    refreshed: int = 0
    skipped_existing: int = 0          # PostgreSQL row is as new or newer: left alone
    rejected: int = 0
    normalized_fields: int = 0         # a legacy value was unsupported and replaced by the field's default
    dry_run: bool = False
    rejects: List[dict] = field(default_factory=list)     # {user_id, reason}
    verified: int = 0
    mismatches: List[dict] = field(default_factory=list)  # {user_id, fields:[...]}  (names only)

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _as_uuid(value: Any) -> Optional[uuid.UUID]:
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return None


def transform(row: Dict[str, Any]) -> tuple[Optional[dict], int, Optional[str]]:
    """Legacy row -> (PostgreSQL values | None, normalized_field_count, reject_reason | None)."""
    user_id = _as_uuid(row.get("user_id"))
    if user_id is None:
        return None, 0, "invalid_user_id"
    prefs = UserPreferences.from_row(row)
    normalized = sum(1 for f in FIELDS if f != "default_mode" and row.get(f) is not None
                     and (list(row[f]) if f == "default_tools" else row[f]) != getattr(prefs, f))
    default_mode = prefs.default_mode
    if row.get("default_mode") and not default_mode:
        normalized += 1
    if default_mode and parse_custom_ref(default_mode):      # legacy data cannot contain custom modes
        default_mode, normalized = None, normalized + 1
    version = row.get("version") or 0
    updated_at = row.get("updated_at")
    if isinstance(updated_at, datetime) and updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return ({
        "user_id": user_id,
        "default_mode": default_mode,
        "default_tools": list(prefs.default_tools),
        "answer_length": prefs.answer_length.value if hasattr(prefs.answer_length, "value") else prefs.answer_length,
        "tone": prefs.tone.value if hasattr(prefs.tone, "value") else prefs.tone,
        "language": prefs.language,
        "personal_instructions": prefs.personal_instructions,
        "citations": prefs.citations.value if hasattr(prefs.citations, "value") else prefs.citations,
        "document_context": prefs.document_context.value if hasattr(prefs.document_context, "value") else prefs.document_context,
        "version": max(int(version), 1),
        "updated_at": updated_at or datetime.now(timezone.utc),
    }, normalized, None)


_UPSERT = text("""
    INSERT INTO user_preferences (user_id, default_mode, default_tools, answer_length, tone, language,
                                  personal_instructions, citations, document_context, version, created_at, updated_at)
    VALUES (:user_id, :default_mode, :default_tools, :answer_length, :tone, :language, :personal_instructions,
            :citations, :document_context, :version, :updated_at, :updated_at)
    ON CONFLICT (user_id) DO UPDATE SET
        default_mode = EXCLUDED.default_mode, default_custom_mode_id = NULL, default_tools = EXCLUDED.default_tools,
        answer_length = EXCLUDED.answer_length, tone = EXCLUDED.tone, language = EXCLUDED.language,
        personal_instructions = EXCLUDED.personal_instructions, citations = EXCLUDED.citations,
        document_context = EXCLUDED.document_context, version = EXCLUDED.version, updated_at = EXCLUDED.updated_at
    WHERE user_preferences.version < EXCLUDED.version AND user_preferences.updated_at < EXCLUDED.updated_at
    RETURNING (xmax = 0) AS inserted
""")


async def migrate(
    rows: Iterable[Dict[str, Any]],
    session_factory: async_sessionmaker[AsyncSession],
    *,
    dry_run: bool = False,
    batch_size: int = 200,
    max_rows_per_second: Optional[float] = None,
) -> MigrationReport:
    report = MigrationReport(dry_run=dry_run)
    batch: List[dict] = []

    async def flush() -> None:
        if not batch:
            return
        if dry_run:
            existing = await _existing_versions(session_factory, [v["user_id"] for v in batch])
            for v in batch:
                current = existing.get(v["user_id"])
                if current is None:
                    report.inserted += 1
                elif current[0] < v["version"] and current[1] < v["updated_at"]:
                    report.refreshed += 1
                else:
                    report.skipped_existing += 1
        else:
            async with session_factory() as session:                # one transaction per batch
                for values in batch:
                    result = (await session.execute(_UPSERT, {**values, "default_tools": values["default_tools"]})).first()
                    if result is None:
                        report.skipped_existing += 1
                    elif result.inserted:
                        report.inserted += 1
                    else:
                        report.refreshed += 1
                await session.commit()
        batch.clear()

    started = time.monotonic()
    for row in rows:
        report.examined += 1
        values, normalized, reason = transform(dict(row))
        if values is None:
            report.rejected += 1
            report.rejects.append({"user_id": str(row.get("user_id")), "reason": reason})
            continue
        report.normalized_fields += normalized
        batch.append(values)
        if len(batch) >= batch_size:
            await flush()
        if max_rows_per_second:
            wait = report.examined / max_rows_per_second - (time.monotonic() - started)
            if wait > 0:
                await asyncio.sleep(wait)
    await flush()
    return report


async def _existing_versions(session_factory, user_ids: List[uuid.UUID]) -> Dict[uuid.UUID, tuple]:
    async with session_factory() as session:
        result = await session.execute(
            text("SELECT user_id, version, updated_at FROM user_preferences WHERE user_id = ANY(:ids)"), {"ids": user_ids})
        return {r.user_id: (r.version, r.updated_at) for r in result}


async def verify(rows: Iterable[Dict[str, Any]], session_factory, report: MigrationReport) -> MigrationReport:
    """Field-level comparison of every migratable legacy row against PostgreSQL. A PostgreSQL row that was edited
    through the new application (higher version) is expected to differ and is not a mismatch."""
    for row in rows:
        values, _n, reason = transform(dict(row))
        if values is None:
            continue
        async with session_factory() as session:
            pg = (await session.execute(text("SELECT * FROM user_preferences WHERE user_id = :u"),
                                        {"u": values["user_id"]})).mappings().first()
        if pg is None:
            report.mismatches.append({"user_id": str(values["user_id"]), "fields": ["<missing row>"]})
            continue
        if pg["version"] > values["version"]:
            continue
        diff = [f for f in FIELDS if (list(pg[f]) if f == "default_tools" else pg[f]) != values[f]]
        if diff:
            report.mismatches.append({"user_id": str(values["user_id"]), "fields": diff})
        else:
            report.verified += 1
    return report
