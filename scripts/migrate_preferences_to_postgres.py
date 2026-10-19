"""Copy legacy Cassandra user preferences into PostgreSQL.

    uv run python scripts/migrate_preferences_to_postgres.py --dry-run          # report only, writes nothing
    uv run python scripts/migrate_preferences_to_postgres.py                     # migrate (idempotent, resumable)
    uv run python scripts/migrate_preferences_to_postgres.py --verify            # migrate-less field-level check

Reads Cassandra (never writes or deletes there), writes PostgreSQL (DATABASE_URL). See the runbook in
docs/MODES_AND_PREFERENCES.md. Exit code 2 when records were rejected or verification found mismatches.
"""

import argparse
import asyncio
import json
import sys
import types
from pathlib import Path

# Same Python 3.12 compatibility patch as app/main.py (the Cassandra driver imports asyncore).
_asyncore = types.ModuleType("asyncore")
_asyncore.dispatcher = type("dispatcher", (), {})
sys.modules.setdefault("asyncore", _asyncore)

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db.cassandra import cassandra_manager  # noqa: E402
from app.db.postgres import dispose_engine  # noqa: E402
from app.db.session import get_session_factory  # noqa: E402
from app.migration.preferences_to_postgres import migrate, verify  # noqa: E402

COLUMNS = ("user_id, default_mode, default_tools, answer_length, tone, language, personal_instructions, citations, "
           "document_context, version, updated_at")


def legacy_rows(fetch_size: int):
    session = cassandra_manager.session
    if session is None:
        raise SystemExit("Cassandra is not reachable; cannot read legacy preferences.")
    stmt = session.prepare(f"SELECT {COLUMNS} FROM user_preferences")
    stmt.fetch_size = fetch_size
    for row in session.execute(stmt):
        yield row._asdict()


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--verify", action="store_true", help="only compare legacy rows with PostgreSQL")
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--max-rows-per-second", type=float, default=None)
    ap.add_argument("--report", default="preferences_migration_report.json")
    args = ap.parse_args()

    cassandra_manager.initialize()
    factory = get_session_factory()
    try:
        from app.migration.preferences_to_postgres import MigrationReport

        report = MigrationReport(dry_run=args.dry_run)
        if not args.verify:
            report = await migrate(legacy_rows(args.batch_size), factory, dry_run=args.dry_run,
                                   batch_size=args.batch_size, max_rows_per_second=args.max_rows_per_second)
        if not args.dry_run:
            report = await verify(legacy_rows(args.batch_size), factory, report)
        Path(args.report).write_text(json.dumps(report.as_dict(), indent=2, default=str), encoding="utf-8")
        summary = {k: v for k, v in report.as_dict().items() if not isinstance(v, list)}
        print(json.dumps(summary, indent=2))
        print(f"report written to {args.report} (counts and reason codes only; no preference contents)")
        return 2 if (report.rejected or report.mismatches) else 0
    finally:
        await dispose_engine()
        cassandra_manager.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
