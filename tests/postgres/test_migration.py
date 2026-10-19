"""Cassandra -> PostgreSQL preferences migration, against real PostgreSQL with an in-memory legacy source."""

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from app.migration.preferences_to_postgres import migrate, transform, verify

pytestmark = pytest.mark.asyncio

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def legacy(user=None, version=2, minutes=0, **over):
    row = dict(user_id=user or uuid.uuid4(), default_mode="tutor", default_tools=["web_search"], answer_length="concise",
               tone="friendly", language="hi", personal_instructions="Prefer Python", citations="hide",
               document_context="attached_only", version=version, updated_at=T0 + timedelta(minutes=minutes))
    row.update(over)
    return row


async def pg_row(factory, user):
    async with factory() as s:
        return (await s.execute(text("SELECT * FROM user_preferences WHERE user_id = :u"), {"u": user})).mappings().first()


async def test_valid_records_migrate_with_every_field_and_the_legacy_version(session_factory):
    rows = [legacy() for _ in range(5)]
    report = await migrate(rows, session_factory, batch_size=2)
    assert (report.examined, report.inserted, report.rejected, report.normalized_fields) == (5, 5, 0, 0)
    got = await pg_row(session_factory, rows[0]["user_id"])
    assert (got["default_mode"], list(got["default_tools"]), got["answer_length"], got["tone"], got["language"],
            got["personal_instructions"], got["citations"], got["document_context"], got["version"]) == (
        "tutor", ["web_search"], "concise", "friendly", "hi", "Prefer Python", "hide", "attached_only", 2)
    verified = await verify(rows, session_factory, report)
    assert verified.verified == 5 and verified.mismatches == []


async def test_rerunning_is_idempotent_and_never_duplicates(session_factory):
    rows = [legacy() for _ in range(4)]
    await migrate(rows, session_factory)
    again = await migrate(rows, session_factory)
    assert (again.inserted, again.refreshed, again.skipped_existing) == (0, 0, 4)
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM user_preferences"))).scalar_one() == 4


async def test_an_interrupted_run_resumes_without_loss(session_factory):
    rows = [legacy() for _ in range(6)]
    await migrate(rows[:3], session_factory)                         # "crash" after the first half
    resumed = await migrate(rows, session_factory)
    assert (resumed.inserted, resumed.skipped_existing) == (3, 3)


async def test_newer_postgres_data_is_never_overwritten_by_stale_legacy_data(session_factory):
    user = uuid.uuid4()
    await migrate([legacy(user, version=2, minutes=0)], session_factory)
    async with session_factory() as s:                               # edited through the NEW application
        await s.execute(text("UPDATE user_preferences SET tone='simple', version=5, updated_at=:t WHERE user_id=:u"),
                        {"u": user, "t": T0 + timedelta(days=3)})
        await s.commit()
    report = await migrate([legacy(user, version=2, minutes=0, tone="friendly")], session_factory)
    assert report.skipped_existing == 1 and (await pg_row(session_factory, user))["tone"] == "simple"
    verified = await verify([legacy(user, tone="friendly")], session_factory, report)
    assert verified.mismatches == []                                  # a newer PG version is expected, not an error


async def test_a_legacy_row_edited_after_an_earlier_copy_refreshes_an_untouched_postgres_row(session_factory):
    user = uuid.uuid4()
    await migrate([legacy(user, version=2, minutes=0)], session_factory)
    report = await migrate([legacy(user, version=3, minutes=10, tone="simple")], session_factory)
    assert report.refreshed == 1 and (await pg_row(session_factory, user))["tone"] == "simple"


async def test_invalid_records_are_reported_and_unsupported_values_are_normalized(session_factory):
    good = legacy()
    odd = legacy(tone="removed-tone", language="klingon")
    bad = legacy(user=None)
    bad["user_id"] = "not-a-uuid"
    report = await migrate([good, odd, bad], session_factory)
    assert (report.inserted, report.rejected) == (2, 1) and report.rejects[0]["reason"] == "invalid_user_id"
    assert report.normalized_fields == 2
    got = await pg_row(session_factory, odd["user_id"])
    assert (got["tone"], got["language"]) == ("professional", "auto")
    assert "Prefer Python" not in str(report.as_dict())               # the report never contains preference contents


async def test_dry_run_reports_without_writing(session_factory):
    rows = [legacy() for _ in range(3)]
    report = await migrate(rows, session_factory, dry_run=True)
    assert report.inserted == 3 and report.dry_run
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM user_preferences"))).scalar_one() == 0


async def test_verification_detects_inconsistencies(session_factory):
    rows = [legacy(), legacy()]
    report = await migrate(rows, session_factory)
    async with session_factory() as s:
        await s.execute(text("UPDATE user_preferences SET language='en' WHERE user_id=:u"), {"u": rows[0]["user_id"]})
        await s.execute(text("DELETE FROM user_preferences WHERE user_id=:u"), {"u": rows[1]["user_id"]})
        await s.commit()
    verified = await verify(rows, session_factory, report)
    assert verified.verified == 0
    assert {m["user_id"]: m["fields"] for m in verified.mismatches} == {
        str(rows[0]["user_id"]): ["language"], str(rows[1]["user_id"]): ["<missing row>"]}


async def test_missing_updated_at_and_version_still_migrate(session_factory):
    row = legacy(version=None, updated_at=None)
    values, _n, reason = transform(dict(row))
    assert reason is None and values["version"] == 1
    assert (await migrate([row], session_factory)).inserted == 1
