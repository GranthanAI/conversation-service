"""Repositories against real PostgreSQL: constraints, owner scoping, compare-and-set, concurrency, rollback."""

import asyncio
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.models.sql_models import CustomModeRecord, UserPreferenceRecord
from app.repositories.pg_repositories import (
    CustomModeConflictError, CustomModeLimitError, CustomModeNameTakenError, CustomModeNotFoundError,
    CustomModeRepository, InvalidDefaultModeError, PreferencesConflictError, PreferencesRepository,
)

pytestmark = pytest.mark.asyncio

ALICE, BOB = uuid.uuid4(), uuid.uuid4()


async def make_mode(factory, owner=ALICE, name="Reviewer", style="direct", limit=20, instructions="Be strict."):
    async with factory() as s:
        rec = await CustomModeRepository(s).create(owner, name=name, description="", instructions=instructions,
                                                    execution_style=style, max_per_user=limit)
        await s.commit()
        return rec


def prefs_values(**over):
    base = dict(default_mode=None, default_tools=[], answer_length="balanced", tone="professional", language="auto",
                personal_instructions="", citations="show", document_context="conversation")
    base.update(over)
    return base


# ------------------------------------------------------------------ custom modes


async def test_create_get_update_archive_roundtrip(session_factory):
    rec = await make_mode(session_factory)
    assert rec.version == 1 and rec.status == "active" and rec.name_normalized == "reviewer"
    async with session_factory() as s:
        repo = CustomModeRepository(s)
        updated = await repo.update(ALICE, rec.mode_id, {"instructions": "Be gentle."}, expected_version=1)
        assert updated.version == 2 and updated.instructions == "Be gentle." and updated.name == "Reviewer"   # partial
        archived = await repo.archive(ALICE, rec.mode_id)
        assert archived.status == "archived" and archived.archived_at is not None and archived.version == 3
        again = await repo.archive(ALICE, rec.mode_id)                  # idempotent
        assert again.version == 3
        await s.commit()


async def test_owner_scoping_other_users_modes_do_not_exist(session_factory):
    rec = await make_mode(session_factory, owner=ALICE)
    async with session_factory() as s:
        repo = CustomModeRepository(s)
        assert await repo.get(BOB, rec.mode_id) is None
        with pytest.raises(CustomModeNotFoundError):
            await repo.update(BOB, rec.mode_id, {"name": "Hacked"}, None)
        with pytest.raises(CustomModeNotFoundError):
            await repo.archive(BOB, rec.mode_id)
        rows, total = await repo.list(BOB, include_archived=True, limit=50, offset=0)
        assert rows == [] and total == 0


async def test_optimistic_locking_rejects_a_stale_version(session_factory):
    rec = await make_mode(session_factory)
    async with session_factory() as s:
        await CustomModeRepository(s).update(ALICE, rec.mode_id, {"name": "Reviewer v2"}, expected_version=1)
        await s.commit()
    async with session_factory() as s:
        with pytest.raises(CustomModeConflictError) as exc:
            await CustomModeRepository(s).update(ALICE, rec.mode_id, {"name": "Stale"}, expected_version=1)
        assert exc.value.current_version == 2


async def test_archived_modes_cannot_be_edited(session_factory):
    rec = await make_mode(session_factory)
    async with session_factory() as s:
        await CustomModeRepository(s).archive(ALICE, rec.mode_id)
        await s.commit()
    async with session_factory() as s:
        with pytest.raises(CustomModeNotFoundError):
            await CustomModeRepository(s).update(ALICE, rec.mode_id, {"name": "x"}, None)


async def test_names_are_unique_per_owner_case_insensitively_and_archiving_frees_the_name(session_factory):
    first = await make_mode(session_factory, name="Tutor Pro")
    with pytest.raises(CustomModeNameTakenError):
        await make_mode(session_factory, name="  tutor   PRO ")
    await make_mode(session_factory, owner=BOB, name="Tutor Pro")          # another owner is unaffected
    async with session_factory() as s:
        await CustomModeRepository(s).archive(ALICE, first.mode_id)
        await s.commit()
    again = await make_mode(session_factory, name="tutor pro")             # name reusable once archived
    assert again.mode_id != first.mode_id


async def test_database_constraints_reject_bad_rows_even_if_the_app_were_bypassed(session_factory):
    bad = [dict(name="  ", instructions="x", execution_style="direct"),
           dict(name="a", instructions="   ", execution_style="direct"),
           dict(name="a", instructions="x", execution_style="python"),
           dict(name="a", instructions="x", execution_style="direct", status="deleted"),
           dict(name="a", instructions="x", execution_style="direct", version=0)]
    for fields in bad:
        async with session_factory() as s:
            s.add(CustomModeRecord(mode_id=uuid.uuid4(), owner_user_id=ALICE, name_normalized=fields["name"].strip() or "n",
                                   **fields))
            with pytest.raises(IntegrityError):
                await s.commit()


async def test_per_user_limit_holds_under_concurrent_creation(session_factory):
    limit = 5

    async def create(i):
        async with session_factory() as s:
            try:
                await CustomModeRepository(s).create(ALICE, name=f"Mode {i}", description="", instructions="x",
                                                      execution_style="direct", max_per_user=limit)
                await s.commit()
                return "ok"
            except CustomModeLimitError:
                await s.rollback()
                return "limit"

    results = await asyncio.gather(*(create(i) for i in range(12)))
    assert results.count("ok") == limit and results.count("limit") == 12 - limit
    async with session_factory() as s:
        _rows, total = await CustomModeRepository(s).list(ALICE, include_archived=False, limit=50, offset=0)
        assert total == limit


async def test_concurrent_creates_of_the_same_name_produce_exactly_one(session_factory):
    async def create():
        async with session_factory() as s:
            try:
                await CustomModeRepository(s).create(ALICE, name="Same", description="", instructions="x",
                                                      execution_style="direct", max_per_user=20)
                await s.commit()
                return "ok"
            except CustomModeNameTakenError:
                await s.rollback()
                return "taken"

    assert sorted(await asyncio.gather(*(create() for _ in range(6)))) == ["ok"] + ["taken"] * 5


async def test_concurrent_updates_with_the_same_version_only_one_wins(session_factory):
    rec = await make_mode(session_factory)

    async def edit(n):
        async with session_factory() as s:
            try:
                await CustomModeRepository(s).update(ALICE, rec.mode_id, {"description": f"d{n}"}, expected_version=1)
                await s.commit()
                return "ok"
            except CustomModeConflictError:
                await s.rollback()
                return "conflict"

    assert sorted(await asyncio.gather(*(edit(i) for i in range(5)))) == ["conflict"] * 4 + ["ok"]


async def test_list_pagination_and_archived_filter_sorted_by_recent_update(session_factory):
    ids = [(await make_mode(session_factory, name=f"M{i}")).mode_id for i in range(5)]
    async with session_factory() as s:
        repo = CustomModeRepository(s)
        await repo.archive(ALICE, ids[0])
        await repo.update(ALICE, ids[1], {"description": "touched"}, None)       # most recently updated
        await s.commit()
    async with session_factory() as s:
        repo = CustomModeRepository(s)
        page1, total = await repo.list(ALICE, include_archived=False, limit=2, offset=0)
        page2, _ = await repo.list(ALICE, include_archived=False, limit=2, offset=2)
        everything, total_all = await repo.list(ALICE, include_archived=True, limit=50, offset=0)
        assert total == 4 and total_all == 5
        assert page1[0].mode_id == ids[1] and len(page1) == 2 and len(page2) == 2
        assert {r.mode_id for r in page1} | {r.mode_id for r in page2} == set(ids[1:])
        assert len(everything) == 5


async def test_a_failed_transaction_leaves_nothing_behind(session_factory):
    async with session_factory() as s:
        repo = CustomModeRepository(s)
        await repo.create(ALICE, name="Temp", description="", instructions="x", execution_style="direct", max_per_user=5)
        await s.rollback()
    async with session_factory() as s:
        assert (await CustomModeRepository(s).list(ALICE, include_archived=True, limit=10, offset=0))[1] == 0


# ------------------------------------------------------------------ preferences


async def test_preferences_insert_then_compare_and_set(session_factory):
    async with session_factory() as s:
        repo = PreferencesRepository(s)
        assert await repo.get(ALICE) is None
        assert await repo.save(ALICE, prefs_values(tone="friendly", default_tools=["a"]), 0) == 1
        assert await repo.save(ALICE, prefs_values(tone="simple"), 1) == 2
        row = await repo.get(ALICE)
        assert row["tone"] == "simple" and row["version"] == 2 and row["default_tools"] == []
        with pytest.raises(PreferencesConflictError) as exc:
            await repo.save(ALICE, prefs_values(tone="friendly"), 1)          # stale
        assert exc.value.current_version == 2
        with pytest.raises(PreferencesConflictError):
            await repo.save(ALICE, prefs_values(), 0)                         # "no row yet" but one exists
        await s.commit()


async def test_concurrent_first_saves_create_exactly_one_row(session_factory):
    async def save(tone):
        async with session_factory() as s:
            try:
                await PreferencesRepository(s).save(ALICE, prefs_values(tone=tone), 0)
                await s.commit()
                return "ok"
            except PreferencesConflictError:
                await s.rollback()
                return "conflict"

    assert sorted(await asyncio.gather(save("friendly"), save("simple"), save("professional"))) == ["conflict", "conflict", "ok"]


async def test_default_custom_mode_is_a_real_foreign_key_owned_by_the_row(session_factory):
    rec = await make_mode(session_factory)
    async with session_factory() as s:
        repo = PreferencesRepository(s)
        await repo.save(ALICE, prefs_values(default_mode=f"custom:{rec.mode_id}", _default_custom_mode_id=rec.mode_id), 0)
        assert (await repo.get(ALICE))["default_mode"] == f"custom:{rec.mode_id}"
        with pytest.raises(InvalidDefaultModeError):
            await repo.save(ALICE, prefs_values(_default_custom_mode_id=uuid.uuid4()), 1)   # no such mode (FK)
        await s.rollback()


async def test_preference_constraints(session_factory):
    async with session_factory() as s:
        s.add(UserPreferenceRecord(user_id=ALICE, tone="aggressive"))
        with pytest.raises(IntegrityError):
            await s.commit()


async def test_reset_deletes_the_row(session_factory):
    async with session_factory() as s:
        repo = PreferencesRepository(s)
        await repo.save(ALICE, prefs_values(), 0)
        await repo.delete(ALICE)
        assert await repo.get(ALICE) is None
        await s.commit()


async def test_migration_downgrade_and_upgrade_are_reversible(migrated_url):
    from alembic import command
    from alembic.config import Config
    import os
    ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    cfg = Config(os.path.join(ROOT, "alembic.ini"))
    cfg.set_main_option("script_location", os.path.join(ROOT, "app", "db", "migrations"))
    cfg.set_main_option("sqlalchemy.url", migrated_url)
    await asyncio.to_thread(command.downgrade, cfg, "base")
    await asyncio.to_thread(command.upgrade, cfg, "head")
