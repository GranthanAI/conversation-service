"""Artifact registry against real PostgreSQL (migrated with the real Alembic chain): deduplication, idempotent
registration, the render lifecycle, concurrency, crash recovery and the database-level guarantees. No LLM provider,
Manim or MinIO is involved: the render backend and the object store are test doubles."""

import asyncio
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.artifacts.fingerprint import ArtifactSpecError, canonical_manim_spec, canonical_mermaid_source, compute_fingerprint
from app.artifacts.service import ArtifactService, RetryLimitReached
from app.artifacts.storage import MemoryObjectStore
from app.artifacts.fingerprint import video_object_key
from app.core.config import settings
from tests.postgres.fakes import FakeBackend, make_mp4

pytestmark = []

ALICE, BOB = uuid.uuid4(), uuid.uuid4()
CONV, CONV2, MSG, MSG2 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
SPEC = {"template": "binary_search", "title": "Binary search", "params": {"values": [1, 3, 5, 7], "target": 5}}


@pytest.fixture
def store():
    return MemoryObjectStore()


@pytest.fixture
def backend():
    return FakeBackend()


@pytest.fixture
def service(session_factory, store, backend):
    return ArtifactService(session_factory, store, backend)


async def reg(service, spec=SPEC, owner=ALICE, conv=CONV, msg=MSG, block="b1"):
    return await service.register(owner, conv, msg, "manim", spec, block_id=block)


# ------------------------------------------------------------------------------------------ fingerprints

def test_manim_canonicalisation_ignores_key_order_and_title_whitespace():
    a = canonical_manim_spec({"template": "t", "title": "  Hello   world ", "params": {"b": 1, "a": [1, 2]}}, 4000)
    b = canonical_manim_spec({"params": {"a": [1, 2], "b": 1}, "title": "Hello world", "template": "t",
                              "schema_version": "1.0"}, 4000)
    assert a == b


def test_fingerprint_changes_with_content_renderer_version_and_quality():
    base = canonical_manim_spec(SPEC, 4000)
    other = canonical_manim_spec({**SPEC, "params": {"values": [1, 3, 5], "target": 5}}, 4000)
    fp = compute_fingerprint("manim", base, "v1", "l")
    assert fp == compute_fingerprint("manim", base, "v1", "l")
    assert fp != compute_fingerprint("manim", other, "v1", "l")
    assert fp != compute_fingerprint("manim", base, "v2", "l")
    assert fp != compute_fingerprint("manim", base, "v1", "h")


def test_mermaid_fingerprint_is_stable_across_line_endings_and_trailing_space():
    a = canonical_mermaid_source("graph TD\r\n  A --> B  \r\n", 8000)
    b = canonical_mermaid_source("graph TD\n  A --> B", 8000)
    assert a == b and compute_fingerprint("mermaid", a, "x", "y") == compute_fingerprint("mermaid", b)


@pytest.mark.parametrize("bad", ["not an object", {"template": "t", "evil": 1}, {"title": "no template"},
                                 {"template": "t", "params": []}])
def test_unusable_specs_are_rejected(bad):
    with pytest.raises(ArtifactSpecError):
        canonical_manim_spec(bad, 4000)


# ------------------------------------------------------------------------------------------ registration

@pytest.mark.asyncio
async def test_registering_mermaid_is_ready_immediately_and_never_renders(service, backend):
    r1 = await service.register(ALICE, CONV, MSG, "mermaid", "graph TD\n  A --> B")
    r2 = await service.register(ALICE, CONV, MSG, "mermaid", "graph TD\r\n  A --> B  ")
    assert r1.created and not r2.created
    assert r1.artifact.artifact_id == r2.artifact.artifact_id
    assert r1.artifact.status == "ready" and r1.artifact.object_key is None
    assert backend.submits == []


@pytest.mark.asyncio
async def test_registration_alone_never_renders_and_is_idempotent(service, backend):
    first, second = await reg(service), await reg(service)
    assert first.created and not second.created
    assert first.artifact.artifact_id == second.artifact.artifact_id
    assert second.artifact.status == "pending"
    assert backend.submits == []                       # registering is not rendering


@pytest.mark.asyncio
async def test_new_artifact_goes_pending_to_ready_and_stores_one_verified_video(service, backend, store):
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    done = await service.get(ALICE, a.artifact_id)
    assert done.status == "ready"
    assert done.object_key == video_object_key(a.artifact_id)
    assert done.size_bytes == len(backend.media) and len(done.checksum_sha256) == 64
    assert done.mime_type == "video/mp4"
    assert store.objects[done.object_key][2]["sha256"] == done.checksum_sha256
    assert len(backend.submits) == 1 and store.puts_ending('.mp4') == 1


@pytest.mark.asyncio
async def test_reopening_a_conversation_repeatedly_reuses_the_ready_artifact(service, backend, store):
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    for _ in range(5):                                  # every open of the chat re-registers the same block
        again = await reg(service)
        assert not again.created and again.artifact.status == "ready"
        await service.advance(a.artifact_id)            # even a stray wake-up must not render
    assert len(backend.submits) == 1 and store.puts_ending('.mp4') == 1 and backend.downloads == 1


@pytest.mark.asyncio
async def test_the_same_animation_in_another_message_and_conversation_is_one_artifact_with_two_refs(service, backend):
    a = (await reg(service)).artifact
    b = (await reg(service, conv=CONV2, msg=MSG2, block="x")).artifact
    assert a.artifact_id == b.artifact_id
    rows = await service.list_for_conversations(ALICE, [CONV, CONV2])
    assert sorted(r.conversation_id for _, r in rows) == sorted([CONV, CONV2])
    assert len({art.artifact_id for art, _ in rows}) == 1


@pytest.mark.asyncio
async def test_changed_source_renderer_version_or_owner_creates_a_distinct_artifact(service, backend):
    base = (await reg(service)).artifact
    changed = (await reg(service, spec={**SPEC, "params": {"values": [2, 4], "target": 4}})).artifact
    backend.version = "manim-templates-2"
    upgraded = (await reg(service)).artifact
    other_user = (await reg(service, owner=BOB)).artifact
    assert len({base.artifact_id, changed.artifact_id, upgraded.artifact_id, other_user.artifact_id}) == 4


@pytest.mark.asyncio
async def test_other_users_cannot_see_an_artifact_and_do_not_share_it(service):
    a = (await reg(service)).artifact
    assert await service.get(BOB, a.artifact_id) is None
    assert await service.list_for_conversations(BOB, [CONV]) == []
    b = (await reg(service, owner=BOB)).artifact                 # identical content, different tenant: own row
    assert b.artifact_id != a.artifact_id


# ------------------------------------------------------------------------------------------ concurrency

@pytest.mark.asyncio
async def test_concurrent_identical_registrations_create_exactly_one_row(service, session_factory):
    results = await asyncio.gather(*[reg(service) for _ in range(12)])
    assert sum(r.created for r in results) == 1
    assert len({r.artifact.artifact_id for r in results}) == 1
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM artifacts"))).scalar_one() == 1
        assert (await s.execute(text("SELECT count(*) FROM artifact_refs"))).scalar_one() == 1


@pytest.mark.asyncio
async def test_concurrent_advances_and_duplicate_deliveries_render_once(service, backend, store):
    a = (await reg(service)).artifact
    await asyncio.gather(*[service.advance(a.artifact_id) for _ in range(10)])
    await asyncio.gather(*[service.advance(a.artifact_id) for _ in range(10)])   # duplicate deliveries after the fact
    assert (await service.get(ALICE, a.artifact_id)).status == "ready"
    assert len(backend.submits) == 1 and store.puts_ending('.mp4') == 1


@pytest.mark.asyncio
async def test_a_second_worker_cannot_advance_while_the_lease_is_held(service, backend, session_factory):
    a = (await reg(service)).artifact
    claimed = await service._claim(a.artifact_id)
    assert claimed is not None
    assert await service._claim(a.artifact_id) is None
    await service.advance(a.artifact_id)                 # blocked by the lease: nothing happens
    assert backend.submits == []


# ------------------------------------------------------------------------------------------ lifecycle / failure / recovery

@pytest.mark.asyncio
async def test_statuses_follow_the_renderer_and_stay_queued_while_it_works(service, backend):
    backend.auto_ready = False
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    row = await service.get(ALICE, a.artifact_id)
    assert row.status in ("queued", "rendering") and row.render_job_id
    backend.states[row.render_job_id] = "rendering"
    await service.advance(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "rendering"
    backend.states[row.render_job_id] = "ready"
    await service.advance(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "ready"
    assert len(backend.submits) == 1


@pytest.mark.asyncio
async def test_crash_after_upload_before_db_update_adopts_the_object_without_rerendering(service, backend, store):
    a = (await reg(service)).artifact
    store.put_bytes(video_object_key(a.artifact_id), make_mp4(5000), "video/mp4")     # uploaded, row still `pending`
    await service.advance(a.artifact_id)
    done = await service.get(ALICE, a.artifact_id)
    assert done.status == "ready" and done.size_bytes == len(make_mp4(5000))
    assert backend.submits == [] and store.puts_ending('.mp4') == 0


@pytest.mark.asyncio
async def test_a_corrupt_leftover_object_is_deleted_and_the_video_is_rendered_normally(service, backend, store):
    a = (await reg(service)).artifact
    store.put_bytes(video_object_key(a.artifact_id), b"\x00" * 3000, "video/mp4")
    await service.advance(a.artifact_id)
    done = await service.get(ALICE, a.artifact_id)
    assert done.status == "ready" and len(backend.submits) == 1
    assert store.objects[done.object_key][0] == backend.media


@pytest.mark.asyncio
async def test_an_invalid_rendered_file_is_never_marked_ready(service, backend, store):
    backend.media = b"definitely not an mp4" * 100
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    row = await service.get(ALICE, a.artifact_id)
    assert row.status == "failed" and row.failure_code == "invalid_output" and row.object_key is None
    assert store.objects == {}


@pytest.mark.asyncio
async def test_a_permanent_rejection_fails_without_retry_loops(service, backend):
    backend.reject_with = "unsupported_template"
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    row = await service.get(ALICE, a.artifact_id)
    assert row.status == "failed" and row.failure_code == "unsupported_template"
    assert row.attempt_count == 0


@pytest.mark.asyncio
async def test_renderer_outage_keeps_the_artifact_pending_and_does_not_burn_an_attempt(service, backend):
    backend.unavailable = True
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    row = await service.get(ALICE, a.artifact_id)
    assert row.status == "pending" and row.attempt_count == 0 and row.lease_until is not None   # backing off
    backend.unavailable = False


@pytest.mark.asyncio
async def test_a_lost_render_job_is_resubmitted_a_bounded_number_of_times(service, backend):
    backend.auto_ready = False
    a = (await reg(service)).artifact
    for _ in range(settings.ARTIFACT_MAX_AUTO_ATTEMPTS + 2):
        backend.forget_jobs = False
        await service.advance(a.artifact_id)             # submit
        backend.forget_jobs = True
        await service.advance(a.artifact_id)             # job vanished -> pending again (or failed when exhausted)
        await service._release(a.artifact_id)
    row = await service.get(ALICE, a.artifact_id)
    assert row.status == "failed" and row.failure_code == "render_job_lost"
    assert len(backend.submits) == settings.ARTIFACT_MAX_AUTO_ATTEMPTS


@pytest.mark.asyncio
async def test_explicit_retry_restarts_a_failed_render_once_and_is_bounded(service, backend):
    backend.reject_with = "render_error"
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "failed"
    backend.reject_with = None
    r1, restarted1 = await service.retry(ALICE, a.artifact_id)
    r2, restarted2 = await service.retry(ALICE, a.artifact_id)      # double click: second is a no-op
    assert restarted1 and not restarted2 and r1.artifact_id == r2.artifact_id == a.artifact_id
    await service.advance(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "ready"
    for _ in range(settings.ARTIFACT_MAX_MANUAL_RETRIES):          # exhaust the manual budget on a stubborn failure
        backend.reject_with = "render_error"
        async with service.sf() as s, s.begin():
            await s.execute(text("UPDATE artifacts SET status='failed', failure_code='render_error', object_key=NULL, "
                                 "checksum_sha256=NULL, size_bytes=NULL WHERE artifact_id=:i"), {"i": a.artifact_id})
        try:
            await service.retry(ALICE, a.artifact_id)
        except RetryLimitReached:
            break
    else:
        pytest.fail("retries were not bounded")


@pytest.mark.asyncio
async def test_retry_is_owner_scoped(service, backend):
    backend.reject_with = "render_error"
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    from app.artifacts.service import ArtifactNotFound
    with pytest.raises(ArtifactNotFound):
        await service.retry(BOB, a.artifact_id)


@pytest.mark.asyncio
async def test_a_vanished_stored_video_is_reconciled_and_restored(service, backend, store):
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    ready = await service.get(ALICE, a.artifact_id)
    del store.objects[ready.object_key]                           # the object disappears from storage
    await service.mark_object_missing(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "pending"
    await service.advance(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "ready"


@pytest.mark.asyncio
async def test_orphan_objects_are_removed_but_registered_ones_are_kept(service, backend, store):
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    orphan = f"artifacts/{uuid.uuid4()}/video.mp4"
    store.put_bytes(orphan, make_mp4())
    assert await service.delete_orphan_objects(grace_seconds=-1) == 1
    assert orphan not in store.objects and video_object_key(a.artifact_id) in store.objects


@pytest.mark.asyncio
async def test_run_once_picks_up_everything_a_crashed_process_left_behind(service, backend):
    ids = [(await reg(service, spec={**SPEC, "title": f"t{i}"})).artifact.artifact_id for i in range(3)]
    assert await service.run_once() == 3
    for i in ids:
        assert (await service.get(ALICE, i)).status == "ready"
    assert await service.run_once() == 0


# ------------------------------------------------------------------------------------------ database guarantees

@pytest.mark.asyncio
async def test_database_rejects_a_duplicate_owner_type_fingerprint_even_if_the_app_is_bypassed(service, session_factory):
    a = (await reg(service)).artifact
    async with session_factory() as s:
        with pytest.raises(IntegrityError):
            await s.execute(text(
                "INSERT INTO artifacts (artifact_id, owner_user_id, artifact_type, fingerprint, spec_text, "
                "origin_conversation_id) VALUES (:i, :o, 'manim', :f, '{}', :c)"),
                {"i": uuid.uuid4(), "o": ALICE, "f": a.fingerprint, "c": CONV})
            await s.commit()


@pytest.mark.asyncio
async def test_database_refuses_a_ready_video_without_a_verified_object(service, session_factory):
    a = (await reg(service)).artifact
    async with session_factory() as s:
        with pytest.raises(IntegrityError):
            await s.execute(text("UPDATE artifacts SET status='ready' WHERE artifact_id=:i"), {"i": a.artifact_id})
            await s.commit()


@pytest.mark.asyncio
async def test_database_rejects_bad_status_and_type(service, session_factory):
    a = (await reg(service)).artifact
    for stmt in ("UPDATE artifacts SET status='bogus' WHERE artifact_id=:i",
                 "UPDATE artifacts SET artifact_type='gif' WHERE artifact_id=:i"):
        async with session_factory() as s:
            with pytest.raises(IntegrityError):
                await s.execute(text(stmt), {"i": a.artifact_id})
                await s.commit()


@pytest.mark.asyncio
async def test_listing_supports_type_and_title_filters_without_wildcard_injection(service):
    await service.register(ALICE, CONV, MSG, "mermaid", "graph TD\n  A --> B", title="Login flow")
    await reg(service)
    assert len(await service.list_for_conversations(ALICE, [CONV])) == 2
    assert [a.artifact_type for a, _ in await service.list_for_conversations(ALICE, [CONV], artifact_type="mermaid")] == ["mermaid"]
    assert len(await service.list_for_conversations(ALICE, [CONV], query="login")) == 1
    assert await service.list_for_conversations(ALICE, [CONV], query="%") == []      # `%` is literal, not a wildcard


@pytest.mark.asyncio
async def test_a_preview_thumbnail_is_stored_beside_the_video_when_the_renderer_made_one(service, backend, store):
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    done = await service.get(ALICE, a.artifact_id)
    assert done.thumbnail_key == f"artifacts/{a.artifact_id}/thumb.png"
    assert store.objects[done.thumbnail_key][0] == backend.thumbnail and store.objects[done.thumbnail_key][1] == "image/png"


@pytest.mark.asyncio
async def test_a_missing_or_bogus_thumbnail_never_blocks_or_fails_the_video(service, backend, store):
    backend.thumbnail = None
    a = (await reg(service)).artifact
    await service.advance(a.artifact_id)
    assert (await service.get(ALICE, a.artifact_id)).status == "ready"
    assert (await service.get(ALICE, a.artifact_id)).thumbnail_key is None
    backend.thumbnail = b"<html>not a png</html>"
    b = (await reg(service, spec={**SPEC, "title": "second"})).artifact
    await service.advance(b.artifact_id)
    done = await service.get(ALICE, b.artifact_id)
    assert done.status == "ready" and done.thumbnail_key is None
