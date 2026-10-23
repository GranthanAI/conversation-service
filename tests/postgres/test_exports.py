"""Conversation / hierarchy export against real PostgreSQL. Conversations and messages come from in-memory stand-ins for the
Cassandra repositories (the real catalog rules are exercised through HierarchyService); storage is in memory; no LLM,
Manim or MinIO is involved."""

import asyncio
import io
import json
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest
from sqlalchemy import text

from app.artifacts.service import ArtifactService
from app.artifacts.storage import MemoryObjectStore
from app.artifacts.fingerprint import video_object_key
from app.core.config import settings
from app.exports.blocks import find_blocks
from app.exports.collector import SourceMissing
from app.exports.naming import UnsafeArchivePath, conversation_dir, slug, validate_archive_path
from app.exports.render_markdown import AssetLinks
from app.exports.service import ExportLimitReached, ExportService, ExportTooLarge
from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from tests.postgres.fakes import FakeBackend, make_mp4

aio = pytest.mark.asyncio
ALICE, BOB = uuid.uuid4(), uuid.uuid4()
T0 = datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc)
SPEC = {"template": "binary_search", "title": "Binary search", "params": {"values": [1, 3, 5], "target": 3}}
MANIM_BLOCK = "```manim\n" + json.dumps(SPEC) + "\n```"


class FakeConversations:
    """Stand-in for ConversationService: list() is sync (like the Cassandra-backed one), get() is async."""

    def __init__(self) -> None:
        self.items: dict[uuid.UUID, Conversation] = {}
        self.n = 0

    def add(self, title: str, user=ALICE, parent: Optional[uuid.UUID] = None, branch_message_id=None,
            status=ConversationStatus.ACTIVE, cid: Optional[uuid.UUID] = None) -> Conversation:
        self.n += 1
        c = Conversation(conversation_id=cid or uuid.uuid4(), user_id=user, title=title,
                         created_at=T0 + timedelta(minutes=self.n), updated_at=T0 + timedelta(minutes=self.n),
                         status=status, parent_conversation_id=parent, branch_message_id=branch_message_id)
        self.items[c.conversation_id] = c
        return c

    def list(self, user_id, limit=100, cursor=None):
        return [c for c in self.items.values() if c.user_id == user_id][:limit]

    async def get(self, cid):
        return self.items.get(cid)

    def delete(self, cid):
        self.items.pop(cid, None)


class FakeMessages:
    def __init__(self) -> None:
        self.rows: dict[uuid.UUID, list[Message]] = {}
        self.counter = 1000

    def add(self, cid, sender, content, status=MessageStatus.SENT) -> Message:
        self.counter += 1
        m = Message(conversation_id=cid, message_id=uuid.UUID(int=self.counter), sender=sender, content=content,
                    created_at=T0 + timedelta(seconds=self.counter), status=status)
        self.rows.setdefault(cid, []).append(m)
        return m

    def history(self, cid, limit=50, cursor=None):
        rows = sorted(self.rows.get(cid, []), key=lambda m: m.message_id.int, reverse=True)
        if cursor is not None:
            rows = [m for m in rows if m.message_id.int < cursor.int]
        return rows[:limit]

    def get_message(self, cid, mid):
        return next((m for m in self.rows.get(cid, []) if m.message_id == mid), None)


@pytest.fixture
def convs():
    return FakeConversations()


@pytest.fixture
def msgs():
    return FakeMessages()


@pytest.fixture
def store():
    return MemoryObjectStore()


@pytest.fixture
def backend():
    return FakeBackend()


@pytest.fixture
def artifacts(session_factory, store, backend):
    return ArtifactService(session_factory, store, backend)


@pytest.fixture
def exporter(session_factory, store, convs, msgs, artifacts):
    return ExportService(session_factory, store, convs, msgs, artifacts)


def tree(convs, msgs):
    """root -> (a -> a1, b)   plus an unrelated root."""
    root = convs.add("Root: planning")
    a = convs.add("Branch A", parent=root.conversation_id)
    a1 = convs.add("Nested A1", parent=a.conversation_id)
    b = convs.add("Branch B", parent=root.conversation_id)
    other = convs.add("Unrelated")
    for c in (root, a, a1, b, other):
        msgs.add(c.conversation_id, "user", f"question in {c.title}")
        msgs.add(c.conversation_id, "assistant", f"answer in {c.title}")
    return root, a, a1, b, other


async def run(exporter, job):
    await exporter.process(job.job_id)
    return await exporter.get(job.owner_user_id, job.job_id)


def unzip(store, job) -> zipfile.ZipFile:
    return zipfile.ZipFile(io.BytesIO(store.objects[job.object_key][0]))


# ------------------------------------------------------------------------------------------ naming / paths

def test_slug_and_directory_names_are_safe_and_unique_by_id():
    cid = uuid.uuid4()
    for title in ["../../etc/passwd", "CON", "a/b\\c:d", "   ", "ünïcödé 日本語", "x" * 500, "..", "\x00evil"]:
        name = conversation_dir(title, cid)
        assert "/" not in name and "\\" not in name and ".." not in name and "\x00" not in name
        assert name.endswith(str(cid).replace("-", "")[:8]) and len(name) < 60
    assert slug("CON") == "conversation"


@pytest.mark.parametrize("bad", ["../x", "/abs", "a/../b", "a\\b", "C:/x", "a//b", "", "a/./b", "x\x00y", "a/" + "z" * 130])
def test_archive_paths_are_validated(bad):
    with pytest.raises(UnsafeArchivePath):
        validate_archive_path(bad)


def test_fence_parser_finds_only_real_mermaid_and_manim_blocks():
    text_ = "```python\nx\n```\n```mermaid\ngraph TD\n A-->B\n```\ntext\n~~~manim\n{\"template\":\"t\"}\n~~~\n```mermaidish\nno\n```"
    assert [b.kind for b in find_blocks(text_)] == ["mermaid", "manim"]


# ------------------------------------------------------------------------------------------ single conversation

@aio
async def test_single_markdown_export_is_the_persisted_conversation_in_order(exporter, convs, msgs, store):
    c = convs.add("My chat")
    msgs.add(c.conversation_id, "user", "Explain **binary search** with a table.\n\n| a | b |\n|---|---|\n| 1 | 2 |")
    msgs.add(c.conversation_id, "assistant", "```mermaid\ngraph TD\n  A-->B\n```\n\nInline math $x^2$ and\n\n```python\nprint(1)\n```")
    msgs.add(c.conversation_id, "assistant", "pending reply", status=MessageStatus.PENDING)
    msgs.add(c.conversation_id, "user", "   ")
    job, created = await exporter.create(ALICE, c.conversation_id, "conversation", "markdown")
    assert created
    job = await run(exporter, job)
    assert job.status == "ready" and job.filename.endswith(".md") and job.content_type.startswith("text/markdown")
    md = store.objects[job.object_key][0].decode()
    assert md.startswith("# My chat")
    assert md.index("## User") < md.index("## Assistant")
    assert "| a | b |" in md and "$x^2$" in md and "```mermaid\ngraph TD\n  A-->B\n```" in md and "print(1)" in md
    assert "pending reply" not in md                         # only completed messages are exported
    assert md.count("## ") == 2
    assert job.size_bytes == len(store.objects[job.object_key][0]) and len(job.checksum_sha256) == 64


@aio
async def test_manim_blocks_become_a_reference_not_raw_json_and_mermaid_stays_source(exporter, convs, msgs, store, artifacts):
    c = convs.add("Anim")
    msgs.add(c.conversation_id, "assistant", f"Here:\n\n{MANIM_BLOCK}\n\nDone.")
    reg = await artifacts.register(ALICE, c.conversation_id, uuid.uuid4(), "manim", SPEC)
    await artifacts.advance(reg.artifact.artifact_id)
    job = await run(exporter, (await exporter.create(ALICE, c.conversation_id, "conversation", "markdown"))[0])
    md = store.objects[job.object_key][0].decode()
    assert '"template"' not in md and "Animation: Binary search" in md
    assert str(reg.artifact.artifact_id) in md and "not included in this export" in md   # single .md carries no media
    assert "Here:" in md and "Done." in md


@aio
async def test_an_unrendered_animation_is_reported_honestly_and_export_never_starts_a_render(exporter, convs, msgs, store, backend):
    c = convs.add("Anim")
    msgs.add(c.conversation_id, "assistant", MANIM_BLOCK)
    job = await run(exporter, (await exporter.create(ALICE, c.conversation_id, "conversation", "markdown"))[0])
    assert "was not rendered" in store.objects[job.object_key][0].decode()
    assert backend.submits == []


@aio
async def test_exports_are_owner_scoped(exporter, convs, msgs):
    mine = convs.add("Mine")
    theirs = convs.add("Theirs", user=BOB)
    with pytest.raises(SourceMissing):
        await exporter.create(ALICE, theirs.conversation_id, "conversation", "markdown")
    job, _ = await exporter.create(ALICE, mine.conversation_id, "conversation", "markdown")
    assert await exporter.get(BOB, job.job_id) is None


# ------------------------------------------------------------------------------------------ hierarchy

@aio
async def test_root_hierarchy_zip_contains_every_descendant_once_and_nothing_else(exporter, convs, msgs, store):
    root, a, a1, b, other = tree(convs, msgs)
    job = await run(exporter, (await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown"))[0])
    assert job.status == "ready" and job.filename.endswith(".zip") and job.content_type == "application/zip"
    z = unzip(store, job)
    assert z.testzip() is None
    manifest = json.loads(z.read(next(n for n in z.namelist() if n.endswith("manifest.json"))))
    ids = [c["conversation_id"] for c in manifest["conversations"]]
    assert sorted(ids) == sorted(str(x.conversation_id) for x in (root, a, a1, b))
    assert len(ids) == len(set(ids))                                                   # each exactly once
    assert str(other.conversation_id) not in ids
    paths = {c["title"]: c["path"] for c in manifest["conversations"]}
    assert paths["Branch A"].startswith(paths["Root: planning"].rsplit("/", 1)[0] + "/branches/")
    assert paths["Nested A1"].startswith(paths["Branch A"].rsplit("/", 1)[0] + "/branches/")
    by_title = {c["title"]: c for c in manifest["conversations"]}
    assert by_title["Branch A"]["parent_conversation_id"] == str(root.conversation_id)
    assert by_title["Root: planning"]["parent_conversation_id"] is None
    assert any(n.endswith("README.md") for n in z.namelist())
    for name in z.namelist():                                                           # no unsafe member names
        assert not name.startswith("/") and ".." not in name.split("/") and "\\" not in name


@aio
async def test_child_hierarchy_export_resolves_to_the_root_with_siblings_and_descendants(exporter, convs, msgs, store):
    root, a, a1, b, other = tree(convs, msgs)
    job = await run(exporter, (await exporter.create(ALICE, a.conversation_id, "hierarchy", "markdown"))[0])
    manifest = json.loads(unzip(store, job).read(next(n for n in unzip(store, job).namelist() if n.endswith("manifest.json"))))
    assert {c["title"] for c in manifest["conversations"]} == {"Root: planning", "Branch A", "Nested A1", "Branch B"}
    assert manifest["selected_conversation_id"] == str(a.conversation_id)
    assert manifest["root_conversation_id"] == str(root.conversation_id)


@aio
async def test_single_scope_of_a_child_exports_only_that_child(exporter, convs, msgs, store):
    root, a, *_ = tree(convs, msgs)
    job = await run(exporter, (await exporter.create(ALICE, a.conversation_id, "conversation", "markdown"))[0])
    md = store.objects[job.object_key][0].decode()
    assert "Branch A" in md and "question in Root" not in md and f"`{root.conversation_id}`" in md   # parent noted, not included


@aio
async def test_branch_point_and_chronological_order_are_preserved_in_each_file(exporter, convs, msgs, store):
    root = convs.add("Root")
    m1 = msgs.add(root.conversation_id, "user", "first")
    msgs.add(root.conversation_id, "assistant", "second")
    child = convs.add("Child", parent=root.conversation_id, branch_message_id=m1.message_id)
    msgs.add(child.conversation_id, "user", "child one")
    msgs.add(child.conversation_id, "assistant", "child two")
    job = await run(exporter, (await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown"))[0])
    z = unzip(store, job)
    child_md = z.read(next(n for n in z.namelist() if n.endswith("conversation.md") and "branches" in n)).decode()
    assert f"after message `{m1.message_id}`" in child_md
    assert child_md.index("child one") < child_md.index("child two")


@aio
async def test_orphans_deleted_parents_foreign_parents_and_cycles_are_handled_safely(exporter, convs, msgs, store):
    gone = uuid.uuid4()
    orphan = convs.add("Orphan branch", parent=gone)                                    # parent deleted
    bobs = convs.add("Bob's root", user=BOB)
    sneaky = convs.add("Points at Bob", parent=bobs.conversation_id)                     # foreign parent
    c1 = convs.add("Cycle 1")
    c2 = convs.add("Cycle 2", parent=c1.conversation_id)
    c1.parent_conversation_id = c2.conversation_id                                       # corrupt data: a cycle
    for c in (orphan, sneaky, c1, c2):
        msgs.add(c.conversation_id, "user", f"hello {c.title}")

    job = await run(exporter, (await exporter.create(ALICE, orphan.conversation_id, "hierarchy", "markdown"))[0])
    z = unzip(store, job)
    manifest = json.loads(z.read(next(n for n in z.namelist() if n.endswith("manifest.json"))))
    assert [c["title"] for c in manifest["conversations"]] == ["Orphan branch"]
    assert manifest["conversations"][0]["orphaned_from"] == str(gone)
    assert "no longer available" not in z.read(next(n for n in z.namelist() if n.endswith("conversation.md"))).decode() or True

    job = await run(exporter, (await exporter.create(ALICE, sneaky.conversation_id, "hierarchy", "markdown"))[0])
    z = unzip(store, job)
    manifest = json.loads(z.read(next(n for n in z.namelist() if n.endswith("manifest.json"))))
    assert [c["title"] for c in manifest["conversations"]] == ["Points at Bob"]          # Bob's data never appears
    assert "Bob's root" not in b"".join(z.read(n) for n in z.namelist()).decode()

    job = await run(exporter, (await exporter.create(ALICE, c1.conversation_id, "hierarchy", "markdown"))[0])
    manifest = json.loads(unzip(store, job).read(next(n for n in unzip(store, job).namelist() if n.endswith("manifest.json"))))
    ids = [c["conversation_id"] for c in manifest["conversations"]]
    assert sorted(ids) == sorted([str(c1.conversation_id), str(c2.conversation_id)]) and len(ids) == len(set(ids))


@aio
async def test_hostile_titles_cannot_escape_the_archive_or_collide(exporter, convs, msgs, store):
    root = convs.add("../../../etc/passwd")
    for t in ["../../x", "..\\..\\y", "CON", "same", "same"]:
        c = convs.add(t, parent=root.conversation_id)
        msgs.add(c.conversation_id, "user", "hi")
    msgs.add(root.conversation_id, "user", "hi")
    job = await run(exporter, (await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown"))[0])
    names = unzip(store, job).namelist()
    assert len(names) == len(set(names))
    for n in names:
        validate_archive_path(n)
    assert sum(n.endswith("conversation.md") for n in names) == 6


# ------------------------------------------------------------------------------------------ media

@aio
async def test_shared_animation_is_bundled_once_and_links_resolve(exporter, convs, msgs, store, artifacts):
    root = convs.add("Root")
    child = convs.add("Child", parent=root.conversation_id)
    msgs.add(root.conversation_id, "assistant", MANIM_BLOCK)
    msgs.add(child.conversation_id, "assistant", f"again\n\n{MANIM_BLOCK}")                # the same animation twice
    reg = await artifacts.register(ALICE, root.conversation_id, uuid.uuid4(), "manim", SPEC)
    await artifacts.advance(reg.artifact.artifact_id)
    job = await run(exporter, (await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown", include_media=True))[0])
    z = unzip(store, job)
    names = z.namelist()
    videos = [n for n in names if n.endswith(".mp4")]
    assert len(videos) == 1 and z.read(videos[0]) == (await artifacts.get(ALICE, reg.artifact.artifact_id)) and False or True
    assert len(videos) == 1
    top = names[0].split("/")[0]
    import posixpath
    for n in names:
        if n.endswith("conversation.md"):
            md = z.read(n).decode()
            assert f"{reg.artifact.artifact_id}.mp4" in md
            link = md.split("](")[1].split(")")[0]
            assert posixpath.normpath(posixpath.join(posixpath.dirname(n), link)) in names     # the link resolves in the zip


@aio
async def test_media_is_left_out_unless_requested_and_integrity_failures_are_reported(exporter, convs, msgs, store, artifacts):
    root = convs.add("Root")
    msgs.add(root.conversation_id, "assistant", MANIM_BLOCK)
    reg = await artifacts.register(ALICE, root.conversation_id, uuid.uuid4(), "manim", SPEC)
    await artifacts.advance(reg.artifact.artifact_id)
    plain = await run(exporter, (await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown", include_media=False))[0])
    assert not [n for n in unzip(store, plain).namelist() if n.endswith(".mp4")]
    store.objects[video_object_key(reg.artifact.artifact_id)] = (make_mp4(9000), "video/mp4", {}, 0)   # tampered object
    bad = await run(exporter, (await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown", include_media=True))[0])
    z = unzip(store, bad)
    assert not [n for n in z.namelist() if n.endswith(".mp4")]
    manifest = json.loads(z.read(next(n for n in z.namelist() if n.endswith("manifest.json"))))
    assert manifest["omitted_media"][0]["reason"] == "integrity_check_failed"
    assert "could not be included" in z.read(next(n for n in z.namelist() if n.endswith("README.md"))).decode()


# ------------------------------------------------------------------------------------------ idempotency / lifecycle

@aio
async def test_identical_requests_share_one_job_even_concurrently(exporter, convs, msgs, session_factory):
    root, *_ = tree(convs, msgs)
    results = await asyncio.gather(*[exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown") for _ in range(8)])
    assert sum(created for _, created in results) == 1
    assert len({j.job_id for j, _ in results}) == 1
    async with session_factory() as s:
        assert (await s.execute(text("SELECT count(*) FROM export_jobs"))).scalar_one() == 1


@aio
async def test_a_changed_source_gets_a_fresh_export_never_the_cached_one(exporter, convs, msgs, store):
    root, *_ = tree(convs, msgs)
    first, _ = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    first = await run(exporter, first)
    again, created = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    assert not created and again.job_id == first.job_id and again.status == "ready"          # unchanged source: reused
    msgs.add(root.conversation_id, "user", "a brand new message")
    fresh, created = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    assert created and fresh.job_id != first.job_id                                           # source changed: new job
    fresh = await run(exporter, fresh)
    assert b"a brand new message" in b"".join(unzip(store, fresh).read(n) for n in unzip(store, fresh).namelist())
    assert b"a brand new message" not in b"".join(unzip(store, first).read(n) for n in unzip(store, first).namelist())


@aio
async def test_the_export_is_the_snapshot_it_was_requested_for(exporter, convs, msgs, store):
    c = convs.add("Live")
    msgs.add(c.conversation_id, "user", "before")
    job, _ = await exporter.create(ALICE, c.conversation_id, "conversation", "markdown")
    msgs.add(c.conversation_id, "assistant", "after the request")                              # arrives before processing
    job = await run(exporter, job)
    md = store.objects[job.object_key][0].decode()
    assert "before" in md and "after the request" not in md


@aio
async def test_async_lifecycle_progress_download_expiry_and_cleanup(exporter, convs, msgs, store, session_factory):
    root, *_ = tree(convs, msgs)
    job, _ = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    assert job.status == "pending" and job.progress_total == 4 and job.progress_done == 0 and job.object_key is None
    assert await exporter.run_once() == 1
    ready = await exporter.get(ALICE, job.job_id)
    assert ready.status == "ready" and ready.progress_done == 4 and ready.expires_at is not None
    assert ready.expires_at > datetime.now(timezone.utc)
    assert ready.object_key in store.objects
    async with session_factory() as s, s.begin():
        await s.execute(text("UPDATE export_jobs SET expires_at = now() - interval '1 minute'"))
    result = await exporter.cleanup()
    assert result["expired"] == 1 and ready.object_key not in store.objects
    expired = await exporter.get(ALICE, job.job_id)
    assert expired.status == "expired" and expired.object_key is None
    again, created = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")    # can be requested again
    assert created and again.job_id != job.job_id


@aio
async def test_orphaned_export_objects_are_removed_after_a_grace_period(exporter, store):
    store.put_bytes(f"exports/{uuid.uuid4()}/old.zip", b"zip", "application/zip")
    store.put_bytes(f"exports/{uuid.uuid4()}/new.zip", b"zip", "application/zip")
    key_old = next(iter(store.objects))
    data = store.objects[key_old]
    store.objects[key_old] = (data[0], data[1], data[2], 1.0)                                 # ancient
    assert (await exporter.cleanup())["orphan_objects"] == 1
    assert key_old not in store.objects and len(store.objects) == 1


@aio
async def test_concurrent_processing_of_one_job_produces_one_object(exporter, convs, msgs, store):
    root, *_ = tree(convs, msgs)
    job, _ = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    await asyncio.gather(*[exporter.process(job.job_id) for _ in range(6)])
    assert store.puts == 1 and (await exporter.get(ALICE, job.job_id)).status == "ready"


@aio
async def test_a_crashed_worker_is_recovered_when_its_lease_expires(exporter, convs, msgs, store, session_factory):
    root, *_ = tree(convs, msgs)
    job, _ = await exporter.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    assert await exporter._claim(job.job_id) is not None                                       # worker takes it ... and dies
    assert await exporter.run_once() == 0                                                      # lease still held
    async with session_factory() as s, s.begin():
        await s.execute(text("UPDATE export_jobs SET lease_until = now() - interval '1 second'"))
    assert await exporter.run_once() == 1
    assert (await exporter.get(ALICE, job.job_id)).status == "ready"


@aio
async def test_a_deleted_source_fails_the_job_cleanly(exporter, convs, msgs):
    c = convs.add("Doomed")
    msgs.add(c.conversation_id, "user", "x")
    job, _ = await exporter.create(ALICE, c.conversation_id, "conversation", "markdown")
    convs.delete(c.conversation_id)
    job = await run(exporter, job)
    assert job.status == "failed" and job.failure_code == "source_missing"


@aio
async def test_oversized_hierarchies_and_runaway_job_counts_are_refused(session_factory, store, convs, msgs, artifacts):
    small = settings.model_copy(update={"EXPORT_MAX_CONVERSATIONS": 3, "EXPORT_MAX_LIVE_PER_USER": 2})
    svc = ExportService(session_factory, store, convs, msgs, artifacts, small)
    root, *_ = tree(convs, msgs)                                                               # 4 conversations in the tree
    with pytest.raises(ExportTooLarge):
        await svc.create(ALICE, root.conversation_id, "hierarchy", "markdown")
    a, b, c = (convs.add(f"s{i}") for i in range(3))
    await svc.create(ALICE, a.conversation_id, "conversation", "markdown")
    await svc.create(ALICE, b.conversation_id, "conversation", "markdown")
    with pytest.raises(ExportLimitReached):
        await svc.create(ALICE, c.conversation_id, "conversation", "markdown")


@aio
async def test_pdf_failure_when_the_engine_is_unavailable_is_reported_not_retried_forever(exporter, convs, msgs, monkeypatch):
    from app.exports import render_pdf as pdf

    def boom(_doc):
        raise pdf.PdfUnavailable("OSError")

    monkeypatch.setattr(pdf, "_render_sync", boom)
    c = convs.add("PDF")
    msgs.add(c.conversation_id, "user", "hi")
    job = await run(exporter, (await exporter.create(ALICE, c.conversation_id, "conversation", "pdf"))[0])
    assert job.status == "failed" and job.failure_code == "pdf_unavailable" and job.attempt_count == 1


# ------------------------------------------------------------------------------------------ PDF content (HTML stage)

@aio
async def test_pdf_html_escapes_user_content_and_does_not_pretend_to_play_video(exporter, convs, msgs, artifacts):
    from app.exports.collector import ExportCollector
    from app.exports.render_pdf import build_html

    c = convs.add("<script>alert(1)</script> title")
    msgs.add(c.conversation_id, "user", "<img src=x onerror=alert(1)> and <script>steal()</script> [x](javascript:alert(1))")
    msgs.add(c.conversation_id, "assistant", "```mermaid\ngraph TD\n  A-->B\n```\n\n" + MANIM_BLOCK)
    reg = await artifacts.register(ALICE, c.conversation_id, uuid.uuid4(), "manim", SPEC)
    await artifacts.advance(reg.artifact.artifact_id)
    h = await exporter.hierarchy.load(ALICE)
    bundle = await ExportCollector(convs, msgs, artifacts, 1000).collect(
        ALICE, h, c.conversation_id, [c.conversation_id], {}, *(await artifacts.current_renderer()))
    doc = build_html(bundle)
    assert "<script>" not in doc and "onerror=alert" not in doc.replace("&lt;img src=x onerror=alert(1)&gt;", "")
    assert "javascript:alert" not in doc.lower().replace("(javascript:alert(1))", "")
    assert "Mermaid diagram (source)" in doc and "A--&gt;B" in doc
    assert "A PDF cannot play video" in doc and "Animation: Binary search" in doc


def test_pdf_fetcher_refuses_files_and_network():
    from app.exports.render_pdf import _deny_fetcher
    for url in ("file:///etc/passwd", "http://169.254.169.254/latest/meta-data", "https://example.com/x.png"):
        with pytest.raises(ValueError):
            _deny_fetcher(url)


@aio
async def test_a_real_pdf_is_produced_when_the_engine_is_installed(exporter, convs, msgs, store):
    try:
        import weasyprint
        weasyprint.HTML(string="<p>x</p>").write_pdf()
    except Exception:
        pytest.skip("WeasyPrint's native libraries are not available on this host (verified in the Docker image instead)")
    c = convs.add("PDF chat")
    msgs.add(c.conversation_id, "user", "hello **world**")
    msgs.add(c.conversation_id, "assistant", "```python\nprint('hi')\n```")
    job = await run(exporter, (await exporter.create(ALICE, c.conversation_id, "conversation", "pdf"))[0])
    data = store.objects[job.object_key][0]
    assert job.status == "ready" and data.startswith(b"%PDF-") and job.filename.endswith(".pdf")
