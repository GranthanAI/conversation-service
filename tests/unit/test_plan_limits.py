"""P6c: service-owned plan limits. Each limit refuses at the boundary and allows at boundary - 1; -1 never refuses; a
downgrade keeps old data readable and refuses only new items. Counters / documents / custom modes run against real SQL
in a throwaway database inside the shared dev Postgres."""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.clients.billing_client import EntitlementsData
from app.core.billing_messages import PlanLimitError, plan_limit_message
from app.core.config import settings
from app.core.plan_defaults import FREE_LIMITS
from app.models.conversation import Conversation, ConversationStatus
from app.models.custom_modes import CustomModeCreate, CustomModeUpdate
from app.models.preferences import PreferencesUpdate
from app.repositories.pg_repositories import CustomModeLimitError, CustomModeRepository, PreferencesRepository
from app.services.custom_mode_service import CustomModeService
from app.services.plan_limits import PlanLimitService
from app.services.preferences_service import PreferencesService
from app.telemetry import metrics
from tests.support.shared_pg import migrated_pg_url  # noqa: F401  (fixture)

pytestmark = pytest.mark.anyio


class FakeEntitlements:
    def __init__(self, plan="free", **limits):
        self.ent = EntitlementsData(user_id="u", plan_code=plan, limits={**FREE_LIMITS, **limits})
        self.calls = 0

    async def get(self, user_id):
        self.calls += 1
        return self.ent


class FakeMessageRepo:
    def __init__(self, existing=0):
        self.existing, self.scans = existing, 0

    def count_user_messages(self, conversation_id):
        self.scans += 1
        return self.existing


class FakeConversations:
    """ConversationService stand-in: get() by id and list() of a user's catalog."""

    def __init__(self):
        self.items = {}

    def add(self, user_id, parent=None, status=ConversationStatus.ACTIVE):
        cid = uuid.uuid4()
        now = datetime.now(timezone.utc)
        self.items[cid] = Conversation(conversation_id=cid, user_id=user_id, title="t", created_at=now, updated_at=now,
                                       status=status, parent_conversation_id=parent)
        return cid

    async def get(self, cid):
        return self.items.get(cid)

    def list(self, user_id, limit=20, cursor=None):
        return [c for c in self.items.values() if c.user_id == user_id and c.status != ConversationStatus.DELETED][:limit]


@pytest.fixture
def enforce(monkeypatch):
    monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", "enforce")


@pytest.fixture
async def sf(migrated_pg_url):
    eng = create_async_engine(migrated_pg_url)
    yield async_sessionmaker(bind=eng, expire_on_commit=False, autoflush=False)
    await eng.dispose()


async def send(sf, ent, repo, convs, user, cid, files=None):
    """One send's limit handling in its own request transaction (check, then record; commit at the end)."""
    async with sf() as s:
        svc = PlanLimitService(ent, session=s, message_repo=repo, conversations=convs)
        await svc.check_send(user, cid, files)
        await svc.record_send(user, cid, files)
        await s.commit()


def assert_plan_limit(exc: PlanLimitError, limit: str, upgrade_to):
    assert exc.status_code == 403
    assert exc.detail == {"code": "billing_denied", "reason": "plan_limit", "limit": limit,
                          "message_key": f"limit.{limit}", "message": plan_limit_message(limit), "upgrade_to": upgrade_to}


def test_message_copy():
    assert plan_limit_message("branches_per_tree") == "Branch limit for your plan reached. Upgrade to add more."


# ---- messages_per_conversation -------------------------------------------------------------------------------

async def test_messages_per_conversation_boundary(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(messages_per_conversation=3), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    cid = convs.add(user)
    before = metrics.plan_limit_denied_total.get(limit="messages_per_conversation")
    for _ in range(3):                                   # sends 1, 2, 3 (count 0, 1, 2 -> allowed: boundary - 1)
        await send(sf, ent, repo, convs, user, cid)
    with pytest.raises(PlanLimitError) as e:             # the 4th (count == limit) is refused
        await send(sf, ent, repo, convs, user, cid)
    assert_plan_limit(e.value, "messages_per_conversation", "pro")
    assert metrics.plan_limit_denied_total.get(limit="messages_per_conversation") == before + 1
    assert repo.scans == 1                               # one Cassandra count, ever


async def test_counter_is_initialised_lazily_from_existing_messages(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(messages_per_conversation=3), FakeMessageRepo(existing=2), FakeConversations(), uuid.uuid4()
    cid = convs.add(user)
    await send(sf, ent, repo, convs, user, cid)          # 2 existing + this one = 3
    with pytest.raises(PlanLimitError):
        await send(sf, ent, repo, convs, user, cid)


async def test_unlimited_messages_never_refuses(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(plan="max", messages_per_conversation=-1), FakeMessageRepo(existing=10_000), FakeConversations(), uuid.uuid4()
    cid = convs.add(user)
    for _ in range(3):
        await send(sf, ent, repo, convs, user, cid)


async def test_lost_race_for_the_last_slot_is_refused_by_the_guarded_increment(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(messages_per_conversation=1), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    cid = convs.add(user)
    await send(sf, ent, repo, convs, user, cid)
    async with sf() as s:                                # a second sender passed its pre-check earlier
        svc = PlanLimitService(ent, session=s, message_repo=repo, conversations=convs)
        with pytest.raises(PlanLimitError):
            await svc.record_send(user, cid, None)


async def test_rolled_back_request_does_not_count(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(messages_per_conversation=1), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    cid = convs.add(user)
    async with sf() as s:                                # the send failed after record_send: the request rolls back
        svc = PlanLimitService(ent, session=s, message_repo=repo, conversations=convs)
        await svc.check_send(user, cid, None)
        await svc.record_send(user, cid, None)
        await s.rollback()
    await send(sf, ent, repo, convs, user, cid)          # the slot is still free


async def test_downgrade_over_limit_keeps_counter_and_refuses_new(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(plan="pro", messages_per_conversation=200), FakeMessageRepo(existing=150), FakeConversations(), uuid.uuid4()
    cid = convs.add(user)
    await send(sf, ent, repo, convs, user, cid)
    ent.ent.limits["messages_per_conversation"] = 50     # downgraded to Free; the conversation has 151
    ent.ent.plan_code = "free"
    with pytest.raises(PlanLimitError):
        await send(sf, ent, repo, convs, user, cid)
    async with sf() as s:                                # nothing was deleted or reset
        n = (await s.execute(text("SELECT user_message_count FROM conversation_message_counters WHERE conversation_id=:c"), {"c": cid})).scalar_one()
    assert n == 151


# ---- documents_per_tree ----------------------------------------------------------------------------------------

async def test_documents_per_tree_boundary_and_reuse(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(documents_per_tree=3), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    root = convs.add(user)
    branch = convs.add(user, parent=root)                # documents of the whole tree count together
    a, b, c, d = (uuid.uuid4() for _ in range(4))
    await send(sf, ent, repo, convs, user, root, [a, b])                 # 2 in the tree
    await send(sf, ent, repo, convs, user, branch, [c])                  # 3: boundary reached, allowed
    await send(sf, ent, repo, convs, user, branch, [a, b, c])            # all already in the tree: allowed
    with pytest.raises(PlanLimitError) as e:
        await send(sf, ent, repo, convs, user, root, [d])                # a 4th distinct document
    assert_plan_limit(e.value, "documents_per_tree", "pro")
    with pytest.raises(PlanLimitError):
        await send(sf, ent, repo, convs, user, branch, [a, d])           # mixed old + new still counts the new one


async def test_documents_boundary_minus_one_allows_one_new(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(documents_per_tree=3), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    root = convs.add(user)
    await send(sf, ent, repo, convs, user, root, [uuid.uuid4(), uuid.uuid4()])
    await send(sf, ent, repo, convs, user, root, [uuid.uuid4()])
    with pytest.raises(PlanLimitError):
        await send(sf, ent, repo, convs, user, root, [uuid.uuid4(), uuid.uuid4()])


async def test_unlimited_documents_never_refuses(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(plan="max", documents_per_tree=-1), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    root = convs.add(user)
    await send(sf, ent, repo, convs, user, root, [uuid.uuid4() for _ in range(150)])


async def test_downgrade_documents_keep_old_ones_usable_and_refuse_new(sf, enforce):
    ent, repo, convs, user = FakeEntitlements(plan="max", documents_per_tree=100), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    root = convs.add(user)
    docs = [uuid.uuid4() for _ in range(5)]
    await send(sf, ent, repo, convs, user, root, docs)
    ent.ent.limits["documents_per_tree"] = 3             # downgraded: the tree has 5
    ent.ent.plan_code = "free"
    await send(sf, ent, repo, convs, user, root, docs[:2])               # existing documents still work
    with pytest.raises(PlanLimitError):
        await send(sf, ent, repo, convs, user, root, [uuid.uuid4()])
    async with sf() as s:
        n = (await s.execute(text("SELECT count(*) FROM tree_documents WHERE root_id=:r"), {"r": root})).scalar_one()
    assert n == 5


# ---- branches_per_tree -----------------------------------------------------------------------------------------

def _branch_svc(ent, convs):
    return PlanLimitService(ent, session=None, message_repo=None, conversations=convs)


async def test_branches_per_tree_boundary(enforce):
    user, convs, ent = uuid.uuid4(), FakeConversations(), FakeEntitlements(branches_per_tree=20)
    root = convs.add(user)
    last = root
    for i in range(19):                                   # 19 existing branches (a chain, then fan out)
        last = convs.add(user, parent=root if i % 2 else last)
    await _branch_svc(ent, convs).check_new_branch(user, root)            # the 20th: boundary - 1 existing -> allowed
    convs.add(user, parent=root)                                          # now 20 branches
    with pytest.raises(PlanLimitError) as e:
        await _branch_svc(ent, convs).check_new_branch(user, root)
    assert_plan_limit(e.value, "branches_per_tree", "pro")
    with pytest.raises(PlanLimitError):                                   # same tree from a deep branch
        await _branch_svc(ent, convs).check_new_branch(user, last)


async def test_branches_ignore_deleted_conversations_and_other_trees(enforce):
    user, convs, ent = uuid.uuid4(), FakeConversations(), FakeEntitlements(branches_per_tree=2)
    root, other = convs.add(user), convs.add(user)
    convs.add(user, parent=root)
    convs.add(user, parent=root, status=ConversationStatus.DELETED)       # deleted: not counted
    for _ in range(5):
        convs.add(user, parent=other)                                      # another tree
    await _branch_svc(ent, convs).check_new_branch(user, root)            # 1 branch in this tree


async def test_unlimited_branches_never_refuses(enforce):
    user, convs, ent = uuid.uuid4(), FakeConversations(), FakeEntitlements(plan="max", branches_per_tree=-1)
    root = convs.add(user)
    for _ in range(300):
        convs.add(user, parent=root)
    await _branch_svc(ent, convs).check_new_branch(user, root)


async def test_branch_downgrade_over_limit_refuses_only_new(enforce):
    user, convs, ent = uuid.uuid4(), FakeConversations(), FakeEntitlements(plan="free", branches_per_tree=20)
    root = convs.add(user)
    for _ in range(30):                                   # created while on a bigger plan
        convs.add(user, parent=root)
    with pytest.raises(PlanLimitError):
        await _branch_svc(ent, convs).check_new_branch(user, root)
    assert len(convs.list(user, 100)) == 31               # all still there


async def test_conversation_service_create_checks_branch_limit(enforce):
    from app.services.conversation_service import ConversationService
    user, convs, ent = uuid.uuid4(), FakeConversations(), FakeEntitlements(branches_per_tree=1)
    root = convs.add(user)
    convs.add(user, parent=root)
    repo = MagicMock()
    svc = ConversationService(repo=repo)
    svc.get = convs.get                                   # parent lookup
    svc.list = convs.list
    limits = PlanLimitService(ent, conversations=svc)
    with pytest.raises(PlanLimitError):
        await svc.create(user, "branch", parent_conversation_id=root, plan_limits=limits)
    repo.create_with_outbox.assert_not_called()


# ---- custom_modes + mode_instruction_chars ----------------------------------------------------------------------

def _mode(name, instructions="Be brief."):
    return CustomModeCreate(name=name, instructions=instructions)


async def test_custom_modes_boundary_and_minus_one(sf, enforce):
    user, ent = uuid.uuid4(), FakeEntitlements(custom_modes=3)
    async with sf() as s:
        svc = CustomModeService(CustomModeRepository(s), plan_limits=PlanLimitService(ent, session=s))
        for i in range(3):                                # 1st..3rd: boundary - 1 existing allowed
            await svc.create(user, _mode(f"m{i}"))
        with pytest.raises(PlanLimitError) as e:
            await svc.create(user, _mode("m3"))
        assert_plan_limit(e.value, "custom_modes", "pro")
        ent.ent.limits["custom_modes"] = -1               # unlimited (capped only by the system setting of 20)
        await svc.create(user, _mode("m3"))
        await s.commit()


async def test_archived_modes_do_not_count(sf, enforce):
    user, ent = uuid.uuid4(), FakeEntitlements(custom_modes=1)
    async with sf() as s:
        svc = CustomModeService(CustomModeRepository(s), plan_limits=PlanLimitService(ent, session=s))
        first = await svc.create(user, _mode("a"))
        with pytest.raises(PlanLimitError):
            await svc.create(user, _mode("b"))
        await svc.archive(user, first.mode_id)
        await svc.create(user, _mode("b"))
        await s.commit()


async def test_custom_modes_downgrade_keeps_existing_readable_and_editable(sf, enforce):
    user, ent = uuid.uuid4(), FakeEntitlements(plan="max", custom_modes=20)
    async with sf() as s:
        svc = CustomModeService(CustomModeRepository(s), plan_limits=PlanLimitService(ent, session=s))
        created = [await svc.create(user, _mode(f"m{i}")) for i in range(5)]
        ent.ent.limits["custom_modes"] = 3                # downgraded to Free with 5 active modes
        ent.ent.plan_code = "free"
        listing = await svc.list(user, include_archived=False, limit=50, offset=0)
        assert listing.total == 5                         # still all readable
        assert (await svc.get(user, created[4].mode_id)).name == "m4"
        renamed = await svc.update(user, created[4].mode_id, CustomModeUpdate(name="renamed"))   # editing is not creating
        assert renamed.name == "renamed"
        with pytest.raises(PlanLimitError):
            await svc.create(user, _mode("new"))
        await s.commit()


async def test_instruction_chars_boundary_on_create_and_update(sf, enforce):
    user, ent = uuid.uuid4(), FakeEntitlements(mode_instruction_chars=1000)
    async with sf() as s:
        svc = CustomModeService(CustomModeRepository(s), plan_limits=PlanLimitService(ent, session=s))
        ok = await svc.create(user, _mode("exact", "x" * 1000))               # boundary: allowed
        with pytest.raises(PlanLimitError) as e:
            await svc.create(user, _mode("toolong", "x" * 1001))              # boundary + 1: refused
        assert_plan_limit(e.value, "mode_instruction_chars", "pro")
        with pytest.raises(PlanLimitError):
            await svc.update(user, ok.mode_id, CustomModeUpdate(instructions="y" * 1001))
        ent.ent.limits["mode_instruction_chars"] = 500    # shrinks: the stored 1000-char mode stays as it is
        assert (await svc.update(user, ok.mode_id, CustomModeUpdate(name="exact2"))).name == "exact2"
        assert len((await svc.get(user, ok.mode_id)).instructions) == 1000
        ent.ent.limits["mode_instruction_chars"] = -1
        await svc.update(user, ok.mode_id, CustomModeUpdate(instructions="z" * 3000))
        await s.commit()


async def test_preferences_personal_instructions_limit(sf, enforce):
    user, ent = uuid.uuid4(), FakeEntitlements(mode_instruction_chars=500)
    async with sf() as s:
        svc = PreferencesService(PreferencesRepository(s), CustomModeRepository(s), plan_limits=PlanLimitService(ent, session=s))
        await svc.update(user, PreferencesUpdate(personal_instructions="a" * 500))
        with pytest.raises(PlanLimitError) as e:
            await svc.update(user, PreferencesUpdate(personal_instructions="a" * 501))
        assert_plan_limit(e.value, "mode_instruction_chars", "pro")
        await svc.update(user, PreferencesUpdate(tone="friendly"))            # other fields are never blocked
        await s.commit()


# ---- modes -----------------------------------------------------------------------------------------------------

async def test_off_mode_checks_nothing_and_never_asks_entitlements(sf, monkeypatch):
    monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", "off")
    ent, repo, convs, user = FakeEntitlements(messages_per_conversation=0, documents_per_tree=0, branches_per_tree=0), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    root = convs.add(user)
    await send(sf, ent, repo, convs, user, root, [uuid.uuid4()])
    await _branch_svc(ent, convs).check_new_branch(user, root)
    assert ent.calls == 0 and repo.scans == 0


async def test_shadow_mode_logs_but_never_refuses(sf, monkeypatch):
    monkeypatch.setattr(settings, "BILLING_ENFORCEMENT", "shadow")
    ent, repo, convs, user = FakeEntitlements(messages_per_conversation=1, documents_per_tree=1, branches_per_tree=0), FakeMessageRepo(), FakeConversations(), uuid.uuid4()
    root = convs.add(user)
    before = metrics.plan_limit_denied_total.get(limit="messages_per_conversation")
    for _ in range(3):
        await send(sf, ent, repo, convs, user, root, [uuid.uuid4()])
    await _branch_svc(ent, convs).check_new_branch(user, root)
    assert metrics.plan_limit_denied_total.get(limit="messages_per_conversation") == before   # counted only when enforced
