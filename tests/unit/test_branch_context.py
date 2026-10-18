"""Branch context inheritance (A -> B -> C): history up to each branch point, no leakage of later
parent messages, independent siblings, fail-closed lineage, document inheritance with provenance.
In-memory fakes only; ids are monotonically increasing like UUIDv7."""

import itertools
import uuid
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.asyncio

from app.models.conversation import Conversation, ConversationStatus
from app.models.message import Message, MessageStatus
from app.services.branch_context_service import (
    INHERITED_MAX_MESSAGES,
    MAX_LINEAGE_DEPTH,
    BranchContextService,
)

USER = uuid.uuid4()
_tick = itertools.count(1)
_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _uid() -> uuid.UUID:
    return uuid.UUID(int=next(_tick))


class FakeStore:
    """Conversations + messages (newest-first reads, like Cassandra's DESC clustering)."""

    def __init__(self):
        self.convs: dict[uuid.UUID, Conversation] = {}
        self.msgs: dict[uuid.UUID, list[Message]] = {}

    def conv(self, title, parent=None, branch=None, user=USER, status=ConversationStatus.ACTIVE, at=None):
        cid = _uid()
        self.convs[cid] = Conversation(
            conversation_id=cid, user_id=user, title=title, created_at=at or _T0, updated_at=_T0,
            status=status, parent_conversation_id=parent, branch_message_id=branch)
        self.msgs[cid] = []
        return cid

    def say(self, cid, sender, text, files=(), status=MessageStatus.SENT):
        m = Message(conversation_id=cid, message_id=_uid(), sender=sender, content=text,
                    created_at=_T0 + timedelta(seconds=len(self.msgs[cid])), status=status, file_ids=list(files))
        self.msgs[cid].append(m)
        return m.message_id

    # --- repo interface used by the service ---
    async def get(self, cid):
        return self.convs.get(cid)

    def history(self, cid, limit=50, cursor=None):
        rows = sorted(self.msgs.get(cid, []), key=lambda m: m.message_id.int, reverse=True)
        return rows[:limit]

    def get_message(self, cid, mid):
        return next((m for m in self.msgs.get(cid, []) if m.message_id == mid), None)

    def history_upto(self, cid, upto, limit=50):
        rows = [m for m in self.msgs.get(cid, []) if m.message_id.int <= upto.int]
        return sorted(rows, key=lambda m: m.message_id.int, reverse=True)[:limit]


def _svc(store):
    return BranchContextService(store, store)


async def _hist(store, cid):
    return [(h["role"], h["content"]) for h in await _svc(store).inherited_history(store.convs[cid])]


@pytest.fixture
def abc():
    s = FakeStore()
    a = s.conv("A")
    s.say(a, "user", "A: What is a KV cache?")
    a_last = s.say(a, "assistant", "A: It stores keys and values.")
    s.say(a, "user", "A-later: unrelated follow-up")        # after B was branched off
    b = s.conv("B", parent=a, branch=a_last)
    s.say(b, "user", "B: and how does it help latency?")
    b_last = s.say(b, "assistant", "B: Avoids recomputation.")
    s.say(b, "user", "B-later: something else")
    c = s.conv("C", parent=b, branch=b_last)
    return s, a, b, c


async def test_c_inherits_a_then_b_in_chronological_order(abc):
    s, a, b, c = abc
    assert await _hist(s, c) == [
        ("user", "A: What is a KV cache?"),
        ("assistant", "A: It stores keys and values."),
        ("user", "B: and how does it help latency?"),
        ("assistant", "B: Avoids recomputation."),
    ]


async def test_later_messages_in_the_parent_never_leak_into_children(abc):
    s, a, b, c = abc
    # a message added to A and to B after the branches were taken
    s.say(a, "assistant", "A-latest: added after the branch")
    s.say(b, "assistant", "B-latest: added after the branch")
    texts = [t for _r, t in await _hist(s, c)]
    assert not any("later" in t or "latest" in t for t in texts)
    assert [t for _r, t in await _hist(s, b)] == ["A: What is a KV cache?", "A: It stores keys and values."]


async def test_siblings_are_independent(abc):
    s, a, b, c = abc
    a_first = s.msgs[a][0].message_id
    sibling = s.conv("B2", parent=a, branch=a_first)
    s.say(sibling, "user", "B2: private to the sibling")
    assert [t for _r, t in await _hist(s, sibling)] == ["A: What is a KV cache?"]
    assert all("B2" not in t for _r, t in await _hist(s, c))
    assert all("B:" not in t for _r, t in await _hist(s, sibling))


async def test_legacy_branch_without_a_boundary_uses_the_creation_time(abc):
    s, a, *_ = abc
    # created between A's 2nd and 3rd message (created_at is _T0 + index seconds)
    legacy = s.conv("legacy", parent=a, branch=None, at=_T0 + timedelta(seconds=1, milliseconds=500))
    assert [t for _r, t in await _hist(s, legacy)] == ["A: What is a KV cache?", "A: It stores keys and values."]


@pytest.mark.parametrize("kind", ["missing", "deleted", "other_user"])
async def test_unavailable_ancestor_ends_the_chain_without_guessing(abc, kind):
    s, a, b, c = abc
    if kind == "missing":
        del s.convs[b]
    elif kind == "deleted":
        s.convs[b].status = ConversationStatus.DELETED
    else:
        s.convs[b].user_id = uuid.uuid4()
    assert await _hist(s, c) == []                      # not even A: the chain is broken at B
    assert (await _svc(s).lineage(s.convs[c])).truncated is not None


async def test_deleted_middle_keeps_what_is_reachable_for_b():
    s = FakeStore()
    a = s.conv("A")
    last = s.say(a, "user", "A1")
    b = s.conv("B", parent=a, branch=last)
    s.say(b, "user", "B1")
    assert await _hist(s, b) == [("user", "A1")]


async def test_cycles_and_excessive_depth_are_bounded():
    s = FakeStore()
    x, y = s.conv("X"), s.conv("Y")
    s.convs[x].parent_conversation_id, s.convs[y].parent_conversation_id = y, x
    s.say(y, "user", "hi")
    lineage = await _svc(s).lineage(s.convs[x])
    assert lineage.truncated == "cycle"

    prev = s.conv("root")
    for i in range(MAX_LINEAGE_DEPTH + 5):
        m = s.say(prev, "user", f"m{i}")
        prev = s.conv(f"n{i}", parent=prev, branch=m)
    lineage = await _svc(s).lineage(s.convs[prev])
    assert lineage.truncated == "max_depth" and len(lineage.segments) == MAX_LINEAGE_DEPTH


async def test_history_is_bounded_keeps_the_newest_and_has_no_duplicates():
    s = FakeStore()
    a = s.conv("A")
    for i in range(40):
        s.say(a, "user" if i % 2 == 0 else "assistant", f"turn {i}")
    last = s.msgs[a][-1].message_id
    b = s.conv("B", parent=a, branch=last)
    hist = await _hist(s, b)
    assert len(hist) == INHERITED_MAX_MESSAGES
    assert hist[-1][1] == "turn 39" and len({t for _r, t in hist}) == len(hist)


async def test_failed_pending_and_system_rows_are_not_inherited():
    s = FakeStore()
    a = s.conv("A")
    s.say(a, "user", "ok question")
    s.say(a, "assistant", "partial", status=MessageStatus.PENDING)
    s.say(a, "assistant", "broken", status=MessageStatus.FAILED)
    last = s.say(a, "system", "summary row")
    b = s.conv("B", parent=a, branch=last)
    assert await _hist(s, b) == [("user", "ok question")]


async def test_documents_are_inherited_with_provenance_up_to_the_branch_points():
    s = FakeStore()
    pdf_a, pdf_b, pdf_late = _uid(), _uid(), _uid()
    a = s.conv("A")
    s.say(a, "user", "see this paper", files=[pdf_a])
    a_last = s.say(a, "assistant", "ok")
    s.say(a, "user", "later upload", files=[pdf_late])
    b = s.conv("B", parent=a, branch=a_last)
    b_msg = s.say(b, "user", "and this one", files=[pdf_b, pdf_a])   # re-attached: deduplicated
    c = s.conv("C", parent=b, branch=b_msg)
    ctx = await _svc(s).context(s.convs[c])
    docs = [(d["document_id"], d["source_title"]) for d in ctx["documents"]]
    assert docs == [(str(pdf_a), "A"), (str(pdf_b), "B")]                  # stable order, no later upload
    assert str(pdf_late) not in [d["document_id"] for d in ctx["documents"]]
    assert [x["title"] for x in ctx["lineage"]] == ["A", "B"] and ctx["truncated"] is None
    # the parents themselves are unchanged and an excluded document is only a client-side choice
    assert s.msgs[a][0].file_ids == [pdf_a]


async def test_a_conversation_without_a_parent_inherits_nothing():
    s = FakeStore()
    a = s.conv("A")
    s.say(a, "user", "hi", files=[_uid()])
    ctx = await _svc(s).context(s.convs[a])
    assert ctx["lineage"] == [] and ctx["documents"] == []
    assert await _hist(s, a) == []


# --- branch point recorded at creation, and wiring into the request event ---

class _CreateRepo:
    def __init__(self):
        self.kwargs = None

    def create_with_outbox(self, **kw):
        self.kwargs = kw
        return Conversation(conversation_id=kw["conversation_id"], user_id=kw["user_id"], title=kw["title"],
                            created_at=_T0, updated_at=_T0, parent_conversation_id=kw["parent_conversation_id"],
                            branch_message_id=kw["branch_message_id"])


async def test_creating_a_branch_freezes_the_branch_point():
    from app.services.conversation_service import ConversationService

    s = FakeStore()
    a = s.conv("A")
    first = s.say(a, "user", "q1")
    last = s.say(a, "assistant", "a1")
    repo = _CreateRepo()
    svc = ConversationService(repo=repo)
    svc.get = s.get  # parent lookup

    c = await svc.create(USER, "child", parent_conversation_id=a, message_repo=s)
    assert c.branch_message_id == last                                  # default: parent's latest message
    c = await svc.create(USER, "child", parent_conversation_id=a, branch_message_id=first, message_repo=s)
    assert c.branch_message_id == first                                 # explicit message
    s.say(a, "user", "q2")
    assert repo.kwargs["branch_message_id"] == first                    # later messages do not move it
    with pytest.raises(ValueError):
        await svc.create(USER, "child", parent_conversation_id=a, branch_message_id=_uid(), message_repo=s)
    root = await svc.create(USER, "root", message_repo=s)
    assert root.branch_message_id is None


async def test_request_event_history_marks_inherited_turns(abc):
    from app.services.message_service import MessageService

    s, a, b, c = abc
    svc = MessageService(repo=None, branch_context=_svc(s))
    own = [{"role": "user", "content": "C: own question"}]
    out = await svc._with_inherited_history(c, own)
    assert [h["content"] for h in out][-1] == "C: own question"
    inherited = [h for h in out if h.get("inherited")]
    assert len(inherited) == 4 and inherited[0]["source_conversation_id"] == str(a)
    assert out[:4] == inherited and "inherited" not in out[-1]
    # a root conversation is untouched
    assert await svc._with_inherited_history(a, own) == own


async def test_event_carries_branch_lineage_with_boundaries_and_omits_legacy_branches(abc):
    from app.services.message_service import MessageService

    s, a, b, c = abc
    svc = MessageService(repo=None, branch_context=_svc(s))
    history, ancestors = await svc._branch_event_context(c, [])
    assert [(x["conversation_id"], x["upto_message_id"]) for x in ancestors] == [
        (str(a), str(s.convs[b].branch_message_id)), (str(b), str(s.convs[c].branch_message_id))]
    assert len(history) == 4
    legacy = s.conv("legacy", parent=a, branch=None)
    assert (await svc._branch_event_context(legacy, []))[1] == []        # no boundary recorded: nothing to prove
    assert (await svc._branch_event_context(a, []))[1] == []             # root conversation
