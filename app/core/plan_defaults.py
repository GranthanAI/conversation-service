"""Bundled copy of the Free plan's limits (billing-service config/plans.yaml, catalog 2026-11-09).

Used only when billing cannot be reached and no earlier entitlement is cached (integration plan 4.7). A test
(tests/unit/test_plan_defaults.py) checks that this copy matches plans.yaml whenever the billing repo is next to this
one. -1 means unlimited everywhere.
"""

FREE_LIMITS: dict[str, int] = {
    "messages_per_day": 25,
    "best_per_day": 0,
    "branches_per_tree": 20,
    "messages_per_conversation": 50,
    "memory_facts": 50,
    "context_tokens": 16000,
    "documents_per_tree": 3,
    "file_size_mb": 10,
    "youtube_per_month": 3,
    "storage_mb": 100,
    "custom_modes": 3,
    "mode_instruction_chars": 1000,
}

# Individual plans in upgrade order (free < pro < max). Anything else (org / payg plans) has no upgrade target here.
PLAN_ORDER: tuple[str, ...] = ("free", "pro", "max")

# Fast models of the Free plan, used to decide which models are allowed while billing is unreachable.
FAST_MODELS: frozenset[str] = frozenset({"openai/gpt-oss-20b", "gemini-2.5-flash-lite"})


def next_plan(plan_code: str) -> str | None:
    """The plan to suggest in an 'Upgrade to ...' message, or None on the top plan / non-individual plans."""
    try:
        i = PLAN_ORDER.index(plan_code)
    except ValueError:
        return None
    return PLAN_ORDER[i + 1] if i + 1 < len(PLAN_ORDER) else None
