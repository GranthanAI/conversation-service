"""L2: the bundled model -> group map matches billing-service's plans.yaml whenever that repo is next to this one."""

import pathlib

import pytest
import yaml

from app.core.plan_defaults import MODEL_GROUPS

PLANS = pathlib.Path(__file__).resolve().parents[3] / "billing-service" / "config" / "plans.yaml"


@pytest.mark.skipif(not PLANS.exists(), reason="billing-service repo not next to this one")
def test_model_groups_match_plans_yaml():
    groups = yaml.safe_load(PLANS.read_text(encoding="utf-8"))["model_groups"]
    expected = {m["id"]: g for g, models in groups.items() for m in models}
    assert MODEL_GROUPS == expected
