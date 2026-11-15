"""M3: the bundled model -> group map (the cold-start fallback) matches a snapshot of billing's GET /billing/v1/models.
The snapshot is a fixture (tests/fixtures/billing_models_catalog.json): refresh it from billing when the catalog changes."""

import json
import pathlib

from app.core.plan_defaults import MODEL_GROUPS

SNAPSHOT = json.loads((pathlib.Path(__file__).parent.parent / "fixtures" / "billing_models_catalog.json").read_text())


def test_bundled_groups_match_the_catalog_snapshot():
    assert MODEL_GROUPS == {m["id"]: m["group"] for m in SNAPSHOT["models"]}
