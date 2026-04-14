from __future__ import annotations

import json
from pathlib import Path


def test_refactoring_view_active_customers_manifest_uses_canonical_sql_server_fixture_contract() -> None:
    manifest_path = (
        Path(__file__).resolve().parents[3]
        / "tests/evals/fixtures/refactoring-sql/view-active-customers/manifest.json"
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]

    assert source["database"] == "AdventureWorks2022"
    assert source["schema"] == "MigrationTest"
    assert sandbox["database"] == "AdventureWorks2022"
    assert sandbox["schema"] == "MigrationTest"
