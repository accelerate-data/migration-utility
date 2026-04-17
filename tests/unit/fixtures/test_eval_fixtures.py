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


def test_dynamic_sql_eval_fixtures_use_canonical_migrationtest_object_names() -> None:
    fixture_root = Path(__file__).resolve().parents[3] / "tests/evals/fixtures"
    fixture_dirs = [
        fixture_root / "analyzing-table/dynamic-sql",
        fixture_root / "generating-tests/dynamic-sql",
        fixture_root / "profiling-table/dynamic-sql",
        fixture_root / "refactoring-sql/dynamic-sql",
        fixture_root / "reviewing-tests/review-approved-dynamic-sql",
        fixture_root / "cmd-refactor/partial",
        fixture_root / "cmd-status/status-single-dimcurrency",
    ]

    forbidden_tokens = [
        "silver.DimCurrency",
        "silver.usp_load_DimCurrency",
        '"schema": "silver"',
        '"item_id": "silver.dimcurrency"',
    ]

    for fixture_dir in fixture_dirs:
        manifest = json.loads((fixture_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["extraction"]["schemas"] == ["MigrationTest"]

        text_files = list(fixture_dir.rglob("*.sql")) + list(fixture_dir.rglob("*.json"))
        for path in text_files:
            text = path.read_text(encoding="utf-8")
            for token in forbidden_tokens:
                assert token not in text, f"{path} still contains legacy token: {token}"

        assert not any(fixture_dir.rglob("silver.dimcurrency.json"))
        assert not any(fixture_dir.rglob("silver.usp_load_dimcurrency.json"))


def test_status_no_target_fixture_has_cataloged_object_state() -> None:
    fixture_root = (
        Path(__file__).resolve().parents[3]
        / "tests/evals/fixtures/cmd-status/status-single-insertselecttarget-no-target"
    )

    expected_paths = [
        "catalog/tables/silver.insertselecttarget.json",
        "catalog/procedures/silver.usp_load_insertselecttarget.json",
        "dbt/dbt_project.yml",
    ]

    for relative_path in expected_paths:
        assert (fixture_root / relative_path).is_file()
