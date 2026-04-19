from __future__ import annotations

import json
from pathlib import Path

from shared.dry_run_support.reset_preserve_catalog import (
    load_preserve_catalog_mutations,
    run_reset_migration_all_preserve_catalog,
)
from tests.unit.dry_run.dry_run_test_helpers import _make_reset_project


def test_load_preserve_catalog_mutations_clears_generated_sections_only(tmp_path: Path) -> None:
    root = _make_reset_project(tmp_path)

    mutations = load_preserve_catalog_mutations(root)

    table_mutation = next(item for item in mutations if item.path.name == "silver.dimcustomer.json")
    assert table_mutation.original["profile"]["status"] == "ok"
    assert "test_gen" not in table_mutation.updated
    assert "profile" in table_mutation.updated
    assert [item.section for item in table_mutation.cleared] == ["table.test_gen"]


def test_run_reset_migration_all_preserve_catalog_keeps_catalog_and_ddl(tmp_path: Path) -> None:
    root = _make_reset_project(tmp_path)
    (root / "ddl").mkdir()
    (root / "dbt" / "models").mkdir(parents=True)
    (root / ".staging").mkdir()

    result = run_reset_migration_all_preserve_catalog(root)

    assert result.deleted_paths == ["dbt", "test-specs", ".staging"]
    assert (root / "catalog").exists()
    assert (root / "ddl").exists()
    assert not (root / "dbt").exists()
    table = json.loads((root / "catalog" / "tables" / "silver.dimcustomer.json").read_text(encoding="utf-8"))
    assert "test_gen" not in table
    assert "profile" in table
