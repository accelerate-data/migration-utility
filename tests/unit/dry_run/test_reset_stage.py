"""Tests for staged dry-run reset behavior."""

from __future__ import annotations

import json
from pathlib import Path

from tests.unit.dry_run.dry_run_test_helpers import _make_reset_project


def test_reset_table_sections_preserves_scoping_and_deletes_test_spec(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_stage import reset_table_sections

    root = _make_reset_project(tmp_path)

    cleared_sections, deleted_files, mutated_files, writer = reset_table_sections(
        root,
        "silver.dimcustomer",
        "profile",
    )

    assert writer == "dbo.usp_load_dimcustomer"
    assert "table.profile" in cleared_sections
    assert "table.test_gen" in cleared_sections
    assert deleted_files == ["test-specs/silver.dimcustomer.json"]
    assert mutated_files == ["catalog/tables/silver.dimcustomer.json"]
    table = json.loads((root / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
    assert "scoping" in table
    assert "profile" not in table


def test_reset_writer_refactor_removes_procedure_refactor(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_stage import reset_writer_refactor

    root = _make_reset_project(tmp_path)

    cleared_sections, mutated_files = reset_writer_refactor(root, "dbo.usp_load_dimcustomer")

    assert cleared_sections == ["procedure:dbo.usp_load_dimcustomer.refactor"]
    assert mutated_files == ["catalog/procedures/dbo.usp_load_dimcustomer.json"]
    proc = json.loads((root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json").read_text())
    assert "refactor" not in proc


def test_run_reset_migration_stage_blocks_before_mutating_any_valid_target(tmp_path: Path) -> None:
    from shared.dry_run_support.reset_stage import run_reset_migration_stage

    root = _make_reset_project(tmp_path)
    blocked_table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
    blocked_table = json.loads(blocked_table_path.read_text(encoding="utf-8"))
    blocked_table["generate"] = {"status": "ok"}
    blocked_table_path.write_text(json.dumps(blocked_table), encoding="utf-8")

    result = run_reset_migration_stage(root, "profile", ["silver.DimCustomer", "silver.DimProduct"])

    assert result.reset == []
    assert result.blocked == ["silver.dimcustomer"]
    assert result.targets[0].status == "blocked"
    assert (root / "test-specs" / "silver.dimproduct.json").exists()
