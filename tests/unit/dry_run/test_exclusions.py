from __future__ import annotations

import json
from pathlib import Path


from shared import dry_run
from tests.unit.dry_run.dry_run_test_helpers import (
    _cli_runner,
    _make_exclude_project,
)

def test_run_exclude_table_sets_flag(tmp_path: Path) -> None:
    """run_exclude sets excluded: true on a table catalog file."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog"])
    assert result.marked == ["silver.auditlog"]
    assert result.not_found == []
    assert result.written_paths == ["catalog/tables/silver.auditlog.json"]
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True

def test_run_exclude_view_sets_flag(tmp_path: Path) -> None:
    """run_exclude sets excluded: true on a view catalog file."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.vw_legacy"])
    assert result.marked == ["silver.vw_legacy"]
    assert result.not_found == []
    assert result.written_paths == ["catalog/views/silver.vw_legacy.json"]
    cat = json.loads((dst / "catalog" / "views" / "silver.vw_legacy.json").read_text())
    assert cat.get("excluded") is True

def test_run_exclude_multiple_fqns(tmp_path: Path) -> None:
    """run_exclude marks multiple objects in one call."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog", "silver.vw_legacy"])
    assert sorted(result.marked) == ["silver.auditlog", "silver.vw_legacy"]
    assert result.not_found == []
    assert sorted(result.written_paths) == [
        "catalog/tables/silver.auditlog.json",
        "catalog/views/silver.vw_legacy.json",
    ]

def test_run_exclude_not_found_reported(tmp_path: Path) -> None:
    """FQN with no catalog file appears in not_found, does not raise."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.Nonexistent"])
    assert result.marked == []
    assert result.not_found == ["silver.nonexistent"]

def test_run_exclude_mixed_found_and_not_found(tmp_path: Path) -> None:
    """Partial success: found items are marked, missing items are in not_found."""
    dst = _make_exclude_project(tmp_path)
    result = dry_run.run_exclude(dst, ["silver.AuditLog", "silver.Missing"])
    assert result.marked == ["silver.auditlog"]
    assert result.not_found == ["silver.missing"]

def test_run_exclude_idempotent(tmp_path: Path) -> None:
    """Calling run_exclude twice on the same FQN does not corrupt the catalog."""
    dst = _make_exclude_project(tmp_path)
    dry_run.run_exclude(dst, ["silver.AuditLog"])
    result = dry_run.run_exclude(dst, ["silver.AuditLog"])
    assert result.marked == ["silver.auditlog"]
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True
    assert cat.get("primary_keys") is not None

def test_run_exclude_preserves_existing_catalog_fields(tmp_path: Path) -> None:
    """run_exclude only adds excluded: true — it does not strip other catalog fields."""
    dst = _make_exclude_project(tmp_path)
    dry_run.run_exclude(dst, ["silver.AuditLog"])
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat["schema"] == "silver"
    assert cat["name"] == "AuditLog"

def test_exclude_cli_subcommand(tmp_path: Path) -> None:
    """CLI exclude subcommand emits valid JSON and sets excluded: true."""
    dst = _make_exclude_project(tmp_path)
    result = _cli_runner.invoke(
        dry_run.app,
        ["exclude", "silver.AuditLog", "--project-root", str(dst)],
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert output["marked"] == ["silver.auditlog"]
    cat = json.loads((dst / "catalog" / "tables" / "silver.auditlog.json").read_text())
    assert cat.get("excluded") is True
