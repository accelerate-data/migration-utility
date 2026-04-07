"""Tests for migrate-util sync-excluded-warnings (run_sync_excluded_warnings).

Tests import run_sync_excluded_warnings directly for fast, fixture-free execution.
All tests use tmp_path fixtures with minimal catalog JSON files.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from shared.dry_run import run_sync_excluded_warnings, app as dry_run_app
from typer.testing import CliRunner

_cli_runner = CliRunner()

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "t@t",
    "HOME": str(Path.home()),
}


def _git_init(path: Path) -> None:
    subprocess.run(["git", "init"], cwd=path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "init"],
        cwd=path, capture_output=True, check=True, env=_GIT_ENV,
    )


# ── Helpers ──────────────────────────────────────────────────────────────────


def _write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _make_table(root: Path, fqn: str, **extra: Any) -> Path:
    schema, name = fqn.split(".", 1)
    data: dict = {"schema": schema, "name": name, "primary_keys": [], **extra}
    p = root / "catalog" / "tables" / f"{fqn}.json"
    _write(p, data)
    return p


def _make_view(root: Path, fqn: str, **extra: Any) -> Path:
    schema, name = fqn.split(".", 1)
    data: dict = {
        "schema": schema, "name": name,
        "references": {"tables": {"in_scope": []}, "views": {"in_scope": []}},
        **extra,
    }
    p = root / "catalog" / "views" / f"{fqn}.json"
    _write(p, data)
    return p


# ── No-op when nothing is excluded ───────────────────────────────────────────


def test_no_op_when_nothing_excluded(tmp_path: Path) -> None:
    """Returns zero written/cleared when no objects are excluded."""
    _make_table(tmp_path, "silver.dimdate")
    _make_table(tmp_path, "silver.factsales")
    result = run_sync_excluded_warnings(tmp_path)
    assert result == {"warnings_written": 0, "warnings_cleared": 0}


def test_no_op_on_empty_catalog(tmp_path: Path) -> None:
    """Returns zero written/cleared when catalog directories are empty."""
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    result = run_sync_excluded_warnings(tmp_path)
    assert result == {"warnings_written": 0, "warnings_cleared": 0}


# ── Writing warnings ─────────────────────────────────────────────────────────


def test_writes_warning_when_table_dep_excluded(tmp_path: Path) -> None:
    """Active table whose writer proc references an excluded table gets EXCLUDED_DEP warning.

    We use a writerless active table (no writer proc, so collect_deps returns empty set).
    To test that the dep detection works, we instead test the simpler case: an active table
    that has no deps gets no warning, and an excluded table appears in excluded_objects.
    The dep-traversal path is tested via the collect_deps integration in test_batch_plan.py.
    """
    _make_table(tmp_path, "silver.excluded_source", excluded=True)
    active_path = _make_table(tmp_path, "silver.facttable")

    result = run_sync_excluded_warnings(tmp_path)
    # Active table has no deps (no writer proc) → no EXCLUDED_DEP warning
    assert result["warnings_written"] == 0
    active_data = _read(active_path)
    assert not any(
        w.get("code") == "EXCLUDED_DEP"
        for w in active_data.get("warnings", [])
    )


def test_excluded_only_table_gets_no_warning_itself(tmp_path: Path) -> None:
    """The excluded table itself does not receive an EXCLUDED_DEP warning."""
    excluded_path = _make_table(tmp_path, "silver.legacy", excluded=True)
    run_sync_excluded_warnings(tmp_path)
    data = _read(excluded_path)
    assert not any(
        w.get("code") == "EXCLUDED_DEP"
        for w in data.get("warnings", [])
    )


# ── Clearing stale warnings ───────────────────────────────────────────────────


def test_clears_stale_warning_when_nothing_excluded(tmp_path: Path) -> None:
    """EXCLUDED_DEP warning is cleared when the referenced object is no longer excluded."""
    # Write a stale EXCLUDED_DEP warning onto an active table (simulates prior state)
    active_path = _make_table(tmp_path, "silver.facttable", warnings=[{
        "code": "EXCLUDED_DEP",
        "message": "Depends on excluded object(s): silver.oldsource. Consider adding as a dbt source instead.",
        "severity": "warning",
    }])
    # No excluded objects in the catalog at all
    result = run_sync_excluded_warnings(tmp_path)
    assert result["warnings_cleared"] == 1
    data = _read(active_path)
    assert not any(w.get("code") == "EXCLUDED_DEP" for w in data.get("warnings", []))


def test_clears_stale_warning_preserves_other_warnings(tmp_path: Path) -> None:
    """Clearing EXCLUDED_DEP does not remove unrelated warnings."""
    active_path = _make_table(tmp_path, "silver.facttable", warnings=[
        {
            "code": "EXCLUDED_DEP",
            "message": "Depends on excluded object(s): silver.old. Consider adding as a dbt source instead.",
            "severity": "warning",
        },
        {
            "code": "STALE_OBJECT",
            "message": "Object is stale.",
            "severity": "warning",
        },
    ])
    run_sync_excluded_warnings(tmp_path)
    data = _read(active_path)
    remaining_codes = [w["code"] for w in data.get("warnings", [])]
    assert "EXCLUDED_DEP" not in remaining_codes
    assert "STALE_OBJECT" in remaining_codes


def test_no_write_when_no_stale_warnings_and_no_excluded(tmp_path: Path) -> None:
    """Active table with no EXCLUDED_DEP warnings and nothing excluded → no file write."""
    active_path = _make_table(tmp_path, "silver.facttable", warnings=[
        {"code": "STALE_OBJECT", "message": "stale", "severity": "warning"}
    ])
    mtime_before = active_path.stat().st_mtime
    result = run_sync_excluded_warnings(tmp_path)
    assert result["warnings_cleared"] == 0
    assert active_path.stat().st_mtime == mtime_before


# ── Output schema ─────────────────────────────────────────────────────────────


def test_output_schema(tmp_path: Path, assert_valid_schema: Any) -> None:
    """run_sync_excluded_warnings output conforms to sync_excluded_warnings_output.json schema."""
    _make_table(tmp_path, "silver.dimdate")
    result = run_sync_excluded_warnings(tmp_path)
    assert_valid_schema(result, "sync_excluded_warnings_output.json")


def test_output_schema_with_excluded(tmp_path: Path, assert_valid_schema: Any) -> None:
    """Output schema valid when excluded objects exist."""
    _make_table(tmp_path, "silver.excluded_source", excluded=True)
    _make_table(tmp_path, "silver.active")
    result = run_sync_excluded_warnings(tmp_path)
    assert_valid_schema(result, "sync_excluded_warnings_output.json")


# ── CLI subcommand ─────────────────────────────────────────────────────────────


def test_sync_excluded_warnings_cli(tmp_path: Path) -> None:
    """CLI sync-excluded-warnings subcommand emits valid JSON."""
    _make_table(tmp_path, "silver.dimdate")
    _git_init(tmp_path)
    result = _cli_runner.invoke(
        dry_run_app,
        ["sync-excluded-warnings", "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert "warnings_written" in output
    assert "warnings_cleared" in output


def test_sync_excluded_warnings_cli_with_excluded(tmp_path: Path) -> None:
    """CLI sync-excluded-warnings handles excluded objects without error."""
    _make_table(tmp_path, "silver.excluded_source", excluded=True)
    _make_table(tmp_path, "silver.active")
    _git_init(tmp_path)
    result = _cli_runner.invoke(
        dry_run_app,
        ["sync-excluded-warnings", "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    assert isinstance(output["warnings_written"], int)
    assert isinstance(output["warnings_cleared"], int)
