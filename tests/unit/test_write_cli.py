"""Tests for run_write_test_gen — test-gen catalog writer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.loader_data import CatalogFileMissingError
from shared.test_harness import run_write_test_gen


def _seed_catalog(project_root: Path, fqn: str, *, kind: str = "tables") -> Path:
    """Create a minimal catalog JSON file and return its path."""
    cat_path = project_root / "catalog" / kind / f"{fqn}.json"
    cat_path.parent.mkdir(parents=True, exist_ok=True)
    cat_path.write_text(json.dumps({"schema": "dbo", "name": fqn}))
    return cat_path


def _seed_spec(project_root: Path, fqn: str, payload: dict | str | None = None) -> Path:
    """Create a test-spec JSON file and return its path.

    If *payload* is a string, it is written raw (to simulate invalid JSON).
    If *payload* is None, a minimal valid spec is written.
    """
    spec_dir = project_root / "test-specs"
    spec_dir.mkdir(parents=True, exist_ok=True)
    spec_path = spec_dir / f"{fqn}.json"
    if isinstance(payload, str):
        spec_path.write_text(payload)
    else:
        spec_path.write_text(json.dumps(payload or {"unit_tests": []}))
    return spec_path


# ── Happy path ────────────────────────────────────────────────────────────────


def test_write_test_gen_ok(tmp_path: Path) -> None:
    """Spec file exists, valid JSON, branches > 0 → status ok, test_gen written."""
    fqn = "dbo.my_table"
    _seed_catalog(tmp_path, fqn)
    _seed_spec(tmp_path, fqn, {"unit_tests": [{"name": "t1"}]})

    result = run_write_test_gen(
        project_root=tmp_path,
        table_fqn=fqn,
        branches=3,
        unit_tests=5,
        coverage="complete",
    )

    assert result["status"] == "ok"
    assert result["ok"] is True
    assert result["table"] == fqn

    # Verify catalog was updated
    cat = json.loads((tmp_path / "catalog" / "tables" / f"{fqn}.json").read_text())
    assert cat["test_gen"]["status"] == "ok"
    assert cat["test_gen"]["branches"] == 3
    assert cat["test_gen"]["unit_tests"] == 5
    assert cat["test_gen"]["coverage"] == "complete"


# ── Error: spec file missing ──────────────────────────────────────────────────


def test_write_test_gen_error_no_spec(tmp_path: Path) -> None:
    """Spec file does not exist → status error."""
    fqn = "dbo.my_table"
    _seed_catalog(tmp_path, fqn)
    # No spec file created

    result = run_write_test_gen(
        project_root=tmp_path,
        table_fqn=fqn,
        branches=3,
        unit_tests=5,
        coverage="complete",
    )

    assert result["status"] == "error"


# ── Error: invalid JSON in spec ───────────────────────────────────────────────


def test_write_test_gen_error_invalid_json(tmp_path: Path) -> None:
    """Spec file exists but is not valid JSON → status error."""
    fqn = "dbo.my_table"
    _seed_catalog(tmp_path, fqn)
    _seed_spec(tmp_path, fqn, "{not valid json")

    result = run_write_test_gen(
        project_root=tmp_path,
        table_fqn=fqn,
        branches=3,
        unit_tests=5,
        coverage="complete",
    )

    assert result["status"] == "error"


# ── Error: zero branches ─────────────────────────────────────────────────────


def test_write_test_gen_error_zero_branches(tmp_path: Path) -> None:
    """branches == 0 → status error even with valid spec."""
    fqn = "dbo.my_table"
    _seed_catalog(tmp_path, fqn)
    _seed_spec(tmp_path, fqn, {"unit_tests": [{"name": "t1"}]})

    result = run_write_test_gen(
        project_root=tmp_path,
        table_fqn=fqn,
        branches=0,
        unit_tests=5,
        coverage="partial",
    )

    assert result["status"] == "error"


# ── Missing catalog ───────────────────────────────────────────────────────────


def test_write_test_gen_missing_catalog(tmp_path: Path) -> None:
    """Neither table nor view catalog exists → CatalogFileMissingError."""
    fqn = "dbo.my_table"
    _seed_spec(tmp_path, fqn, {"unit_tests": []})
    # No catalog created

    with pytest.raises(CatalogFileMissingError):
        run_write_test_gen(
            project_root=tmp_path,
            table_fqn=fqn,
            branches=3,
            unit_tests=5,
            coverage="complete",
        )


# ── View autodetect ───────────────────────────────────────────────────────────


def test_write_test_gen_view_autodetect(tmp_path: Path) -> None:
    """View catalog exists (no table catalog) → writes test_gen to view catalog."""
    fqn = "dbo.my_view"
    _seed_catalog(tmp_path, fqn, kind="views")
    _seed_spec(tmp_path, fqn, {"unit_tests": [{"name": "v1"}]})

    result = run_write_test_gen(
        project_root=tmp_path,
        table_fqn=fqn,
        branches=2,
        unit_tests=4,
        coverage="partial",
    )

    assert result["status"] == "ok"
    assert "views" in result["catalog_path"]

    cat = json.loads((tmp_path / "catalog" / "views" / f"{fqn}.json").read_text())
    assert cat["test_gen"]["status"] == "ok"
    assert cat["test_gen"]["branches"] == 2
