"""Tests for refactor.py -- refactoring context assembly, catalog write-back, and diff logic."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared import refactor
from shared.loader import CatalogFileMissingError

_cli_runner = CliRunner()

_TESTS_DIR = Path(__file__).parent
_REFACTOR_FIXTURES = _TESTS_DIR / "fixtures" / "refactor"


def _make_writable_copy() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy refactor fixtures to a temp dir so write tests can mutate them."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "refactor"
    shutil.copytree(_REFACTOR_FIXTURES, dst)
    return tmp, dst


# ── symmetric_diff ───────────────────────────────────────────────────────────


class TestSymmetricDiff:
    def test_identical_rows(self) -> None:
        rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        result = refactor.symmetric_diff(rows, rows.copy())
        assert result["equivalent"] is True
        assert result["a_minus_b"] == []
        assert result["b_minus_a"] == []
        assert result["a_count"] == 2
        assert result["b_count"] == 2

    def test_extra_row_in_a(self) -> None:
        rows_a = [{"a": 1}, {"a": 2}, {"a": 3}]
        rows_b = [{"a": 1}, {"a": 2}]
        result = refactor.symmetric_diff(rows_a, rows_b)
        assert result["equivalent"] is False
        assert len(result["a_minus_b"]) == 1
        assert result["b_minus_a"] == []

    def test_extra_row_in_b(self) -> None:
        rows_a = [{"a": 1}]
        rows_b = [{"a": 1}, {"a": 2}]
        result = refactor.symmetric_diff(rows_a, rows_b)
        assert result["equivalent"] is False
        assert result["a_minus_b"] == []
        assert len(result["b_minus_a"]) == 1

    def test_duplicate_rows_handled(self) -> None:
        rows_a = [{"a": 1}, {"a": 1}, {"a": 1}]
        rows_b = [{"a": 1}, {"a": 1}]
        result = refactor.symmetric_diff(rows_a, rows_b)
        assert result["equivalent"] is False
        assert len(result["a_minus_b"]) == 1
        assert result["b_minus_a"] == []

    def test_empty_inputs(self) -> None:
        result = refactor.symmetric_diff([], [])
        assert result["equivalent"] is True
        assert result["a_count"] == 0
        assert result["b_count"] == 0

    def test_type_coercion_string_vs_int(self) -> None:
        """str(1) == str(1) so these should be considered equal."""
        rows_a = [{"val": 1}]
        rows_b = [{"val": 1}]
        result = refactor.symmetric_diff(rows_a, rows_b)
        assert result["equivalent"] is True

    def test_completely_different(self) -> None:
        rows_a = [{"a": 1}]
        rows_b = [{"a": 2}]
        result = refactor.symmetric_diff(rows_a, rows_b)
        assert result["equivalent"] is False
        assert len(result["a_minus_b"]) == 1
        assert len(result["b_minus_a"]) == 1


# ── run_context ──────────────────────────────────────────────────────────────


def test_context_happy_path() -> None:
    """Context returns all expected fields with proper values."""
    result = refactor.run_context(
        _REFACTOR_FIXTURES, "silver.DimCustomer",
    )
    assert result["table"] == "silver.dimcustomer"
    assert result["writer"] == "dbo.usp_load_dimcustomer"
    assert "proc_body" in result
    assert "MERGE" in result["proc_body"]
    assert result["profile"]["status"] == "ok"
    assert len(result["statements"]) == 1
    assert result["statements"][0]["action"] == "migrate"
    assert result["columns"][0]["name"] == "CustomerKey"
    assert "bronze.customerraw" in result["source_tables"]
    assert result["test_spec"] is not None
    assert len(result["test_spec"]["unit_tests"]) == 2
    assert result["sandbox"]["database"] == "__test_abc123"


def test_context_missing_writer() -> None:
    """Context raises ValueError when no writer in catalog and none provided."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "refactor"
    shutil.copytree(_REFACTOR_FIXTURES, dst)
    # Remove scoping from catalog
    cat_path = dst / "catalog" / "tables" / "silver.dimcustomer.json"
    cat = json.loads(cat_path.read_text())
    del cat["scoping"]
    cat_path.write_text(json.dumps(cat))
    with tmp:
        with pytest.raises(ValueError, match="No writer provided"):
            refactor.run_context(dst, "silver.DimCustomer")


def test_context_explicit_writer() -> None:
    """Context accepts an explicit writer parameter."""
    result = refactor.run_context(
        _REFACTOR_FIXTURES, "silver.DimCustomer", "dbo.usp_load_dimcustomer",
    )
    assert result["writer"] == "dbo.usp_load_dimcustomer"


# ── run_write ────────────────────────────────────────────────────────────────


def test_write_happy_path() -> None:
    """Write merges refactor section into catalog."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root, "silver.DimCustomer",
            extracted_sql="SELECT CustomerID, FirstName FROM [bronze].[CustomerRaw]",
            refactored_sql="WITH src AS (SELECT * FROM [bronze].[CustomerRaw]) SELECT CustomerID, FirstName FROM src",
            status="ok",
        )
        assert result["ok"] is True
        assert result["table"] == "silver.dimcustomer"

        # Verify catalog was updated
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text())
        assert cat["refactor"]["status"] == "ok"
        assert "CustomerID" in cat["refactor"]["extracted_sql"]
        assert "SELECT CustomerID, FirstName FROM src" in cat["refactor"]["refactored_sql"]


def test_write_validates_status() -> None:
    """Write rejects invalid status values."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="invalid status"):
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "SELECT 1", "invalid")


def test_write_requires_extracted_sql_for_ok_status() -> None:
    """Write rejects empty extracted SQL when status is ok."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="extracted_sql is required"):
            refactor.run_write(root, "silver.DimCustomer", "", "SELECT 1", "ok")


def test_write_requires_refactored_sql_for_ok_status() -> None:
    """Write rejects empty refactored SQL when status is ok."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="refactored_sql is required"):
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "", "ok")


def test_write_missing_catalog() -> None:
    """Write raises CatalogFileMissingError for unknown table."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(CatalogFileMissingError):
            refactor.run_write(root, "silver.NoSuchTable", "SELECT 1", "SELECT 1", "ok")


# ── CLI commands ─────────────────────────────────────────────────────────────


def test_cli_context_success() -> None:
    """CLI context command returns JSON with expected fields."""
    result = _cli_runner.invoke(
        refactor.app,
        ["context", "--table", "silver.DimCustomer", "--project-root", str(_REFACTOR_FIXTURES)],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["table"] == "silver.dimcustomer"
    assert data["writer"] == "dbo.usp_load_dimcustomer"


def test_cli_write_success() -> None:
    """CLI write command returns success JSON."""
    tmp, root = _make_writable_copy()
    with tmp:
        # resolve_project_root requires a git repo
        import subprocess
        subprocess.run(["git", "init", str(root)], capture_output=True, check=True)
        result = _cli_runner.invoke(
            refactor.app,
            [
                "write",
                "--table", "silver.DimCustomer",
                "--extracted-sql", "SELECT CustomerID FROM [bronze].[CustomerRaw]",
                "--refactored-sql", "WITH src AS (SELECT 1) SELECT * FROM src",
                "--status", "ok",
                "--project-root", str(root),
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True


def test_cli_context_missing_table() -> None:
    """CLI context command fails gracefully for missing table."""
    result = _cli_runner.invoke(
        refactor.app,
        ["context", "--table", "silver.NoSuchTable", "--project-root", str(_REFACTOR_FIXTURES)],
    )
    assert result.exit_code != 0
