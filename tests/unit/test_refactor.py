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


def test_context_happy_path(assert_valid_schema) -> None:
    """Context returns all expected fields with proper values."""
    result = refactor.run_context(
        _REFACTOR_FIXTURES, "silver.DimCustomer",
    )
    assert_valid_schema(result, "refactor_context_output.json")
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


def test_write_happy_path(assert_valid_schema) -> None:
    """Write merges refactor section into the writer procedure's catalog."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root, "silver.DimCustomer",
            extracted_sql="SELECT CustomerID, FirstName FROM [bronze].[CustomerRaw]",
            refactored_sql="WITH src AS (SELECT * FROM [bronze].[CustomerRaw]) SELECT CustomerID, FirstName FROM src",
            status="ok",
        )
        assert_valid_schema(result, "refactor_write_output.json")
        assert result["ok"] is True
        assert result["table"] == "silver.dimcustomer"
        assert result["writer"] == "dbo.usp_load_dimcustomer"

        # Verify procedure catalog was updated (not table catalog)
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["status"] == "ok"
        assert "CustomerID" in proc_cat["refactor"]["extracted_sql"]
        assert "SELECT CustomerID, FirstName FROM src" in proc_cat["refactor"]["refactored_sql"]

        # Table catalog should NOT have a refactor section
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        table_cat = json.loads(table_path.read_text())
        assert "refactor" not in table_cat


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


def test_write_missing_procedure_catalog() -> None:
    """Write raises CatalogFileMissingError when procedure catalog is absent."""
    tmp, root = _make_writable_copy()
    with tmp:
        # Remove the procedure catalog file
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_path.unlink()
        with pytest.raises(CatalogFileMissingError):
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "SELECT 1", "ok")


def test_write_no_selected_writer() -> None:
    """Write raises ValueError when table catalog has no selected_writer."""
    tmp, root = _make_writable_copy()
    with tmp:
        # Remove scoping from table catalog
        cat_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        cat = json.loads(cat_path.read_text())
        del cat["scoping"]
        cat_path.write_text(json.dumps(cat))
        with pytest.raises(ValueError, match="No scoping.selected_writer"):
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "SELECT 1", "ok")


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


# ── View auto-detection in run_context ────────────────────────────────────────


def test_context_view_auto_detect() -> None:
    """run_context auto-detects a view FQN and returns view-specific fields."""
    result = refactor.run_context(_REFACTOR_FIXTURES, "silver.vw_active_customers")
    assert result["table"] == "silver.vw_active_customers"
    assert result["object_type"] == "view"
    assert "view_sql" in result
    assert "CustomerID" in result["view_sql"]
    assert "writer" not in result
    assert "proc_body" not in result
    assert "statements" not in result
    assert result["profile"]["status"] == "ok"
    assert result["columns"][0]["name"] == "CustomerID"
    assert "bronze.customerraw" in result["source_tables"]


def test_context_view_missing_profile() -> None:
    """run_context raises ValueError when view catalog has no profile."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
        cat = json.loads(cat_path.read_text())
        del cat["profile"]
        cat_path.write_text(json.dumps(cat))
        with pytest.raises(ValueError, match="no 'profile' section"):
            refactor.run_context(root, "silver.vw_active_customers")


def test_context_view_missing_sql() -> None:
    """run_context raises ValueError when view catalog has no sql."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
        cat = json.loads(cat_path.read_text())
        del cat["sql"]
        cat_path.write_text(json.dumps(cat))
        with pytest.raises(ValueError, match="no 'sql' key"):
            refactor.run_context(root, "silver.vw_active_customers")


def test_cli_context_view_success() -> None:
    """CLI context command returns view-specific JSON when given a view FQN."""
    result = _cli_runner.invoke(
        refactor.app,
        ["context", "--table", "silver.vw_active_customers", "--project-root", str(_REFACTOR_FIXTURES)],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["object_type"] == "view"
    assert "view_sql" in data
    assert "writer" not in data


# ── View auto-detection in run_write ──────────────────────────────────────────


def test_write_view_happy_path() -> None:
    """run_write auto-detects a view and writes to the view catalog."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root, "silver.vw_active_customers",
            extracted_sql="SELECT c.CustomerID FROM bronze.CustomerRaw c WHERE c.IsActive = 1",
            refactored_sql="WITH src AS (SELECT * FROM bronze.CustomerRaw WHERE IsActive = 1) SELECT CustomerID FROM src",
            status="ok",
        )
        assert result["ok"] is True
        assert result["table"] == "silver.vw_active_customers"
        assert result.get("object_type") == "view"

        # Verify view catalog was updated (not procedure catalog)
        view_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
        view_cat = json.loads(view_path.read_text())
        assert view_cat["refactor"]["status"] == "ok"
        assert "CustomerID" in view_cat["refactor"]["extracted_sql"]


# ── run_sweep ────────────────────────────────────────────────────────────────


class TestSweep:
    """Tests for the refactor sweep planning function."""

    def test_all_absent_status(self) -> None:
        """Tables with no refactor status get recommended_action='refactor'."""
        result = refactor.run_sweep(
            _REFACTOR_FIXTURES, ["silver.DimCustomer"],
        )
        obj = result["objects"][0]
        assert obj["fqn"] == "silver.dimcustomer"
        assert obj["object_type"] == "table"
        assert obj["refactor_status"] is None
        assert obj["recommended_action"] == "refactor"

    def test_mixed_statuses(self, assert_valid_schema) -> None:
        """ok=skip, partial=re-refactor, absent=refactor."""
        tmp, root = _make_writable_copy()
        with tmp:
            result = refactor.run_sweep(
                root,
                ["silver.FactSales", "silver.DimProduct", "silver.DimCustomer"],
            )
            assert_valid_schema(result, "refactor_sweep_output.json")

            by_fqn = {o["fqn"]: o for o in result["objects"]}
            assert by_fqn["silver.factsales"]["recommended_action"] == "skip"
            assert by_fqn["silver.factsales"]["refactor_status"] == "ok"
            assert by_fqn["silver.dimproduct"]["recommended_action"] == "re-refactor"
            assert by_fqn["silver.dimproduct"]["refactor_status"] == "partial"
            assert by_fqn["silver.dimcustomer"]["recommended_action"] == "refactor"
            assert by_fqn["silver.dimcustomer"]["refactor_status"] is None

    def test_view_with_ok_status(self) -> None:
        """View with refactor.status=ok gets skip recommendation."""
        tmp, root = _make_writable_copy()
        with tmp:
            view_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
            view_cat = json.loads(view_path.read_text())
            view_cat["refactor"] = {"status": "ok", "extracted_sql": "SELECT 1", "refactored_sql": "SELECT 1"}
            view_path.write_text(json.dumps(view_cat))

            result = refactor.run_sweep(root, ["silver.vw_active_customers"])
            obj = result["objects"][0]
            assert obj["object_type"] == "view"
            assert obj["recommended_action"] == "skip"
            assert obj["writer"] is None

    def test_shared_staging_detected(self) -> None:
        """Source tables referenced by 2+ non-skip FQNs are shared staging candidates."""
        tmp, root = _make_writable_copy()
        with tmp:
            result = refactor.run_sweep(
                root, ["silver.DimCustomer", "silver.DimProduct"],
            )
            assert "bronze.customerraw" in result["shared_staging_candidates"]

    def test_single_table_no_shared_staging(self) -> None:
        """Single table sweep has no shared staging candidates."""
        tmp, root = _make_writable_copy()
        with tmp:
            result = refactor.run_sweep(root, ["silver.DimCustomer"])
            assert result["shared_staging_candidates"] == []

    def test_existing_dbt_models_detected(self) -> None:
        """Sweep detects existing staging and mart models on disk."""
        result = refactor.run_sweep(
            _REFACTOR_FIXTURES, ["silver.FactSales"],
        )
        obj = result["objects"][0]
        assert "stg_customerraw.sql" in obj["existing_stg_models"]
        assert obj["existing_mart_model"] is not None
        assert "factsales.sql" in obj["existing_mart_model"]

    def test_shared_sources_persisted(self) -> None:
        """Sweep persists shared_sources on affected catalog entries."""
        tmp, root = _make_writable_copy()
        with tmp:
            refactor.run_sweep(
                root, ["silver.DimCustomer", "silver.DimProduct"],
            )
            proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
            proc_cat = json.loads(proc_path.read_text())
            assert "bronze.customerraw" in proc_cat["refactor"]["shared_sources"]

            proc_path2 = root / "catalog" / "procedures" / "dbo.usp_load_dimproduct.json"
            proc_cat2 = json.loads(proc_path2.read_text())
            assert "bronze.customerraw" in proc_cat2["refactor"]["shared_sources"]

    def test_skip_objects_excluded_from_shared_staging(self) -> None:
        """Objects with skip recommendation don't contribute to shared staging detection."""
        result = refactor.run_sweep(
            _REFACTOR_FIXTURES,
            ["silver.FactSales", "silver.DimCustomer"],
        )
        assert "bronze.customerraw" not in result["shared_staging_candidates"]


# ── CLI sweep command ────────────────────────────────────────────────────────


def test_cli_sweep_success() -> None:
    """CLI sweep command returns valid JSON output."""
    result = _cli_runner.invoke(
        refactor.app,
        ["sweep", "--tables", "silver.DimCustomer", "--tables", "silver.FactSales",
         "--project-root", str(_REFACTOR_FIXTURES)],
    )
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "objects" in data
    assert len(data["objects"]) == 2
