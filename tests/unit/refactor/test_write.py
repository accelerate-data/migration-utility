from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from shared import refactor
from shared.loader import CatalogFileMissingError
from shared.output_models.refactor import RefactorWriteOutput
from tests.unit.refactor.helpers import (
    _compare_sql_result,
    _make_writable_copy,
    _semantic_review,
)

_cli_runner = CliRunner()


def test_write_happy_path() -> None:
    """Write merges refactor section into the writer procedure's catalog."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root, "silver.DimCustomer",
            extracted_sql="SELECT CustomerID, FirstName FROM [bronze].[CustomerRaw]",
            refactored_sql="WITH src AS (SELECT * FROM [bronze].[CustomerRaw]) SELECT CustomerID, FirstName FROM src",
            semantic_review=_semantic_review(),
            compare_sql_result=_compare_sql_result(),
        )
        assert isinstance(result, RefactorWriteOutput)
        assert result.ok is True
        assert result.table == "silver.dimcustomer"
        assert result.writer == "dbo.usp_load_dimcustomer"
        assert result.status == "ok"

        # Verify procedure catalog was updated (not table catalog)
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["status"] == "ok"
        assert "CustomerID" in proc_cat["refactor"]["extracted_sql"]
        assert "SELECT CustomerID, FirstName FROM src" in proc_cat["refactor"]["refactored_sql"]
        assert proc_cat["refactor"]["semantic_review"]["passed"] is True
        assert proc_cat["refactor"]["compare_sql"]["passed"] is True

        # Table catalog should NOT have a refactor section
        table_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        table_cat = json.loads(table_path.read_text())
        assert "refactor" not in table_cat

def test_write_both_sql_yields_ok_status() -> None:
    """Write with both SQL non-empty sets status to ok."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root,
            "silver.DimCustomer",
            "SELECT 1",
            "SELECT 1",
            semantic_review=_semantic_review(),
            compare_sql_result=_compare_sql_result(),
        )
        assert result.ok is True
        assert result.status == "ok"
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["status"] == "ok"

def test_write_only_extracted_yields_partial() -> None:
    """Write with only extracted SQL sets status to partial."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "", semantic_review=_semantic_review(), compare_sql_result=_compare_sql_result())
        assert result.ok is True
        assert result.status == "partial"
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["status"] == "partial"

def test_write_both_empty_yields_error() -> None:
    """Write with both SQL empty sets status to error."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(root, "silver.DimCustomer", "", "", semantic_review=_semantic_review(), compare_sql_result=_compare_sql_result())
        assert result.ok is True
        assert result.status == "error"
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["status"] == "error"

def test_write_harness_mode_persists_partial_even_when_semantic_review_passes() -> None:
    """Logical-only proof without compare-sql cannot produce ok."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root,
            "silver.DimCustomer",
            "SELECT 1",
            "WITH src AS (SELECT 1) SELECT * FROM src",
            semantic_review=_semantic_review(),
            compare_required=False,
        )
        assert result.status == "partial"
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["compare_sql"]["required"] is False
        assert proc_cat["refactor"]["compare_sql"]["executed"] is False
        assert proc_cat["refactor"]["status"] == "partial"

def test_write_failed_compare_sql_persists_partial() -> None:
    """Executable compare failure must block ok status."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root,
            "silver.DimCustomer",
            "SELECT 1",
            "WITH src AS (SELECT 1) SELECT * FROM src",
            semantic_review=_semantic_review(),
            compare_sql_result=_compare_sql_result(passed=False),
        )
        assert result.status == "partial"
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["compare_sql"]["passed"] is False
        assert proc_cat["refactor"]["compare_sql"]["failed_scenarios"] == ["scenario_b"]

def test_write_failed_semantic_review_persists_partial() -> None:
    """Semantic review issues must block ok status."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root,
            "silver.DimCustomer",
            "SELECT 1",
            "WITH src AS (SELECT 1) SELECT * FROM src",
            semantic_review=_semantic_review(
                passed=False,
                issues=[{"code": "EQUIVALENCE_PARTIAL", "message": "filter predicate changed", "severity": "warning"}],
            ),
            compare_sql_result=_compare_sql_result(),
        )
        assert result.status == "partial"
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["refactor"]["semantic_review"]["passed"] is False

def test_write_rejects_write_keywords_in_extracted_sql() -> None:
    """Write rejects extracted SQL that still contains DML write keywords."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="extracted_sql must be a pure SELECT"):
            refactor.run_write(root, "silver.DimCustomer", "UPDATE dbo.t SET x = 1", "WITH src AS (SELECT 1) SELECT * FROM src")

def test_write_rejects_write_keywords_in_refactored_sql() -> None:
    """Write rejects refactored SQL that still contains DML write keywords."""
    tmp, root = _make_writable_copy()
    with tmp:
        with pytest.raises(ValueError, match="refactored_sql must be a pure SELECT"):
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "WITH src AS (SELECT 1) UPDATE dbo.t SET x = 1")

def test_write_missing_procedure_catalog() -> None:
    """Write raises CatalogFileMissingError when procedure catalog is absent."""
    tmp, root = _make_writable_copy()
    with tmp:
        # Remove the procedure catalog file
        proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
        proc_path.unlink()
        with pytest.raises(CatalogFileMissingError):
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "SELECT 1")

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
            refactor.run_write(root, "silver.DimCustomer", "SELECT 1", "SELECT 1")

def test_cli_write_success() -> None:
    """CLI write command returns success JSON."""
    tmp, root = _make_writable_copy()
    with tmp:
        # resolve_project_root requires a git repo
        import subprocess
        subprocess.run(["git", "init", str(root)], capture_output=True, check=True)
        semantic_path = root / "semantic.json"
        compare_path = root / "compare.json"
        semantic_path.write_text(json.dumps(_semantic_review()), encoding="utf-8")
        compare_path.write_text(json.dumps(_compare_sql_result()), encoding="utf-8")
        result = _cli_runner.invoke(
            refactor.app,
            [
                "write",
                "--table", "silver.DimCustomer",
                "--extracted-sql", "SELECT CustomerID FROM [bronze].[CustomerRaw]",
                "--refactored-sql", "WITH src AS (SELECT 1) SELECT * FROM src",
                "--semantic-review-file", str(semantic_path),
                "--compare-sql-file", str(compare_path),
                "--project-root", str(root),
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["ok"] is True
        assert data["status"] == "ok"

def test_cli_write_validation_failure() -> None:
    """CLI write surfaces validation failures for non-SELECT refactored SQL."""
    tmp, root = _make_writable_copy()
    with tmp:
        import subprocess
        subprocess.run(["git", "init", str(root)], capture_output=True, check=True)
        result = _cli_runner.invoke(
            refactor.app,
            [
                "write",
                "--table", "silver.DimCustomer",
                "--extracted-sql", "SELECT CustomerID FROM [bronze].[CustomerRaw]",
                "--refactored-sql", "UPDATE dbo.t SET x = 1",
                "--project-root", str(root),
            ],
        )
        assert result.exit_code == 1
        assert "refactored_sql must be a pure SELECT" in result.output

def test_write_view_happy_path() -> None:
    """run_write auto-detects a view and writes to the view catalog."""
    tmp, root = _make_writable_copy()
    with tmp:
        result = refactor.run_write(
            root, "silver.vw_active_customers",
            extracted_sql="SELECT c.CustomerID FROM bronze.CustomerRaw c WHERE c.IsActive = 1",
            refactored_sql="WITH src AS (SELECT * FROM bronze.CustomerRaw WHERE IsActive = 1) SELECT CustomerID FROM src",
            semantic_review=_semantic_review(),
            compare_sql_result=_compare_sql_result(),
        )
        assert isinstance(result, RefactorWriteOutput)
        assert result.ok is True
        assert result.table == "silver.vw_active_customers"
        assert result.object_type == "view"

        # Verify view catalog was updated (not procedure catalog)
        view_path = root / "catalog" / "views" / "silver.vw_active_customers.json"
        view_cat = json.loads(view_path.read_text())
        assert view_cat["refactor"]["status"] == "ok"
        assert view_cat["refactor"]["semantic_review"]["passed"] is True
        assert "CustomerID" in view_cat["refactor"]["extracted_sql"]
