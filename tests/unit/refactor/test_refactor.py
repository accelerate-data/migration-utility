"""Tests for refactor.py -- refactoring context assembly, catalog write-back, and diff logic."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from shared import refactor
from shared.catalog_models import (
    CompareSqlSummary,
    RefactorSection,
    SemanticCheck,
    SemanticChecks,
    SemanticReview,
)
from shared.loader import CatalogFileMissingError
from shared.output_models.refactor import RefactorContextOutput, RefactorWriteOutput
from shared.output_models.sandbox import CompareSqlOutput, CompareSqlScenario

_cli_runner = CliRunner()

_TESTS_DIR = Path(__file__).parent
_REFACTOR_FIXTURES = _TESTS_DIR / "fixtures"


def _make_writable_copy() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy refactor fixtures to a temp dir so write tests can mutate them."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "refactor"
    shutil.copytree(_REFACTOR_FIXTURES, dst)
    return tmp, dst


def _semantic_review(*, passed: bool = True, issues: list[dict[str, object]] | None = None) -> dict[str, object]:
    return {
        "passed": passed,
        "checks": {
            "source_tables": {"passed": passed, "summary": "source tables match"},
            "output_columns": {"passed": passed, "summary": "output columns match"},
            "joins": {"passed": passed, "summary": "joins match"},
            "filters": {"passed": passed, "summary": "filters match"},
            "aggregation_grain": {"passed": passed, "summary": "aggregation grain matches"},
        },
        "issues": issues or [],
    }


def _compare_sql_result(*, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "sandbox_database": "SBX_ABC123000000",
        "total": 2,
        "passed": 2 if passed else 1,
        "failed": 0 if passed else 1,
        "results": [
            {"scenario_name": "scenario_a", "status": "ok", "equivalent": True, "a_count": 1, "b_count": 1, "a_minus_b": [], "b_minus_a": []},
            {
                "scenario_name": "scenario_b",
                "status": "ok" if passed else "error",
                "equivalent": passed,
                "a_count": 1,
                "b_count": 1,
                "a_minus_b": [] if passed else [{"CustomerID": "42"}],
                "b_minus_a": [],
                "errors": [] if passed else [{"code": "ROW_DIFF", "message": "rows differ"}],
            },
        ],
    }


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
    assert isinstance(result, RefactorContextOutput)
    assert result.table == "silver.dimcustomer"
    assert result.writer == "dbo.usp_load_dimcustomer"
    assert result.proc_body is not None
    assert "MERGE" in result.proc_body
    assert result.profile["status"] == "ok"
    assert len(result.statements) == 1
    assert result.statements[0]["action"] == "migrate"
    assert result.columns[0]["name"] == "CustomerKey"
    assert "bronze.customerraw" in result.source_tables
    assert result.test_spec is not None
    assert len(result.test_spec["unit_tests"]) == 2
    assert result.sandbox["database"] == "SBX_ABC123000000"


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
    assert result.writer == "dbo.usp_load_dimcustomer"


# ── run_write ────────────────────────────────────────────────────────────────


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
    assert isinstance(result, RefactorContextOutput)
    assert result.table == "silver.vw_active_customers"
    assert result.object_type == "view"
    assert result.view_sql is not None
    assert "CustomerID" in result.view_sql
    assert result.writer is None
    assert result.proc_body is None
    assert result.statements is None
    assert result.profile["status"] == "ok"
    assert result.columns[0]["name"] == "CustomerID"
    assert "bronze.customerraw" in result.source_tables


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


# ── Context: selected_writer_ddl_slice ─────────────────────────────────────────


class TestContextWriterSlice:

    def test_run_context_uses_selected_writer_slice_without_full_proc_body(self) -> None:
        """Sliced writers expose only the selected table slice to LLM-facing context."""
        tmp, root = _make_writable_copy()
        with tmp:
            proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
            proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
            proc_cat["table_slices"] = {
                "silver.dimcustomer": (
                    "MERGE INTO silver.DimCustomer AS tgt "
                    "USING bronze.CustomerRaw AS src "
                    "ON tgt.CustomerID = src.CustomerID "
                    "WHEN MATCHED THEN UPDATE SET FirstName = src.FirstName"
                )
            }
            proc_cat["references"]["tables"]["in_scope"].append(
                {"schema": "bronze", "name": "Unrelated", "is_selected": True, "is_updated": False}
            )
            proc_path.write_text(json.dumps(proc_cat), encoding="utf-8")

            result = refactor.run_context(root, "silver.DimCustomer")
            assert isinstance(result, RefactorContextOutput)
            assert result.selected_writer_ddl_slice.startswith("MERGE INTO silver.DimCustomer")
            assert result.proc_body == ""
            assert result.statements == []
            assert result.source_tables == ["bronze.customerraw"]
            assert set(result.source_columns) == {"bronze.customerraw"}
            assert not hasattr(result, "writer_ddl_slice")

    def test_run_context_selected_writer_slice_absent_for_unsliced_writer(self) -> None:
        """Unsliced writers keep full proc_body and no selected slice."""
        result = refactor.run_context(_REFACTOR_FIXTURES, "silver.DimCustomer")
        assert isinstance(result, RefactorContextOutput)
        assert result.selected_writer_ddl_slice is None
        assert result.proc_body
        assert not hasattr(result, "writer_ddl_slice")

    def test_run_context_missing_selected_writer_slice_raises(self) -> None:
        """A sliced writer without a target-table slice is not safe LLM context."""
        tmp, root = _make_writable_copy()
        with tmp:
            proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
            proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
            proc_cat["table_slices"] = {"silver.other": "MERGE INTO silver.Other ..."}
            proc_path.write_text(json.dumps(proc_cat), encoding="utf-8")

            with pytest.raises(ValueError, match="no slice exists for target silver\\.dimcustomer"):
                refactor.run_context(root, "silver.DimCustomer")

    def test_run_context_selected_writer_slice_uses_manifest_dialect(self) -> None:
        """Selected-slice source extraction uses the project dialect, not a T-SQL default."""
        tmp, root = _make_writable_copy()
        with tmp:
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["technology"] = "oracle"
            manifest["dialect"] = "oracle"
            for role in ("source", "sandbox", "target"):
                manifest["runtime"][role]["technology"] = "oracle"
                manifest["runtime"][role]["dialect"] = "oracle"
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

            proc_path = root / "catalog" / "procedures" / "dbo.usp_load_dimcustomer.json"
            proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
            proc_cat["table_slices"] = {
                "silver.dimcustomer": """
                    INSERT INTO silver.DimCustomer (CustomerID)
                    SELECT CustomerID FROM bronze.CustomerRaw
                    MINUS
                    SELECT CustomerID FROM bronze.CustomerRejects
                """
            }
            proc_path.write_text(json.dumps(proc_cat), encoding="utf-8")

            result = refactor.run_context(root, "silver.DimCustomer")

            assert result.source_tables == ["bronze.customerraw", "bronze.customerrejects"]


# ── Pydantic model validation ─────────────────────────────────────────────────


class TestCatalogModels:
    """Tests for tightened RefactorSection and related models."""

    def test_semantic_review_valid(self) -> None:
        data = _semantic_review()
        model = SemanticReview.model_validate(data)
        assert model.passed is True
        assert model.checks.source_tables.passed is True
        assert model.checks.aggregation_grain.summary == "aggregation grain matches"

    def test_semantic_review_rejects_extra_field(self) -> None:
        data = _semantic_review()
        data["unexpected_field"] = "boom"
        with pytest.raises(ValidationError):
            SemanticReview.model_validate(data)

    def test_compare_sql_summary_valid(self) -> None:
        model = CompareSqlSummary(
            required=True, executed=True, passed=True,
            scenarios_total=2, scenarios_passed=2,
        )
        assert model.passed is True
        assert model.failed_scenarios == []

    def test_compare_sql_summary_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError):
            CompareSqlSummary(
                required=True, executed=True, passed=True,
                scenarios_total=2, scenarios_passed=2,
                bogus="nope",
            )

    def test_refactor_section_typed_fields(self) -> None:
        section = RefactorSection(
            status="ok",
            extracted_sql="SELECT 1",
            refactored_sql="WITH src AS (SELECT 1) SELECT * FROM src",
            semantic_review=SemanticReview.model_validate(_semantic_review()),
            compare_sql=CompareSqlSummary(
                required=True, executed=True, passed=True,
                scenarios_total=2, scenarios_passed=2,
            ),
        )
        assert section.semantic_review.passed is True
        assert section.compare_sql.scenarios_total == 2

    def test_refactor_section_rejects_extra_field(self) -> None:
        with pytest.raises(ValidationError):
            RefactorSection(status="ok", extra_junk="bad")


class TestOutputModels:
    """Tests for CLI output Pydantic models."""

    def test_refactor_context_output_rejects_extra(self) -> None:
        with pytest.raises(ValidationError):
            RefactorContextOutput(
                table="silver.t", profile={}, columns=[], source_tables=[],
                bogus="nope",
            )

    def test_refactor_write_output_success(self) -> None:
        model = RefactorWriteOutput(
            ok=True, table="silver.t", status="ok",
            writer="dbo.usp", catalog_path="/tmp/x.json",
        )
        assert model.ok is True
        assert model.writer == "dbo.usp"

    def test_refactor_write_output_failure(self) -> None:
        model = RefactorWriteOutput(
            ok=False, table="silver.t", error="something broke",
        )
        assert model.ok is False
        assert model.error == "something broke"

    def test_compare_sql_output_valid(self) -> None:
        data = _compare_sql_result()
        model = CompareSqlOutput.model_validate(data)
        assert model.total == 2
        assert len(model.results) == 2
        assert isinstance(model.results[0], CompareSqlScenario)

    def test_compare_sql_output_rejects_extra(self) -> None:
        data = _compare_sql_result()
        data["bogus"] = "nope"
        with pytest.raises(ValidationError):
            CompareSqlOutput.model_validate(data)
