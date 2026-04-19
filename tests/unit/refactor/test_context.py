from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared import refactor
from shared.output_models.refactor import RefactorContextOutput
from tests.unit.refactor.helpers import (
    _REFACTOR_FIXTURES,
    _make_writable_copy,
)

_cli_runner = CliRunner()


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

def test_context_columns_expose_only_target_sql_type() -> None:
    """Refactor context hides source/debug/legacy type fields for target and source columns."""
    tmp, root = _make_writable_copy()
    with tmp:
        target_path = root / "catalog" / "tables" / "silver.dimcustomer.json"
        target_cat = json.loads(target_path.read_text(encoding="utf-8"))
        target_cat["columns"][0].update(
            {
                "type": "NUMBER",
                "data_type": "NUMBER(10,0)",
                "source_sql_type": "NUMBER(10,0)",
                "canonical_tsql_type": "INT",
                "sql_type": "INT",
            }
        )
        target_path.write_text(json.dumps(target_cat), encoding="utf-8")

        source_path = root / "catalog" / "tables" / "bronze.customerraw.json"
        source_path.write_text(
            json.dumps(
                {
                    "schema": "bronze",
                    "name": "customerraw",
                    "columns": [
                        {
                            "name": "CustomerID",
                            "type": "NUMBER",
                            "data_type": "NUMBER(10,0)",
                            "source_sql_type": "NUMBER(10,0)",
                            "canonical_tsql_type": "INT",
                            "sql_type": "INT",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        result = refactor.run_context(root, "silver.DimCustomer")

    assert result.columns[0]["sql_type"] == "INT"
    assert set(result.columns[0]) <= {"name", "sql_type", "is_nullable", "is_identity", "max_length", "precision", "scale"}
    source_column = result.source_columns["bronze.customerraw"][0]
    assert source_column["sql_type"] == "INT"
    assert "source_sql_type" not in source_column
    assert "canonical_tsql_type" not in source_column
    assert "data_type" not in source_column
    assert "type" not in source_column

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

def test_cli_context_missing_table() -> None:
    """CLI context command fails gracefully for missing table."""
    result = _cli_runner.invoke(
        refactor.app,
        ["context", "--table", "silver.NoSuchTable", "--project-root", str(_REFACTOR_FIXTURES)],
    )
    assert result.exit_code != 0

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
