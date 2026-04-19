from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared import profile
from shared.loader import CatalogFileMissingError, CatalogLoadError
from shared.output_models.profile import ProfileContext
from tests.unit.profile.helpers import _PROFILE_FIXTURES, _make_writable_copy

_cli_runner = CliRunner()


def test_context_rich_catalog_all_signals_present() -> None:
    """Context with rich catalog returns all catalog signals."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    assert isinstance(result, ProfileContext)
    assert result.table == "silver.factsales"
    assert result.writer == "dbo.usp_load_fact_sales"

    signals = result.catalog_signals
    assert len(signals.primary_keys) == 1
    assert signals.primary_keys[0].columns == ["sale_id"]
    assert len(signals.foreign_keys) == 1
    assert signals.foreign_keys[0].columns == ["customer_key"]
    assert len(signals.auto_increment_columns) == 1
    assert signals.auto_increment_columns[0].column == "sale_id"
    assert signals.change_capture.enabled is True
    assert signals.change_capture.mechanism == "cdc"
    assert len(signals.sensitivity_classifications) == 1
    assert signals.sensitivity_classifications[0].column == "customer_email"

def test_context_rich_catalog_columns() -> None:
    """Context includes column list from table catalog."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    col_names = [c.name for c in result.columns]
    assert "sale_id" in col_names
    assert "customer_key" in col_names
    assert "load_date" in col_names

def test_context_columns_expose_only_target_sql_type() -> None:
    """Context columns hide source/debug/legacy type fields from prompt inputs."""
    tmp, root = _make_writable_copy()
    with tmp:
        table_path = root / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(table_path.read_text(encoding="utf-8"))
        cat["columns"][0].update(
            {
                "type": "NUMBER",
                "data_type": "NUMBER(10,0)",
                "source_sql_type": "NUMBER(10,0)",
                "canonical_tsql_type": "INT",
                "sql_type": "INT",
            }
        )
        table_path.write_text(json.dumps(cat), encoding="utf-8")

        result = profile.run_context(root, "silver.FactSales", "dbo.usp_load_fact_sales")

    column = result.columns[0].model_dump(exclude_none=True)
    assert column["sql_type"] == "INT"
    assert "source_sql_type" not in column
    assert "canonical_tsql_type" not in column
    assert "data_type" not in column
    assert "type" not in column

def test_context_rich_catalog_writer_references() -> None:
    """Context includes writer procedure references."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    refs = result.writer_references
    table_refs = refs.tables.in_scope
    ref_names = [f"{t.object_schema}.{t.name}" for t in table_refs]
    assert any("FactSales" in n for n in ref_names)

def test_context_rich_catalog_proc_body() -> None:
    """Context includes proc body text."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    assert "INSERT INTO" in result.proc_body
    assert "silver.FactSales" in result.proc_body

def test_context_bare_catalog_empty_arrays() -> None:
    """Context with bare catalog returns empty arrays, no errors."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimCustomer", "dbo.usp_merge_dim_customer",
    )
    assert isinstance(result, ProfileContext)
    signals = result.catalog_signals
    assert signals.primary_keys == []
    assert signals.foreign_keys == []
    assert signals.auto_increment_columns == []
    assert signals.unique_indexes == []
    assert signals.change_capture is None
    assert signals.sensitivity_classifications == []

def test_context_related_procedures_included() -> None:
    """Context with writer that has EXEC chains includes related proc bodies."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimCustomer", "dbo.usp_merge_dim_customer",
    )
    related = result.related_procedures
    assert len(related) >= 1
    related_names = [r.procedure for r in related]
    assert "dbo.usp_helper_log" in related_names
    helper = next(r for r in related if r.procedure == "dbo.usp_helper_log")
    assert helper.proc_body is not None
    assert "INSERT INTO" in helper.proc_body

def test_context_missing_table_catalog_raises() -> None:
    """Context with nonexistent table raises CatalogFileMissingError."""
    with pytest.raises(CatalogFileMissingError):
        profile.run_context(
            _PROFILE_FIXTURES, "dbo.NonexistentTable", "dbo.usp_load_fact_sales",
        )

def test_context_missing_proc_catalog_raises() -> None:
    """Context with nonexistent writer proc raises CatalogFileMissingError."""
    with pytest.raises(CatalogFileMissingError):
        profile.run_context(
            _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_nonexistent_proc",
        )

def test_context_missing_proc_body_returns_empty_string() -> None:
    """Context with valid catalog but missing DDL body returns empty proc_body."""
    tmp, ddl_path = _make_writable_copy()
    try:
        # Create a proc catalog file for a proc that has no DDL body
        proc_dir = ddl_path / "catalog" / "procedures"
        proc_dir.mkdir(parents=True, exist_ok=True)
        ghost_proc = {
            "schema": "dbo",
            "name": "usp_ghost",
            "references": {"tables": {"in_scope": []}, "procedures": {"in_scope": []}},
        }
        (proc_dir / "dbo.usp_ghost.json").write_text(
            json.dumps(ghost_proc, indent=2), encoding="utf-8",
        )
        result = profile.run_context(ddl_path, "silver.FactSales", "dbo.usp_ghost")
        assert result.proc_body == ""
    finally:
        tmp.cleanup()

def test_context_rejected_for_seed_table() -> None:
    """Seed tables do not assemble writer-driven profiling context."""
    tmp, root = _make_writable_copy()
    with tmp:
        cat_path = root / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        cat["is_seed"] = True
        cat["is_source"] = False
        cat_path.write_text(json.dumps(cat), encoding="utf-8")

        with pytest.raises(ValueError, match="seed table"):
            profile.run_context(root, "silver.FactSales")

def test_context_corrupt_table_catalog_raises() -> None:
    """context with corrupt table catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "ddl").mkdir()
        (root / "ddl" / "tables.sql").write_text("CREATE TABLE silver.FactSales (Id INT)\nGO\n", encoding="utf-8")
        (root / "ddl" / "procedures.sql").write_text("CREATE PROCEDURE dbo.usp_load AS SELECT 1\nGO\n", encoding="utf-8")
        (root / "catalog" / "tables").mkdir(parents=True)
        (root / "catalog" / "tables" / "silver.factsales.json").write_text("{truncated", encoding="utf-8")
        (root / "catalog" / "procedures").mkdir(parents=True)
        (root / "catalog" / "procedures" / "dbo.usp_load.json").write_text(
            '{"references":{"tables":{"in_scope":[{"schema":"silver","name":"FactSales","is_selected":false,"is_updated":true}],"out_of_scope":[]},"views":{"in_scope":[],"out_of_scope":[]},"functions":{"in_scope":[],"out_of_scope":[]},"procedures":{"in_scope":[],"out_of_scope":[]}}}',
            encoding="utf-8",
        )
        with pytest.raises(CatalogLoadError):
            profile.run_context(root, "silver.FactSales", "dbo.usp_load")

def test_context_corrupt_proc_catalog_raises() -> None:
    """context with corrupt procedure catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "ddl").mkdir()
        (root / "ddl" / "tables.sql").write_text("CREATE TABLE silver.T (Id INT)\nGO\n", encoding="utf-8")
        (root / "ddl" / "procedures.sql").write_text("CREATE PROCEDURE dbo.usp_load AS SELECT 1\nGO\n", encoding="utf-8")
        (root / "catalog" / "tables").mkdir(parents=True)
        (root / "catalog" / "tables" / "silver.t.json").write_text(
            '{"columns":[],"primary_keys":[],"unique_indexes":[],"foreign_keys":[],"auto_increment_columns":[],"change_capture":null,"sensitivity_classifications":[],"referenced_by":{"procedures":{"in_scope":[],"out_of_scope":[]},"views":{"in_scope":[],"out_of_scope":[]},"functions":{"in_scope":[],"out_of_scope":[]}}}',
            encoding="utf-8",
        )
        (root / "catalog" / "procedures").mkdir(parents=True)
        (root / "catalog" / "procedures" / "dbo.usp_load.json").write_text("{truncated", encoding="utf-8")
        with pytest.raises(CatalogLoadError):
            profile.run_context(root, "silver.T", "dbo.usp_load")

def test_context_truncate_insert_proc_body() -> None:
    """Context for a TRUNCATE+INSERT procedure includes both statements."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimProduct", "dbo.usp_truncate_insert_dim_product",
    )
    assert isinstance(result, ProfileContext)
    assert result.table == "silver.dimproduct"
    assert result.writer == "dbo.usp_truncate_insert_dim_product"
    assert "TRUNCATE TABLE silver.DimProduct" in result.proc_body
    assert "INSERT INTO silver.DimProduct" in result.proc_body

class TestContextWriterSlice:

    def test_run_context_uses_selected_writer_slice_without_full_proc_body(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Sliced writers expose only the selected table slice to LLM-facing context."""
        tmp, root = _make_writable_copy()
        try:
            proc_path = root / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
            proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
            proc_cat["table_slices"] = {
                "silver.factsales": "MERGE INTO silver.FactSales AS tgt USING bronze.SalesRaw AS src ON tgt.sale_id = src.sale_id"
            }
            proc_cat["references"]["tables"]["in_scope"].append(
                {"schema": "bronze", "name": "Unrelated", "is_selected": True, "is_updated": False}
            )
            proc_path.write_text(json.dumps(proc_cat), encoding="utf-8")
            monkeypatch.setattr(
                profile,
                "load_ddl",
                lambda *_args, **_kwargs: pytest.fail("selected slice context must not load full DDL"),
            )

            result = profile.run_context(root, "silver.FactSales", "dbo.usp_load_fact_sales")
            assert isinstance(result, ProfileContext)
            assert result.selected_writer_ddl_slice.startswith("MERGE INTO silver.FactSales")
            assert result.proc_body == ""
            refs = result.writer_references.tables.in_scope
            assert [(ref.object_schema, ref.name, ref.is_selected, ref.is_updated) for ref in refs] == [
                ("bronze", "salesraw", True, False),
            ]
            assert not hasattr(result, "writer_ddl_slice")
        finally:
            tmp.cleanup()

    def test_run_context_selected_writer_slice_absent_for_unsliced_writer(self) -> None:
        """Unsliced writers keep full proc_body and no selected slice."""
        result = profile.run_context(
            _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
        )
        assert isinstance(result, ProfileContext)
        assert result.selected_writer_ddl_slice is None
        assert result.proc_body
        assert not hasattr(result, "writer_ddl_slice")

    def test_run_context_missing_selected_writer_slice_raises(self) -> None:
        """A sliced writer without a target-table slice is not safe LLM context."""
        tmp, root = _make_writable_copy()
        try:
            proc_path = root / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
            proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
            proc_cat["table_slices"] = {"silver.other": "MERGE INTO silver.Other ..."}
            proc_path.write_text(json.dumps(proc_cat), encoding="utf-8")

            with pytest.raises(ValueError, match="no slice exists for target silver\\.factsales"):
                profile.run_context(root, "silver.FactSales", "dbo.usp_load_fact_sales")
        finally:
            tmp.cleanup()
