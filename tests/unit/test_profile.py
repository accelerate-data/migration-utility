"""Tests for profile.py -- profiling context assembly and catalog write-back.

Tests import shared.profile core functions directly (not via subprocess) to keep
execution fast and test coverage clear.  Run via uv to ensure shared is
importable: uv run --project <shared> pytest tests/ad-migration/migration/
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from shared import profile
from shared.loader import CatalogFileMissingError, CatalogLoadError

_cli_runner = CliRunner()

_TESTS_DIR = Path(__file__).parent
_PROFILE_FIXTURES = _TESTS_DIR / "fixtures" / "profile"


def _make_writable_copy() -> tuple[tempfile.TemporaryDirectory, Path]:
    """Copy profile fixtures to a temp dir so write tests can mutate them."""
    tmp = tempfile.TemporaryDirectory()
    dst = Path(tmp.name) / "profile"
    shutil.copytree(_PROFILE_FIXTURES, dst)
    return tmp, dst


# ── Context: rich catalog signals ────────────────────────────────────────────


def test_context_rich_catalog_all_signals_present(assert_valid_schema) -> None:
    """Context with rich catalog returns all catalog signals."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    assert_valid_schema(result, "profile_context.json")
    assert result["table"] == "silver.factsales"
    assert result["writer"] == "dbo.usp_load_fact_sales"

    signals = result["catalog_signals"]
    assert len(signals["primary_keys"]) == 1
    assert signals["primary_keys"][0]["columns"] == ["sale_id"]
    assert len(signals["foreign_keys"]) == 1
    assert signals["foreign_keys"][0]["columns"] == ["customer_key"]
    assert len(signals["auto_increment_columns"]) == 1
    assert signals["auto_increment_columns"][0]["column"] == "sale_id"
    assert signals["change_capture"]["enabled"] is True
    assert signals["change_capture"]["mechanism"] == "cdc"
    assert len(signals["sensitivity_classifications"]) == 1
    assert signals["sensitivity_classifications"][0]["column"] == "customer_email"


def test_context_rich_catalog_columns() -> None:
    """Context includes column list from table catalog."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    col_names = [c["name"] for c in result["columns"]]
    assert "sale_id" in col_names
    assert "customer_key" in col_names
    assert "load_date" in col_names


def test_context_rich_catalog_writer_references() -> None:
    """Context includes writer procedure references."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    refs = result["writer_references"]
    table_refs = refs["tables"]["in_scope"]
    ref_names = [f"{t['schema']}.{t['name']}" for t in table_refs]
    assert any("FactSales" in n for n in ref_names)


def test_context_rich_catalog_proc_body() -> None:
    """Context includes proc body text."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.FactSales", "dbo.usp_load_fact_sales",
    )
    assert "INSERT INTO" in result["proc_body"]
    assert "silver.FactSales" in result["proc_body"]


# ── Context: bare catalog (no constraints) ───────────────────────────────────


def test_context_bare_catalog_empty_arrays(assert_valid_schema) -> None:
    """Context with bare catalog returns empty arrays, no errors."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimCustomer", "dbo.usp_merge_dim_customer",
    )
    assert_valid_schema(result, "profile_context.json")
    signals = result["catalog_signals"]
    assert signals["primary_keys"] == []
    assert signals["foreign_keys"] == []
    assert signals["auto_increment_columns"] == []
    assert signals["unique_indexes"] == []
    assert signals["change_capture"] is None
    assert signals["sensitivity_classifications"] == []


# ── Context: related procedures ──────────────────────────────────────────────


def test_context_related_procedures_included() -> None:
    """Context with writer that has EXEC chains includes related proc bodies."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimCustomer", "dbo.usp_merge_dim_customer",
    )
    related = result["related_procedures"]
    assert len(related) >= 1
    related_names = [r["procedure"] for r in related]
    assert "dbo.usp_helper_log" in related_names
    helper = next(r for r in related if r["procedure"] == "dbo.usp_helper_log")
    assert "proc_body" in helper
    assert "INSERT INTO" in helper["proc_body"]


# ── Context: error paths ────────────────────────────────────────────────────


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
        assert result["proc_body"] == ""
    finally:
        tmp.cleanup()


# ── Write: valid profile ─────────────────────────────────────────────────────


def test_write_valid_profile_merges() -> None:
    """Write valid profile merges into catalog file."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "rationale": "Pure INSERT with no UPDATE or DELETE.",
                "source": "llm",
            },
            "primary_key": {
                "columns": ["sale_id"],
                "primary_key_type": "surrogate",
                "source": "catalog",
            },
        }
        result = profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        assert result["ok"] is True

        # Verify catalog file was updated
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        cat = json.loads(cat_path.read_text(encoding="utf-8"))
        assert "profile" in cat
        assert cat["profile"]["status"] == "ok"
        assert cat["profile"]["classification"]["resolved_kind"] == "fact_transaction"
    finally:
        tmp.cleanup()


# ── Write: missing required field ────────────────────────────────────────────


def test_write_missing_required_field_raises() -> None:
    """Write with missing required field raises ValueError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "writer": "dbo.usp_load_fact_sales",
            # missing "status"
        }
        with pytest.raises(ValueError, match="validation failed"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()


# ── Write: invalid enum value ────────────────────────────────────────────────


def test_write_invalid_enum_raises() -> None:
    """Write with invalid enum value raises ValueError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "invalid_kind",
                "source": "llm",
            },
        }
        with pytest.raises(ValueError, match="validation failed"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()


def test_write_invalid_fk_type_raises() -> None:
    """Write with invalid FK type raises ValueError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "foreign_keys": [
                {
                    "column": "customer_key",
                    "fk_type": "invalid_type",
                    "source": "llm",
                }
            ],
        }
        with pytest.raises(ValueError, match="validation failed"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()


def test_write_invalid_suggested_action_raises() -> None:
    """Write with invalid suggested action raises ValueError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "pii_actions": [
                {
                    "column": "email",
                    "suggested_action": "encrypt",
                    "source": "llm",
                }
            ],
        }
        with pytest.raises(ValueError, match="validation failed"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()


def test_write_invalid_source_raises() -> None:
    """Write with invalid source enum raises ValueError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        bad_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "source": "invalid_source",
            },
        }
        with pytest.raises(ValueError, match="validation failed"):
            profile.run_write(ddl_path, "silver.FactSales", bad_profile)
    finally:
        tmp.cleanup()


# ── Write: nonexistent catalog ───────────────────────────────────────────────


def test_write_nonexistent_catalog_raises() -> None:
    """Write to nonexistent catalog file raises CatalogFileMissingError."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_nonexistent",
        }
        with pytest.raises(CatalogFileMissingError):
            profile.run_write(ddl_path, "dbo.NonexistentTable", valid_profile)
    finally:
        tmp.cleanup()


# ── Write: idempotent ────────────────────────────────────────────────────────


def test_write_idempotent() -> None:
    """Running write twice with the same profile produces identical catalog."""
    tmp, ddl_path = _make_writable_copy()
    try:
        valid_profile = {
            "status": "ok",
            "writer": "dbo.usp_load_fact_sales",
            "classification": {
                "resolved_kind": "fact_transaction",
                "rationale": "Pure INSERT.",
                "source": "llm",
            },
            "primary_key": {
                "columns": ["sale_id"],
                "primary_key_type": "surrogate",
                "source": "catalog",
            },
            "watermark": {
                "column": "load_date",
                "rationale": "WHERE load_date > @batch_date in proc.",
                "source": "llm",
            },
        }
        profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        first = cat_path.read_text(encoding="utf-8")

        profile.run_write(ddl_path, "silver.FactSales", valid_profile)
        second = cat_path.read_text(encoding="utf-8")

        assert first == second
    finally:
        tmp.cleanup()


# ── CLI: structured error JSON on failure ────────────────────────────────────


def test_write_cli_emits_error_json_on_validation_failure() -> None:
    """write CLI emits structured error JSON to stdout and exits 1 on validation failure."""
    tmp, ddl_path = _make_writable_copy()
    try:
        subprocess.run(["git", "init"], cwd=ddl_path, capture_output=True, check=True)
        bad_profile = json.dumps({"writer": "dbo.usp_load_fact_sales"})  # missing status
        result = _cli_runner.invoke(
            profile.app,
            ["write", "--project-root", str(ddl_path), "--table", "silver.FactSales", "--profile", bad_profile],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["ok"] is False
        assert "error" in output
        assert output["table"] == "silver.factsales"
    finally:
        tmp.cleanup()


# ── Corrupt catalog JSON tests ──────────────────────────────────────────


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


def test_write_corrupt_existing_table_catalog_exit_2() -> None:
    """write with corrupt existing table catalog exits code 2."""
    tmp, ddl_path = _make_writable_copy()
    try:
        subprocess.run(["git", "init"], cwd=ddl_path, capture_output=True, check=True)
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        cat_path.write_text("{truncated", encoding="utf-8")
        good_profile = json.dumps({"status": "ok", "writer": "dbo.usp_load_fact_sales"})
        result = _cli_runner.invoke(
            profile.app,
            ["write", "--project-root", str(ddl_path), "--table", "silver.FactSales", "--profile", good_profile],
        )
        assert result.exit_code == 2
    finally:
        tmp.cleanup()


def test_write_invalid_profile_json_arg_exit_2() -> None:
    """write with invalid JSON string argument exits code 2."""
    tmp, ddl_path = _make_writable_copy()
    try:
        subprocess.run(["git", "init"], cwd=ddl_path, capture_output=True, check=True)
        result = _cli_runner.invoke(
            profile.app,
            ["write", "--project-root", str(ddl_path), "--table", "silver.FactSales", "--profile", "{not json"],
        )
        assert result.exit_code == 2
    finally:
        tmp.cleanup()


# ── Context: TRUNCATE+INSERT pattern (statement 6) ──────────────────────────


def test_context_truncate_insert_proc_body(assert_valid_schema) -> None:
    """Context for a TRUNCATE+INSERT procedure includes both statements."""
    result = profile.run_context(
        _PROFILE_FIXTURES, "silver.DimProduct", "dbo.usp_truncate_insert_dim_product",
    )
    assert_valid_schema(result, "profile_context.json")
    assert result["table"] == "silver.dimproduct"
    assert result["writer"] == "dbo.usp_truncate_insert_dim_product"
    assert "TRUNCATE TABLE silver.DimProduct" in result["proc_body"]
    assert "INSERT INTO silver.DimProduct" in result["proc_body"]


# ── run_view_context ──────────────────────────────────────────────────────────


def test_view_context_object_types(assert_valid_schema) -> None:
    """object_type is stamped correctly on all reference buckets."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_Multi")
    assert_valid_schema(result, "view_profile_context.json")

    # references.tables in_scope → "table"
    for entry in result["references"]["tables"]["in_scope"]:
        assert entry["object_type"] == "table"
    # references.views in_scope → "view"
    for entry in result["references"]["views"]["in_scope"]:
        assert entry["object_type"] == "view"
    # references.functions in_scope → "function"
    for entry in result["references"]["functions"]["in_scope"]:
        assert entry["object_type"] == "function"
    # referenced_by.procedures in_scope → "procedure"
    for entry in result["referenced_by"]["procedures"]["in_scope"]:
        assert entry["object_type"] == "procedure"


def test_view_context_multi_sql_elements(assert_valid_schema) -> None:
    """sql_elements and logic_summary are surfaced from scoping."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_Multi")
    assert_valid_schema(result, "view_profile_context.json")
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "join" in element_types
    assert "aggregation" in element_types
    assert "group_by" in element_types
    assert "Joins FactSales" in result["logic_summary"]


def test_view_context_mv_includes_columns(assert_valid_schema) -> None:
    """Materialized views surface columns; is_materialized_view is True."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.mv_Monthly")
    assert_valid_schema(result, "view_profile_context.json")
    assert result["is_materialized_view"] is True
    col_names = [c["name"] for c in result["columns"]]
    assert "month_key" in col_names
    assert "total_amount" in col_names


def test_view_context_non_mv_no_columns() -> None:
    """Non-materialized view does not include columns key."""
    result = profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_Simple")
    assert "columns" not in result


def test_view_context_missing_catalog_raises() -> None:
    """Missing view catalog raises CatalogFileMissingError."""
    with pytest.raises(CatalogFileMissingError):
        profile.run_view_context(_PROFILE_FIXTURES, "silver.vw_NoExist")


def test_view_context_no_scoping_raises() -> None:
    """View catalog without scoping section raises ValueError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        (root / "catalog" / "views" / "silver.vw_noscope.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_NoScope",
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="scoping not completed"):
            profile.run_view_context(root, "silver.vw_NoScope")


def test_view_context_scoping_error_status_raises() -> None:
    """View with scoping.status=error raises ValueError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        (root / "catalog" / "views" / "silver.vw_err.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_Err",
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "scoping": {"status": "error", "sql_elements": None, "warnings": [], "errors": []},
            }),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="scoping not completed"):
            profile.run_view_context(root, "silver.vw_Err")


# ── run_write (view path) ─────────────────────────────────────────────────────

_VALID_VIEW_PROFILE = {
    "status": "ok",
    "classification": "stg",
    "rationale": "Single-source pass-through.",
    "source": "llm",
}


def test_write_view_profile_stg() -> None:
    """Valid stg profile is merged into view catalog."""
    tmp, root = _make_writable_copy()
    try:
        result = profile.run_write(root, "silver.vw_Simple", _VALID_VIEW_PROFILE)
        assert result["ok"] is True
        assert "views" in result["catalog_path"]
        written = json.loads((root / "catalog" / "views" / "silver.vw_simple.json").read_text(encoding="utf-8"))
        assert written["profile"]["classification"] == "stg"
    finally:
        tmp.cleanup()


def test_write_view_profile_mart() -> None:
    """Valid mart profile is merged into view catalog."""
    tmp, root = _make_writable_copy()
    try:
        mart_profile = {**_VALID_VIEW_PROFILE, "classification": "mart"}
        result = profile.run_write(root, "silver.vw_Simple", mart_profile)
        assert result["ok"] is True
        written = json.loads((root / "catalog" / "views" / "silver.vw_simple.json").read_text(encoding="utf-8"))
        assert written["profile"]["classification"] == "mart"
    finally:
        tmp.cleanup()


def test_write_view_profile_bad_classification_raises() -> None:
    """Invalid classification raises ValueError."""
    tmp, root = _make_writable_copy()
    try:
        bad = {**_VALID_VIEW_PROFILE, "classification": "dim_non_scd"}
        with pytest.raises(ValueError, match="invalid classification"):
            profile.run_write(root, "silver.vw_Simple", bad)
    finally:
        tmp.cleanup()


def test_write_view_profile_missing_field_raises() -> None:
    """Missing required field raises ValueError."""
    tmp, root = _make_writable_copy()
    try:
        bad = {"status": "ok", "classification": "stg", "source": "llm"}  # missing rationale
        with pytest.raises(ValueError, match="missing required field"):
            profile.run_write(root, "silver.vw_Simple", bad)
    finally:
        tmp.cleanup()


def test_write_view_profile_idempotent() -> None:
    """Writing the same profile twice leaves the catalog consistent."""
    tmp, root = _make_writable_copy()
    try:
        profile.run_write(root, "silver.vw_Simple", _VALID_VIEW_PROFILE)
        profile.run_write(root, "silver.vw_Simple", _VALID_VIEW_PROFILE)
        written = json.loads((root / "catalog" / "views" / "silver.vw_simple.json").read_text(encoding="utf-8"))
        assert written["profile"]["classification"] == "stg"
    finally:
        tmp.cleanup()


# ── check_view_scoping_analyzed ───────────────────────────────────────────────


def test_guard_view_scoping_passes() -> None:
    """Guard passes when scoping.status == analyzed."""
    from shared.guards import check_view_scoping_analyzed
    result = check_view_scoping_analyzed(_PROFILE_FIXTURES, "silver.vw_Simple")
    assert result["passed"] is True


def test_guard_view_scoping_not_analyzed() -> None:
    """Guard fails when scoping.status == error."""
    from shared.guards import check_view_scoping_analyzed
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        (root / "catalog" / "views" / "silver.vw_err.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_Err",
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "scoping": {"status": "error", "sql_elements": None, "warnings": [], "errors": []},
            }),
            encoding="utf-8",
        )
        result = check_view_scoping_analyzed(root, "silver.vw_Err")
        assert result["passed"] is False
        assert result["code"] == "VIEW_SCOPING_NOT_COMPLETED"


def test_guard_view_scoping_missing_section() -> None:
    """Guard fails when scoping key is absent from catalog."""
    from shared.guards import check_view_scoping_analyzed
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        (root / "catalog" / "views" / "silver.vw_noscope.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_NoScope",
                "references": {"tables": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        result = check_view_scoping_analyzed(root, "silver.vw_NoScope")
        assert result["passed"] is False
        assert result["code"] == "VIEW_SCOPING_NOT_COMPLETED"
