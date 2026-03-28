"""Tests for discover.py — DDL object catalog CLI.

Tests import shared.discover core functions directly (not via subprocess) to keep
execution fast and test coverage clear.  Run via uv to ensure shared is
importable: uv run --project <shared> pytest tests/ad-migration/migration/
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import DdlParseError

_TESTS_DIR = Path(__file__).parent
_FLAT_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "flat"
_UNPARSEABLE_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "unparseable"


# ── test_list_flat_tables ──────────────────────────────────────────────────


def test_list_flat_tables() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables, "tsql")
    objects = result["objects"]
    assert "silver.dimproduct" in objects
    assert "bronze.product" in objects
    assert "bronze.customer" in objects
    assert "bronze.sales" in objects
    assert "bronze.salesorder" in objects
    assert "bronze.geography" in objects
    assert "bronze.runcontrol" in objects
    assert "dbo.config" in objects


# ── test_list_flat_procedures ─────────────────────────────────────────────


def test_list_flat_procedures() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.procedures, "tsql")
    objects = result["objects"]
    assert "dbo.usp_loaddimproduct" in objects
    assert "dbo.usp_logmessage" in objects
    assert "dbo.usp_mergedimproduct" in objects
    assert "dbo.usp_loadwithcte" in objects
    assert "dbo.usp_loadwithmulticte" in objects
    assert "dbo.usp_loadwithcase" in objects
    assert "dbo.usp_loadwithleftjoin" in objects
    assert "dbo.usp_conditionalmerge" in objects
    assert "dbo.usp_trycatchload" in objects
    assert "dbo.usp_correlatedsubquery" in objects


# ── test_list_flat_missing_optional ───────────────────────────────────────


def test_list_flat_missing_optional() -> None:
    """Directory with only tables.sql — views list returns empty without error."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        (p / "tables.sql").write_text(
            "CREATE TABLE dbo.SomeTable (Id INT)\nGO\n", encoding="utf-8"
        )
        result = discover.run_list(p, discover.ObjectType.views, "tsql")
    assert result["objects"] == []


# ── test_list_indexed_same_as_flat ────────────────────────────────────────


def test_list_indexed_same_as_flat() -> None:
    """Indexed dir returns same object names as flat dir."""
    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "indexed"
        index_directory(_FLAT_FIXTURES, out)

        flat_result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables, "tsql")
        indexed_result = discover.run_list(out, discover.ObjectType.tables, "tsql")

    assert flat_result["objects"] == indexed_result["objects"]


# ── test_list_unparseable_stored_with_error ──────────────────────────────


def test_list_unparseable_stored_with_error() -> None:
    """Unparseable DDL blocks are stored with parse_error, not skipped."""
    from shared.loader import load_directory

    result = load_directory(_UNPARSEABLE_FIXTURES)
    has_error = any(e.parse_error is not None for e in result.procedures.values())
    assert has_error


# ── test_show_table_columns ───────────────────────────────────────────────


def test_show_table_columns() -> None:
    """show on a table returns columns list populated from AST."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    assert result["type"] == "table"
    assert result["parse_error"] is None
    columns = result["columns"]
    assert isinstance(columns, list)
    col_names = [c["name"] for c in columns]
    assert "ProductKey" in col_names
    assert "ProductAlternateKey" in col_names
    assert "EnglishProductName" in col_names
    # Every column entry has name and sql_type keys
    for col in columns:
        assert "name" in col
        assert "sql_type" in col


# ── test_show_unparseable_has_parse_error ─────────────────────────────────


def test_show_unparseable_has_parse_error() -> None:
    """show on a proc with unparseable DDL returns non-null parse_error."""
    from shared.loader import load_directory

    catalog = load_directory(_UNPARSEABLE_FIXTURES)
    errored = [name for name, e in catalog.procedures.items() if e.parse_error]
    assert len(errored) > 0


# ── test_refs_ast_bracket_notation ────────────────────────────────────────


def test_refs_ast_bracket_notation() -> None:
    """refs for silver.DimProduct finds proc that uses [silver].[DimProduct] bracket notation."""
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    assert result["name"] == "silver.dimproduct"
    referenced_by = result["referenced_by"]
    # usp_loaddimproduct uses [silver].[DimProduct] — must be found via AST
    assert "dbo.usp_loaddimproduct" in referenced_by


# ── test_refs_no_false_positive ───────────────────────────────────────────


def test_refs_no_false_positive_string_literal() -> None:
    """Proc that mentions 'silver.DimProduct' only in a comment is NOT returned by refs."""
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    referenced_by = result["referenced_by"]
    # usp_logmessage only mentions silver.DimProduct in a comment, not in DML
    assert "dbo.usp_logmessage" not in referenced_by


# ── test_discover_cli_list_succeeds_with_unparseable ─────────────────────


def test_discover_cli_list_succeeds_with_unparseable() -> None:
    """discover CLI list succeeds even with unparseable blocks (stored with error)."""
    from typer.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(
        discover.app,
        ["list", "--ddl-path", str(_UNPARSEABLE_FIXTURES), "--type", "procedures"],
    )
    assert result.exit_code == 0


# ── New pattern tests ────────────────────────────────────────────────────


def test_show_merge_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_MergeDimProduct", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]
    assert "MERGE" in result["refs"]["write_operations"]["silver.dimproduct"]
    assert result["classification"] == "deterministic"


def test_show_cte_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadWithCTE", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_multi_cte_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadWithMultiCTE", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_case_when_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadWithCase", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_left_join_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadWithLeftJoin", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_if_else_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ConditionalMerge", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_try_catch_proc_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_TryCatchLoad", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_correlated_subquery_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_CorrelatedSubquery", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_sequential_with_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_SequentialWith", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "dbo.config" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_update_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_SimpleUpdate", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_delete_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_SimpleDelete", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]


def test_show_delete_top_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_DeleteTop", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]


def test_show_truncate_only_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_TruncateOnly", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "TRUNCATE" in result["refs"]["write_operations"]["silver.dimproduct"]


def test_show_select_into_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_SelectInto", "tsql")
    assert "silver.dimproduct_staging" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]
    assert "SELECT_INTO" in result["refs"]["write_operations"]["silver.dimproduct_staging"]


def test_show_right_join_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_RightOuterJoin", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_subquery_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_SubqueryInWhere", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_window_function_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_WindowFunction", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_while_loop_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_WhileLoop", "tsql")
    assert "bronze.product" in result["refs"]["writes_to"]
    assert "dbo.config" in result["refs"]["writes_to"]


def test_show_nested_control_flow_refs() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_NestedControlFlow", "tsql")
    assert "silver.dimproduct" in result["refs"]["writes_to"]
    assert "bronze.product" in result["refs"]["reads_from"]


def test_show_exec_simple_has_exec() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ExecSimple", "tsql")
    assert result["has_exec"] is True
    assert result["classification"] == "claude_assisted"
    assert result["refs"]["writes_to"] == []
    assert result["refs"]["reads_from"] == []


def test_show_exec_dynamic_has_exec() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ExecDynamic", "tsql")
    assert result["has_exec"] is True
    assert result["classification"] == "claude_assisted"


def test_show_deterministic_no_exec() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadDimProduct", "tsql")
    assert result["has_exec"] is False
    assert result["classification"] == "deterministic"
    assert "INSERT" in result["refs"]["write_operations"]["silver.dimproduct"]


def test_refs_finds_all_writer_procs() -> None:
    result = discover.run_refs(_FLAT_FIXTURES, "silver.DimProduct", "tsql")
    rb = result["referenced_by"]
    # Deterministic writers
    assert "dbo.usp_loaddimproduct" in rb
    assert "dbo.usp_mergedimproduct" in rb
    assert "dbo.usp_loadwithcte" in rb
    assert "dbo.usp_loadwithmulticte" in rb
    assert "dbo.usp_loadwithcase" in rb
    assert "dbo.usp_loadwithleftjoin" in rb
    assert "dbo.usp_conditionalmerge" in rb
    assert "dbo.usp_trycatchload" in rb
    assert "dbo.usp_correlatedsubquery" in rb
    assert "dbo.usp_sequentialwith" in rb
    assert "dbo.usp_simpleupdate" in rb
    assert "dbo.usp_simpledelete" in rb
    assert "dbo.usp_deletetop" in rb
    assert "dbo.usp_truncateonly" in rb
    assert "dbo.usp_rightouterjoin" in rb
    assert "dbo.usp_subqueryinwhere" in rb
    assert "dbo.usp_windowfunction" in rb
    assert "dbo.usp_nestedcontrolflow" in rb
    # EXEC procs should NOT appear (no deterministic refs to DimProduct)
    assert "dbo.usp_execsimple" not in rb
    assert "dbo.usp_execdynamic" not in rb
    assert "dbo.usp_execspexecutesql" not in rb
    # Comment-only mention should NOT appear
    assert "dbo.usp_logmessage" not in rb
