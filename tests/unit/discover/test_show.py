from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import (
    CatalogNotFoundError,
)
from tests.unit.discover.discover_test_helpers import (
    _FLAT_FIXTURES,
    _SOURCE_TABLE_GUARD_FIXTURES,
    _UNPARSEABLE_FIXTURES,
    _make_project_with_proc_view_refs,
    _make_table_cat,
)

def test_show_seed_table_includes_seed_marker() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ddl_dir = root / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE silver.SeedLookup (SeedKey INT NOT NULL)\nGO\n",
            encoding="utf-8",
        )
        _make_table_cat(
            root,
            "silver.seedlookup",
            {"status": "no_writer_found"},
            {"is_seed": True, "is_source": False, "columns": [{"name": "SeedKey", "sql_type": "INT"}]},
        )
        result = discover.run_show(root, "silver.seedlookup")
        assert result.is_seed is True
        assert result.is_source is None

def test_show_table_columns() -> None:
    """show on a table returns columns list populated from AST."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.DimProduct")
    assert result.type == "table"
    assert result.is_source is None
    assert result.parse_error is None
    columns = result.columns
    assert isinstance(columns, list)
    col_names = [c.name for c in columns]
    assert "ProductKey" in col_names
    assert "ProductAlternateKey" in col_names
    assert "EnglishProductName" in col_names
    # Every column entry has name and sql_type
    for col in columns:
        assert col.name
        assert col.sql_type

def test_show_source_table_includes_source_marker() -> None:
    result = discover.run_show(_SOURCE_TABLE_GUARD_FIXTURES, "silver.DimSource")
    assert result.type == "table"
    assert result.is_source is True

def test_show_unparseable_has_parse_error() -> None:
    """show on a proc with unparseable DDL returns non-null parse_error."""
    from shared.loader import load_directory

    catalog = load_directory(_UNPARSEABLE_FIXTURES)
    errored = [name for name, e in catalog.procedures.items() if e.parse_error]
    assert len(errored) > 0

def test_show_deterministic_has_statements() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadDimProduct")
    assert result.needs_llm is False
    assert result.routing_reasons == []
    assert result.statements is not None
    actions = {s.action for s in result.statements}
    assert "migrate" in actions

def test_show_static_exec_is_deterministic() -> None:
    """Static EXEC procs are deterministic — catalog-enrich resolves them."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ExecSimple")
    assert result.needs_llm is False
    assert result.routing_reasons == []
    assert result.statements is not None

def test_show_dynamic_exec_needs_llm() -> None:
    """Dynamic EXEC(@var) procs need LLM — reads raw_ddl."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ExecDynamic")
    assert result.needs_llm is True
    assert result.routing_reasons == ["dynamic_sql_variable"]
    assert result.statements is None

def test_show_uses_routing_mode_and_reasons(tmp_path) -> None:
    import shutil

    shutil.copytree(_FLAT_FIXTURES / "ddl", tmp_path / "ddl")
    shutil.copytree(_FLAT_FIXTURES / "catalog", tmp_path / "catalog")

    proc_path = tmp_path / "catalog" / "procedures" / "dbo.usp_conditionalmerge.json"
    proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
    proc_cat["needs_llm"] = False
    proc_cat["mode"] = "control_flow_fallback"
    proc_cat["routing_reasons"] = ["if_else"]
    proc_path.write_text(json.dumps(proc_cat, indent=2) + "\n", encoding="utf-8")

    result = discover.run_show(tmp_path, "dbo.usp_ConditionalMerge")
    assert result.needs_llm is False
    assert result.routing_reasons == ["if_else"]
    assert result.statements is not None

def test_show_statements_truncate_is_skip() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_TruncateOnly")
    actions = [s.action for s in result.statements]
    assert "skip" in actions
    assert "migrate" not in actions

def test_show_statements_table_has_none() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "silver.DimProduct")
    assert result.statements is None

def test_show_errors_without_catalog() -> None:
    """show errors when no catalog/ directory exists."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        ddl_dir = p / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "procedures.sql").write_text(
            "CREATE PROCEDURE dbo.usp_Test AS BEGIN SELECT 1 END\nGO\n",
            encoding="utf-8",
        )
        with pytest.raises(CatalogNotFoundError):
            discover.run_show(p, "dbo.usp_Test")

def test_show_proc_view_refs_not_in_reads_from() -> None:
    """run_show for a proc with references.views entries does not put views in reads_from.

    Views are classified separately from tables in the proc catalog — this test
    confirms that run_show does not conflate the two buckets when building the
    refs.reads_from list.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_proc_view_refs(Path(tmp))
        result = discover.run_show(root, "dbo.usp_LoadData")

    refs = result.refs
    assert refs is not None
    # Table that is read should be present
    assert "silver.factsales" in refs.reads_from
    # The view dependency must NOT appear in the tables reads_from list
    assert "dbo.vw_customer_dim" not in refs.reads_from
    # The table that is written should be present
    assert "silver.factsales" in refs.writes_to

def test_show_delete_top_is_migrate() -> None:
    """Pattern #4: DELETE TOP procedure is deterministic with migrate action."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_DeleteTop")
    assert result.needs_llm is False
    assert result.statements is not None
    actions = {s.action for s in result.statements}
    assert "migrate" in actions

def test_show_try_catch_is_deterministic() -> None:
    """Pattern #46: TRY/CATCH procedure is deterministic — branches are flattened."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_TryCatchLoad")
    assert result.needs_llm is False
    assert result.statements is not None
    actions = {s.action for s in result.statements}
    assert "migrate" in actions

def test_show_nested_control_flow_is_deterministic() -> None:
    """Pattern #48: Nested IF inside TRY/CATCH is deterministic — all branches flattened."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_NestedControlFlow")
    assert result.needs_llm is False
    assert result.statements is not None
    actions = {s.action for s in result.statements}
    assert "migrate" in actions

def test_show_recursive_cte_is_migrate() -> None:
    """Pattern #36: Recursive CTE procedure is deterministic with migrate action."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_RecursiveCTE")
    assert result.needs_llm is False
    assert result.statements is not None
    actions = {s.action for s in result.statements}
    assert "migrate" in actions

def test_show_view_join_returns_sql_elements() -> None:
    """run_show for a view with a JOIN returns sql_elements containing a join entry."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_ProductCatalog")
    assert result.type == "view"
    assert result.needs_llm is None  # not applicable for views
    assert result.sql_elements is not None
    element_types = {e.type for e in result.sql_elements}
    assert "join" in element_types

def test_show_view_aggregation_returns_sql_elements() -> None:
    """run_show for a view with GROUP BY + SUM/COUNT returns aggregation and group_by elements."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_SalesSummary")
    assert result.type == "view"
    assert result.needs_llm is None  # not applicable for views
    assert result.sql_elements is not None
    element_types = {e.type for e in result.sql_elements}
    assert "aggregation" in element_types
    assert "group_by" in element_types

def test_show_view_window_function_returns_sql_elements() -> None:
    """run_show for a view with ROW_NUMBER OVER returns window_function element."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_RankedProducts")
    assert result.type == "view"
    assert result.needs_llm is None  # not applicable for views
    assert result.sql_elements is not None
    element_types = {e.type for e in result.sql_elements}
    assert "window_function" in element_types

def test_show_view_errors_key_present_for_all_types() -> None:
    """run_show always returns an errors key regardless of object type."""
    view_result = discover.run_show(_FLAT_FIXTURES, "silver.vw_ProductCatalog")
    assert hasattr(view_result, "errors")
    proc_result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadDimProduct")
    assert hasattr(proc_result, "errors")

def test_show_view_case_expression_returns_sql_elements() -> None:
    """View with CASE expression returns case element."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_CustomerTier")
    assert result.needs_llm is None
    element_types = {e.type for e in result.sql_elements}
    assert "case" in element_types

def test_show_view_subquery_returns_sql_elements() -> None:
    """View with scalar subquery and EXISTS returns subquery element."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_ActiveCustomers")
    assert result.needs_llm is None
    element_types = {e.type for e in result.sql_elements}
    assert "subquery" in element_types

def test_show_view_single_cte_returns_sql_elements() -> None:
    """View with a single CTE returns cte element with count 1."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_TopProducts")
    assert result.needs_llm is None
    element_types = {e.type for e in result.sql_elements}
    assert "cte" in element_types
    cte_el = next(e for e in result.sql_elements if e.type == "cte")
    assert "1" in cte_el.detail

def test_show_view_multi_cte_returns_correct_count() -> None:
    """View with two CTEs returns cte element with count 2."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_SalesWithRegion")
    assert result.needs_llm is None
    element_types = {e.type for e in result.sql_elements}
    assert "cte" in element_types
    cte_el = next(e for e in result.sql_elements if e.type == "cte")
    assert "2" in cte_el.detail

def test_show_view_simple_select_returns_empty_elements() -> None:
    """Simple SELECT view with no joins/aggregations returns empty sql_elements."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_SimpleCustomer")
    assert result.needs_llm is None
    assert result.sql_elements == []

def test_show_view_duplicate_join_deduplicated() -> None:
    """View joining the same table twice produces deduplicated join elements."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_DuplicateJoin")
    assert result.needs_llm is None
    join_details = [e.detail for e in result.sql_elements if e.type == "join"]
    # Two joins to bronze.Orders — detail strings differ by alias target but same table;
    # at minimum, no exact-duplicate detail strings should appear
    assert len(join_details) == len(set(join_details))

def test_show_view_combined_elements() -> None:
    """View with JOIN + GROUP BY + WINDOW returns all three element types."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_Combined")
    assert result.needs_llm is None
    element_types = {e.type for e in result.sql_elements}
    assert "join" in element_types
    assert "group_by" in element_types
    assert "window_function" in element_types
