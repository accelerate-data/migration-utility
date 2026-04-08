"""Tests for discover.py — DDL object catalog CLI.

Tests import shared.discover core functions directly (not via subprocess) to keep
execution fast and test coverage clear.  Run via uv to ensure shared is
importable: uv run --project <shared> pytest tests/ad-migration/migration/
"""

from __future__ import annotations

import tempfile
import json
from pathlib import Path

import pytest

from shared import discover
from shared.loader import CatalogFileMissingError, CatalogLoadError, CatalogNotFoundError, DdlParseError, ObjectNotFoundError

_TESTS_DIR = Path(__file__).parent
_FLAT_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "flat"
_UNPARSEABLE_FIXTURES = _TESTS_DIR / "fixtures" / "discover" / "unparseable"


# ── test_list_flat_tables ──────────────────────────────────────────────────


def test_list_flat_tables(assert_valid_schema) -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables)
    assert_valid_schema(result, "discover_list_output.json")
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


def test_list_flat_procedures(assert_valid_schema) -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.procedures)
    assert_valid_schema(result, "discover_list_output.json")
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
        ddl_dir = p / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE dbo.SomeTable (Id INT)\nGO\n", encoding="utf-8"
        )
        # Minimal catalog dir to satisfy mandatory check
        (p / "catalog" / "tables").mkdir(parents=True)
        (p / "catalog" / "tables" / "dbo.sometable.json").write_text(
            '{"columns":[],"primary_keys":[],"unique_indexes":[],"foreign_keys":[],'
            '"auto_increment_columns":[],"change_capture":null,"sensitivity_classifications":[],'
            '"referenced_by":{"procedures":{"in_scope":[],"out_of_scope":[]},'
            '"views":{"in_scope":[],"out_of_scope":[]},"functions":{"in_scope":[],"out_of_scope":[]}}}',
            encoding="utf-8",
        )
        result = discover.run_list(p, discover.ObjectType.views)
    assert result["objects"] == []


# ── test_list_indexed_same_as_flat ────────────────────────────────────────


def test_list_indexed_same_as_flat() -> None:
    """Indexed dir returns same object names as flat dir."""
    import shutil

    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "indexed"
        index_directory(_FLAT_FIXTURES, out)
        # Copy catalog/ from flat fixtures so indexed dir also has catalog
        shutil.copytree(_FLAT_FIXTURES / "catalog", out / "catalog")

        flat_result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables)
        indexed_result = discover.run_list(out, discover.ObjectType.tables)

    assert flat_result["objects"] == indexed_result["objects"]


# ── test_list_unparseable_stored_with_error ──────────────────────────────


def test_list_unparseable_stored_with_error() -> None:
    """Unparseable DDL blocks are stored with parse_error, not skipped."""
    from shared.loader import load_directory

    result = load_directory(_UNPARSEABLE_FIXTURES)
    has_error = any(e.parse_error is not None for e in result.procedures.values())
    assert has_error


# ── test_show_table_columns ───────────────────────────────────────────────


def test_show_table_columns(assert_valid_schema) -> None:
    """show on a table returns columns list populated from AST."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.DimProduct")
    assert_valid_schema(result, "discover_show_output.json")
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


# ── test_discover_cli_list_succeeds_with_unparseable ─────────────────────


def test_discover_cli_list_succeeds_with_unparseable() -> None:
    """discover CLI list succeeds even with unparseable blocks (stored with error)."""
    from typer.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(
        discover.app,
        ["list", "--project-root", str(_UNPARSEABLE_FIXTURES), "--type", "procedures"],
    )
    assert result.exit_code == 0


# ── show: statement analysis (no catalog needed) ─────────────────────────


def test_show_deterministic_has_statements(assert_valid_schema) -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadDimProduct")
    assert_valid_schema(result, "discover_show_output.json")
    assert result["needs_llm"] is False
    assert result["routing_reasons"] == []
    assert result["statements"] is not None
    actions = {s["action"] for s in result["statements"]}
    assert "migrate" in actions


def test_show_static_exec_is_deterministic() -> None:
    """Static EXEC procs are deterministic — catalog-enrich resolves them."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ExecSimple")
    assert result["needs_llm"] is False
    assert result["routing_reasons"] == []
    assert result["statements"] is not None


def test_show_dynamic_exec_needs_llm() -> None:
    """Dynamic EXEC(@var) procs need LLM — reads raw_ddl."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_ExecDynamic")
    assert result["needs_llm"] is True
    assert result["routing_reasons"] == ["dynamic_sql_variable"]
    assert result["statements"] is None


def test_show_uses_routing_mode_and_reasons(tmp_path, assert_valid_schema) -> None:
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
    assert_valid_schema(result, "discover_show_output.json")
    assert result["needs_llm"] is False
    assert result["routing_reasons"] == ["if_else"]
    assert result["statements"] is not None


def test_show_statements_truncate_is_skip() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_TruncateOnly")
    actions = [s["action"] for s in result["statements"]]
    assert "skip" in actions
    assert "migrate" not in actions


def test_show_statements_table_has_none() -> None:
    result = discover.run_show(_FLAT_FIXTURES, "silver.DimProduct")
    assert result["statements"] is None


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


# ── Catalog-first refs tests ────────────────────────────────────────────


_CATALOG_FIXTURES = _TESTS_DIR / "fixtures" / "catalog"


def test_refs_catalog_finds_writers(assert_valid_schema) -> None:
    """refs uses catalog data when catalog/tables/*.json exists."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.FactSales")
    assert_valid_schema(result, "discover_refs_output.json")
    assert result["source"] == "catalog"
    writer_names = [w["procedure"] for w in result["writers"]]
    assert "dbo.usp_load_fact_sales" in writer_names
    # Writer has is_updated flag
    writer = next(w for w in result["writers"] if w["procedure"] == "dbo.usp_load_fact_sales")
    assert writer["is_updated"] is True


def test_refs_catalog_finds_readers() -> None:
    """refs catalog path correctly identifies readers (is_selected only)."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.FactSales")
    assert result["source"] == "catalog"
    assert "dbo.usp_read_fact_sales" in result["readers"]
    assert "dbo.vw_sales_summary" in result["readers"]


def test_refs_catalog_no_confidence() -> None:
    """Catalog-path refs output has no confidence or status fields."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.FactSales")
    assert result["source"] == "catalog"
    for w in result["writers"]:
        assert "confidence" not in w
        assert "status" not in w


def test_refs_errors_without_catalog() -> None:
    """refs raises CatalogNotFoundError when no catalog/ directory exists."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        ddl_dir = p / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE dbo.T (Id INT)\nGO\n", encoding="utf-8",
        )
        with pytest.raises(CatalogNotFoundError):
            discover.run_refs(p, "dbo.T")


# ── Corrupt catalog JSON tests ──────────────────────────────────────────


def _make_project_with_corrupt_catalog(tmp: Path, object_type: str, fqn: str) -> Path:
    """Set up a minimal project with one corrupt catalog file."""
    ddl_dir = tmp / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "tables.sql").write_text(
        "CREATE TABLE dbo.T (Id INT)\nGO\n", encoding="utf-8",
    )
    (ddl_dir / "procedures.sql").write_text(
        "CREATE PROCEDURE dbo.usp_test AS SELECT 1\nGO\n", encoding="utf-8",
    )
    cat_dir = tmp / "catalog" / object_type
    cat_dir.mkdir(parents=True)
    (cat_dir / f"{fqn}.json").write_text("{truncated", encoding="utf-8")
    return tmp


def test_show_corrupt_catalog_raises_catalog_load_error() -> None:
    """show with corrupt catalog JSON raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_show(root, "dbo.T")


def test_refs_corrupt_table_catalog_raises() -> None:
    """refs with corrupt table catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_refs(root, "dbo.T")


def test_write_statements_corrupt_proc_catalog_raises() -> None:
    """write-statements with corrupt existing proc catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "procedures", "dbo.usp_test")
        with pytest.raises(CatalogLoadError):
            discover.run_write_statements(root, "dbo.usp_test", [{"action": "migrate", "id": "1"}])


def test_list_succeeds_despite_corrupt_catalog() -> None:
    """list does not read catalog JSON, so corrupt catalogs don't affect it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        result = discover.run_list(root, discover.ObjectType.tables)
        assert "dbo.t" in result["objects"]


def test_write_scoping_corrupt_table_catalog_raises() -> None:
    """write-scoping with corrupt existing table catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_write_scoping(root, "dbo.T", {"status": "resolved", "selected_writer": "dbo.usp_load"})


# ── View reference classification tests ─────────────────────────────────────


def _make_project_with_proc_view_refs(tmp: Path) -> Path:
    """Project where a proc references both a table and a view in its catalog."""
    ddl_dir = tmp / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "procedures.sql").write_text(
        "CREATE PROCEDURE dbo.usp_LoadData AS BEGIN SELECT 1 END\nGO\n",
        encoding="utf-8",
    )
    (ddl_dir / "tables.sql").write_text(
        "CREATE TABLE silver.FactSales (Id INT)\nGO\n", encoding="utf-8",
    )
    cat_dir = tmp / "catalog"
    (cat_dir / "procedures").mkdir(parents=True)
    (cat_dir / "tables").mkdir(parents=True)
    (cat_dir / "procedures" / "dbo.usp_loaddata.json").write_text(
        json.dumps({
            "references": {
                "tables": {
                    "in_scope": [
                        {
                            "schema": "silver", "name": "FactSales",
                            "is_selected": True, "is_updated": True,
                            "is_insert_all": False, "columns": [],
                        },
                    ],
                    "out_of_scope": [],
                },
                "views": {
                    "in_scope": [
                        {"schema": "dbo", "name": "vw_customer_dim", "is_selected": True, "is_updated": False},
                    ],
                    "out_of_scope": [],
                },
                "functions": {"in_scope": [], "out_of_scope": []},
                "procedures": {"in_scope": [], "out_of_scope": []},
            },
        }),
        encoding="utf-8",
    )
    (cat_dir / "tables" / "silver.factsales.json").write_text(
        json.dumps({
            "columns": [], "primary_keys": [], "unique_indexes": [], "foreign_keys": [],
            "auto_increment_columns": [], "change_capture": None, "sensitivity_classifications": [],
            "referenced_by": {
                "procedures": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
        }),
        encoding="utf-8",
    )
    return tmp


def test_show_proc_view_refs_not_in_reads_from() -> None:
    """run_show for a proc with references.views entries does not put views in reads_from.

    Views are classified separately from tables in the proc catalog — this test
    confirms that run_show does not conflate the two buckets when building the
    refs.reads_from list.
    """
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_proc_view_refs(Path(tmp))
        result = discover.run_show(root, "dbo.usp_LoadData")

    refs = result["refs"]
    assert refs is not None
    # Table that is read should be present
    assert "silver.factsales" in refs["reads_from"]
    # The view dependency must NOT appear in the tables reads_from list
    assert "dbo.vw_customer_dim" not in refs["reads_from"]
    # The table that is written should be present
    assert "silver.factsales" in refs["writes_to"]


def _make_project_with_view_catalog(tmp: Path) -> Path:
    """Project with a view catalog entry that has referenced_by.procedures."""
    ddl_dir = tmp / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "views.sql").write_text(
        "CREATE VIEW dbo.vw_customer_dim AS SELECT Id FROM dbo.Customer\nGO\n",
        encoding="utf-8",
    )
    cat_dir = tmp / "catalog"
    (cat_dir / "views").mkdir(parents=True)
    (cat_dir / "views" / "dbo.vw_customer_dim.json").write_text(
        json.dumps({
            "schema": "dbo",
            "name": "vw_customer_dim",
            "references": {
                "tables": {"in_scope": [], "out_of_scope": []},
                "views": {"in_scope": [], "out_of_scope": []},
            },
            "referenced_by": {
                "procedures": {
                    "in_scope": [
                        {
                            "schema": "dbo", "name": "usp_load_fact_sales",
                            "is_selected": True, "is_updated": False,
                        },
                    ],
                    "out_of_scope": [],
                },
                "views": {"in_scope": [], "out_of_scope": []},
                "functions": {"in_scope": [], "out_of_scope": []},
            },
        }),
        encoding="utf-8",
    )
    return tmp


def test_refs_view_catalog_returns_view_type() -> None:
    """run_refs on a view FQN returns type='view' and the referencing proc as reader."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_view_catalog(Path(tmp))
        result = discover.run_refs(root, "dbo.vw_customer_dim")

    assert result["source"] == "catalog"
    assert result["type"] == "view"
    assert "dbo.usp_load_fact_sales" in result["readers"]


def test_show_delete_top_is_migrate() -> None:
    """Pattern #4: DELETE TOP procedure is deterministic with migrate action."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_DeleteTop")
    assert result["needs_llm"] is False
    assert result["statements"] is not None
    actions = {s["action"] for s in result["statements"]}
    assert "migrate" in actions


def test_show_try_catch_is_deterministic() -> None:
    """Pattern #46: TRY/CATCH procedure is deterministic — branches are flattened."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_TryCatchLoad")
    assert result["needs_llm"] is False
    assert result["statements"] is not None
    actions = {s["action"] for s in result["statements"]}
    assert "migrate" in actions


def test_show_nested_control_flow_is_deterministic() -> None:
    """Pattern #48: Nested IF inside TRY/CATCH is deterministic — all branches flattened."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_NestedControlFlow")
    assert result["needs_llm"] is False
    assert result["statements"] is not None
    actions = {s["action"] for s in result["statements"]}
    assert "migrate" in actions


def test_show_recursive_cte_is_migrate() -> None:
    """Pattern #36: Recursive CTE procedure is deterministic with migrate action."""
    result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_RecursiveCTE")
    assert result["needs_llm"] is False
    assert result["statements"] is not None
    actions = {s["action"] for s in result["statements"]}
    assert "migrate" in actions


# --- View scoping tests ---

def test_run_write_view_scoping_happy_path() -> None:
    """write-view-scoping with analyzed status writes scoping to catalog/views/."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        # Create minimal view catalog
        views_dir = root / "catalog" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "silver.vw_test.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_test",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        scoping = {
            "status": "analyzed",
            "sql_elements": [{"type": "join", "detail": "INNER JOIN bronze.customer"}],
            "call_tree": {"reads_from": ["bronze.customer"], "views_referenced": []},
            "logic_summary": "Joins customer data.",
            "rationale": "Simple join view.",
            "warnings": [],
            "errors": [],
        }
        result = discover.run_write_view_scoping(root, "silver.vw_test", scoping)
        assert result["status"] == "ok"
        written = json.loads(Path(result["written"]).read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "analyzed"
        assert written["scoping"]["sql_elements"][0]["type"] == "join"


def test_run_write_view_scoping_invalid_status() -> None:
    """write-view-scoping rejects statuses that are not analyzed|error."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        views_dir = root / "catalog" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "silver.vw_test.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_test",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="Invalid view scoping status"):
            discover.run_write_view_scoping(root, "silver.vw_test", {"status": "resolved"})


def test_run_write_view_scoping_missing_catalog() -> None:
    """write-view-scoping raises CatalogFileMissingError when catalog file is absent."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "views").mkdir(parents=True)
        with pytest.raises(CatalogFileMissingError):
            discover.run_write_view_scoping(root, "silver.vw_missing", {"status": "analyzed"})


# --- _analyze_view_select tests (via run_show on flat fixtures) ---


def test_show_view_join_returns_sql_elements() -> None:
    """run_show for a view with a JOIN returns sql_elements containing a join entry."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_ProductCatalog")
    assert result["type"] == "view"
    assert result["needs_llm"] is None  # not applicable for views
    assert result["sql_elements"] is not None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "join" in element_types


def test_show_view_aggregation_returns_sql_elements() -> None:
    """run_show for a view with GROUP BY + SUM/COUNT returns aggregation and group_by elements."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_SalesSummary")
    assert result["type"] == "view"
    assert result["needs_llm"] is None  # not applicable for views
    assert result["sql_elements"] is not None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "aggregation" in element_types
    assert "group_by" in element_types


def test_show_view_window_function_returns_sql_elements() -> None:
    """run_show for a view with ROW_NUMBER OVER returns window_function element."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_RankedProducts")
    assert result["type"] == "view"
    assert result["needs_llm"] is None  # not applicable for views
    assert result["sql_elements"] is not None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "window_function" in element_types


def test_show_view_errors_key_present_for_all_types() -> None:
    """run_show always returns an errors key regardless of object type."""
    view_result = discover.run_show(_FLAT_FIXTURES, "silver.vw_ProductCatalog")
    assert "errors" in view_result
    proc_result = discover.run_show(_FLAT_FIXTURES, "dbo.usp_LoadDimProduct")
    assert "errors" in proc_result


def test_show_view_case_expression_returns_sql_elements() -> None:
    """View with CASE expression returns case element."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_CustomerTier")
    assert result["needs_llm"] is None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "case" in element_types


def test_show_view_subquery_returns_sql_elements() -> None:
    """View with scalar subquery and EXISTS returns subquery element."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_ActiveCustomers")
    assert result["needs_llm"] is None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "subquery" in element_types


def test_show_view_single_cte_returns_sql_elements() -> None:
    """View with a single CTE returns cte element with count 1."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_TopProducts")
    assert result["needs_llm"] is None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "cte" in element_types
    cte_el = next(e for e in result["sql_elements"] if e["type"] == "cte")
    assert "1" in cte_el["detail"]


def test_show_view_multi_cte_returns_correct_count() -> None:
    """View with two CTEs returns cte element with count 2."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_SalesWithRegion")
    assert result["needs_llm"] is None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "cte" in element_types
    cte_el = next(e for e in result["sql_elements"] if e["type"] == "cte")
    assert "2" in cte_el["detail"]


def test_show_view_simple_select_returns_empty_elements() -> None:
    """Simple SELECT view with no joins/aggregations returns empty sql_elements."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_SimpleCustomer")
    assert result["needs_llm"] is None
    assert result["sql_elements"] == []


def test_show_view_duplicate_join_deduplicated() -> None:
    """View joining the same table twice produces deduplicated join elements."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_DuplicateJoin")
    assert result["needs_llm"] is None
    join_details = [e["detail"] for e in result["sql_elements"] if e["type"] == "join"]
    # Two joins to bronze.Orders — detail strings differ by alias target but same table;
    # at minimum, no exact-duplicate detail strings should appear
    assert len(join_details) == len(set(join_details))


def test_show_view_combined_elements() -> None:
    """View with JOIN + GROUP BY + WINDOW returns all three element types."""
    result = discover.run_show(_FLAT_FIXTURES, "silver.vw_Combined")
    assert result["needs_llm"] is None
    element_types = {e["type"] for e in result["sql_elements"]}
    assert "join" in element_types
    assert "group_by" in element_types
    assert "window_function" in element_types


def test_write_scoping_cli_auto_detects_view_catalog() -> None:
    """write-scoping CLI routes to view path when catalog/views/<fqn>.json exists.

    Uses the repo root (a valid git repo) as project-root. Creates and cleans up
    a temporary view catalog at catalog/views/silver.vw_cli_test.json.
    """
    import json as _json
    from typer.testing import CliRunner

    # Repo root is 4 levels up from this test file: tests/unit/test_discover.py
    repo_root = Path(__file__).parent.parent.parent
    views_dir = repo_root / "catalog" / "views"
    cat_file = views_dir / "silver.vw_cli_test.json"
    scoping_file = repo_root / ".staging-test-scoping.json"
    try:
        views_dir.mkdir(parents=True, exist_ok=True)
        cat_file.write_text(
            _json.dumps({
                "schema": "silver", "name": "vw_cli_test",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        scoping_file.write_text(
            _json.dumps({"status": "analyzed", "sql_elements": [], "warnings": [], "errors": []}),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(
            discover.app,
            ["write-scoping", "--project-root", str(repo_root), "--name", "silver.vw_cli_test",
             "--scoping-file", str(scoping_file)],
        )
        assert result.exit_code == 0, result.output
        written = _json.loads(cat_file.read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "analyzed"
    finally:
        cat_file.unlink(missing_ok=True)
        scoping_file.unlink(missing_ok=True)


def test_run_write_view_scoping_error_status() -> None:
    """write-view-scoping accepts error status (parse failure case)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        views_dir = root / "catalog" / "views"
        views_dir.mkdir(parents=True)
        (views_dir / "silver.vw_broken.json").write_text(
            json.dumps({
                "schema": "silver", "name": "vw_broken",
                "references": {"tables": {"in_scope": [], "out_of_scope": []},
                               "views": {"in_scope": [], "out_of_scope": []},
                               "functions": {"in_scope": [], "out_of_scope": []}},
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []},
                                  "views": {"in_scope": [], "out_of_scope": []},
                                  "functions": {"in_scope": [], "out_of_scope": []}},
            }),
            encoding="utf-8",
        )
        scoping = {
            "status": "error",
            "sql_elements": None,
            "errors": [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": "unexpected token"}],
        }
        result = discover.run_write_view_scoping(root, "silver.vw_broken", scoping)
        assert result["status"] == "ok"
        written = json.loads(Path(result["written"]).read_text(encoding="utf-8"))
        assert written["scoping"]["status"] == "error"
        assert written["scoping"]["sql_elements"] is None


# ── write-source tests ────────────────────────────────────────────────────────


def _make_table_cat(root: Path, fqn: str, scoping: dict, extra: dict | None = None) -> Path:
    tables_dir = root / "catalog" / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)
    schema, name = fqn.split(".", 1)
    data: dict = {"schema": schema, "name": name, "scoping": scoping}
    if extra:
        data.update(extra)
    path = tables_dir / f"{fqn}.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_write_source_sets_flag(assert_valid_schema) -> None:
    """run_write_source sets is_source: true on a no_writer_found table."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(root, "silver.lookup", {"status": "no_writer_found"})
        result = discover.run_write_source(root, "silver.lookup", True)
        assert_valid_schema(result, "write_source_output.json")
        assert result["is_source"] is True
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert written["is_source"] is True


def test_write_source_resolved_table(assert_valid_schema) -> None:
    """run_write_source accepts resolved tables (cross-domain source scenario)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root, "silver.crossdomain",
            {"status": "resolved", "selected_writer": "dbo.usp_other"},
        )
        result = discover.run_write_source(root, "silver.crossdomain", True)
        assert_valid_schema(result, "write_source_output.json")
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert written["is_source"] is True


def test_write_source_false_resets_flag(assert_valid_schema) -> None:
    """run_write_source with value=False writes is_source: false (always present)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root, "silver.audit", {"status": "no_writer_found"}, {"is_source": True}
        )
        result = discover.run_write_source(root, "silver.audit", False)
        assert_valid_schema(result, "write_source_output.json")
        assert result["is_source"] is False
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert written["is_source"] is False


def test_write_source_missing_catalog_raises() -> None:
    """run_write_source raises CatalogFileMissingError when catalog file absent."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "tables").mkdir(parents=True)
        with pytest.raises(CatalogFileMissingError):
            discover.run_write_source(root, "silver.nonexistent", True)


def test_write_source_unanalyzed_guard_raises() -> None:
    """run_write_source raises ValueError when table has not been analyzed yet."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        tables_dir = root / "catalog" / "tables"
        tables_dir.mkdir(parents=True)
        (tables_dir / "silver.fresh.json").write_text(
            json.dumps({"schema": "silver", "name": "Fresh"}), encoding="utf-8"
        )
        with pytest.raises(ValueError, match="not been analyzed"):
            discover.run_write_source(root, "silver.fresh", True)


# ── write-slice tests ─────────────────────────────────────────────────────────


def _make_proc_cat(root: Path, fqn: str) -> Path:
    """Create a minimal proc catalog file at catalog/procedures/<fqn>.json."""
    proc_dir = root / "catalog" / "procedures"
    proc_dir.mkdir(parents=True, exist_ok=True)
    schema, name = fqn.split(".", 1)
    data = {
        "schema": schema,
        "name": name,
        "references": {
            "tables": {"in_scope": [], "out_of_scope": []},
        },
    }
    path = proc_dir / f"{fqn}.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


class TestWriteTableSlice:

    def test_write_table_slice_happy_path(self, assert_valid_schema) -> None:
        """run_write_table_slice writes slice text into proc catalog table_slices."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_proc_cat(root, "dbo.usp_multi")
            result = discover.run_write_table_slice(root, "dbo.usp_multi", "dim.target", "MERGE INTO dim.target ...")
            assert_valid_schema(result, "write_slice_output.json")
            assert result["status"] == "ok"
            proc_path = root / "catalog" / "procedures" / "dbo.usp_multi.json"
            written = json.loads(proc_path.read_text(encoding="utf-8"))
            assert written["table_slices"]["dim.target"] == "MERGE INTO dim.target ..."

    def test_write_table_slice_accumulates(self, assert_valid_schema) -> None:
        """run_write_table_slice accumulates slices for distinct tables under the same proc."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_proc_cat(root, "dbo.usp_multi")
            discover.run_write_table_slice(root, "dbo.usp_multi", "dim.table_a", "INSERT INTO dim.table_a ...")
            discover.run_write_table_slice(root, "dbo.usp_multi", "dim.table_b", "INSERT INTO dim.table_b ...")
            proc_path = root / "catalog" / "procedures" / "dbo.usp_multi.json"
            written = json.loads(proc_path.read_text(encoding="utf-8"))
            assert "dim.table_a" in written["table_slices"]
            assert "dim.table_b" in written["table_slices"]

    def test_write_table_slice_missing_catalog(self) -> None:
        """run_write_table_slice raises CatalogFileMissingError when proc catalog is absent."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "catalog" / "procedures").mkdir(parents=True)
            with pytest.raises(CatalogFileMissingError):
                discover.run_write_table_slice(root, "dbo.usp_nonexistent", "dim.target", "SELECT 1")
