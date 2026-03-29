"""Tests for the discover CLI (list, show, refs).

Uses the shared DDL fixtures in fixtures/discover/flat/.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import typer

from shared.discover import run_list, run_refs, run_show, ObjectType

FIXTURES = Path(__file__).parent / "fixtures" / "discover" / "flat"


# ── list ─────────────────────────────────────────────────────────────────────


def test_list_tables():
    result = run_list(FIXTURES, ObjectType.tables)
    assert "silver.dimcustomer" in result["objects"]
    assert "silver.factsales" in result["objects"]
    assert "bronze.customer" in result["objects"]
    assert "bronze.sales" in result["objects"]


def test_list_procedures():
    result = run_list(FIXTURES, ObjectType.procedures)
    assert "silver.usp_load_dimcustomer" in result["objects"]
    assert "silver.usp_load_factsales" in result["objects"]
    assert "silver.usp_load_with_cte" in result["objects"]
    assert "silver.usp_load_with_multi_cte" in result["objects"]
    assert "silver.usp_conditional_load" in result["objects"]
    assert "silver.usp_correlated_subquery" in result["objects"]


def test_list_views():
    result = run_list(FIXTURES, ObjectType.views)
    assert "silver.vw_customersales" in result["objects"]


def test_list_functions():
    result = run_list(FIXTURES, ObjectType.functions)
    assert "silver.fn_format_name" in result["objects"]


# ── show ─────────────────────────────────────────────────────────────────────


def test_show_table_columns():
    result = run_show(FIXTURES, "silver.DimCustomer")
    assert result["name"] == "silver.dimcustomer"
    assert result["type"] == "table"
    col_names = [c["name"] for c in result["columns"]]
    assert "CustomerKey" in col_names
    assert "FirstName" in col_names
    assert "Region" in col_names


def test_show_simple_proc_refs():
    result = run_show(FIXTURES, "silver.usp_load_dimcustomer")
    assert result["type"] == "procedure"
    assert result["needs_llm"] is False
    assert result["classification"] == "deterministic"
    assert "silver.dimcustomer" in result["refs"]["writes_to"]
    assert "bronze.customer" in result["refs"]["reads_from"]


def test_show_merge_proc_refs():
    result = run_show(FIXTURES, "silver.usp_load_factsales")
    assert "silver.factsales" in result["refs"]["writes_to"]
    assert "bronze.sales" in result["refs"]["reads_from"]


def test_show_cte_proc_refs():
    result = run_show(FIXTURES, "silver.usp_load_with_cte")
    assert "silver.dimcustomer" in result["refs"]["writes_to"]
    assert "bronze.customer" in result["refs"]["reads_from"]


def test_show_multi_cte_proc_refs():
    result = run_show(FIXTURES, "silver.usp_load_with_multi_cte")
    assert "silver.dimcustomer" in result["refs"]["writes_to"]
    assert "bronze.salesorder" in result["refs"]["reads_from"]
    assert "bronze.customer" in result["refs"]["reads_from"]


def test_show_if_else_proc_needs_llm():
    """IF/ELSE control flow → needs_llm=True, partial refs from single-pass."""
    result = run_show(FIXTURES, "silver.usp_conditional_load")
    assert result["needs_llm"] is True
    assert result["classification"] == "claude_assisted"
    # The MERGE and DELETE are inside IF/ELSE — single-pass won't capture them.
    # But the Command block text preserves the raw SQL for LLM consumption.
    stmts = result["statements"]
    command_stmts = [s for s in stmts if s["type"] in ("Command", "If")]
    assert len(command_stmts) > 0
    # The raw text should contain the write targets
    command_text = " ".join(s["sql"] for s in command_stmts)
    assert "DimCustomer" in command_text


def test_show_try_catch_proc_needs_llm():
    """TRY/CATCH control flow → needs_llm=True."""
    result = run_show(FIXTURES, "silver.usp_try_catch_load")
    assert result["needs_llm"] is True
    assert result["classification"] == "claude_assisted"
    stmts = result["statements"]
    command_stmts = [s for s in stmts if s["type"] in ("Command", "If")]
    assert len(command_stmts) > 0
    command_text = " ".join(s["sql"] for s in command_stmts)
    assert "FactSales" in command_text


def test_show_not_found():
    with pytest.raises((SystemExit, typer.Exit)) as exc_info:
        run_show(FIXTURES, "silver.does_not_exist")
    assert exc_info.value.exit_code == 1


# ── refs ─────────────────────────────────────────────────────────────────────


def _writer_names(result: dict) -> list[str]:
    """Extract procedure names from writer entries."""
    return [w["procedure"] for w in result["writers"]]


def test_refs_dimcustomer_writers():
    result = run_refs(FIXTURES, "silver.DimCustomer")
    assert result["name"] == "silver.dimcustomer"
    names = _writer_names(result)
    # Deterministic direct writers
    assert "silver.usp_load_dimcustomer" in names
    assert "silver.usp_load_with_cte" in names
    assert "silver.usp_load_with_case" in names
    assert "silver.usp_load_with_multi_cte" in names
    # Check rich writer entry structure
    entry = next(w for w in result["writers"] if w["procedure"] == "silver.usp_load_dimcustomer")
    assert entry["write_type"] == "direct"
    assert "TRUNCATE" in entry["write_operations"] or "INSERT" in entry["write_operations"]
    assert entry["confidence"] >= 0.70
    assert entry["status"] == "confirmed"
    # usp_conditional_load and usp_try_catch_load have needs_llm — flagged
    assert "silver.usp_conditional_load" in result["llm_required"]
    # usp_full_reload: block-level parse_error — also in llm_required
    assert "silver.usp_full_reload" in result["llm_required"]


def test_refs_dimcustomer_readers():
    result = run_refs(FIXTURES, "silver.DimCustomer")
    # The view reads from DimCustomer
    assert "silver.vw_customersales" in result["readers"]


def test_refs_factsales_writers_and_view():
    result = run_refs(FIXTURES, "silver.FactSales")
    names = _writer_names(result)
    assert "silver.usp_load_factsales" in names
    assert "silver.vw_customersales" in result["readers"]
    # Check MERGE operation on the writer
    entry = next(w for w in result["writers"] if w["procedure"] == "silver.usp_load_factsales")
    assert "MERGE" in entry["write_operations"]


def test_refs_bronze_customer_readers():
    result = run_refs(FIXTURES, "bronze.Customer")
    assert "silver.usp_load_dimcustomer" in result["readers"]
    assert "silver.usp_load_with_cte" in result["readers"]
    # usp_conditional_load reads bronze.Customer inside IF/ELSE — partial,
    # flagged in llm_required for LLM to complete
    assert "silver.usp_conditional_load" in result["llm_required"]


def test_refs_unknown_object():
    result = run_refs(FIXTURES, "silver.DoesNotExist")
    assert result["readers"] == []
    assert result["writers"] == []


# ── function tracking ───────────────────────────────────────────────────────


def test_show_proc_uses_functions():
    result = run_show(FIXTURES, "silver.usp_load_formatted")
    assert result["type"] == "procedure"
    assert "silver.fn_format_name" in result["refs"]["uses_functions"]


def test_refs_function_finds_callers():
    result = run_refs(FIXTURES, "silver.fn_format_name")
    assert "silver.usp_load_formatted" in result["readers"]


def test_refs_rejects_procedure():
    result = run_refs(FIXTURES, "silver.usp_load_dimcustomer")
    assert "error" in result
