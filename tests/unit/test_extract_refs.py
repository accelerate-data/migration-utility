"""Tests for extract_refs and parse_body_statements.

Covers all DML patterns that sqlglot handles deterministically, plus
control flow patterns that require the two-pass strategy.

SQL fixtures live in fixtures/discover/flat/*.sql.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pytest
import sqlglot

from shared.catalog import scan_routing_flags
from shared.loader import (
    collect_refs_from_statements,
    load_directory,
    parse_body_statements,
)
from shared.loader_parse import classify_statement

FIXTURES = Path(__file__).parent / "fixtures" / "discover" / "flat"


def _make_proc(body: str) -> str:
    return f"CREATE PROCEDURE silver.test_proc\nAS\nBEGIN\n{body}\nEND;"


def _refs_from_proc(body: str) -> tuple[list[str], list[str]]:
    raw_ddl = _make_proc(body)
    stmts, _needs_llm, _seg_err = parse_body_statements(raw_ddl)
    refs = collect_refs_from_statements(stmts)
    return refs.writes_to, refs.reads_from


def _ops_from_proc(body: str) -> dict[str, list[str]]:
    raw_ddl = _make_proc(body)
    stmts, _needs_llm, _seg_err = parse_body_statements(raw_ddl)
    refs = collect_refs_from_statements(stmts)
    return refs.write_operations


@lru_cache(maxsize=1)
def _catalog():
    return load_directory(FIXTURES)


def _refs_from_fixture(proc_name: str) -> tuple[list[str], list[str]]:
    entry = _catalog().procedures[proc_name]
    stmts, _needs_llm, _seg_err = parse_body_statements(entry.raw_ddl)
    refs = collect_refs_from_statements(stmts)
    return refs.writes_to, refs.reads_from


# ── DML patterns from fixtures ───────────────────────────────────────────────


def test_truncate_insert():
    writes, reads = _refs_from_fixture("dbo.usp_loaddimproduct")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_merge():
    writes, reads = _refs_from_fixture("dbo.usp_mergedimproduct")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_cte():
    writes, reads = _refs_from_fixture("dbo.usp_loadwithcte")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_sequential_with_blocks():
    writes, reads = _refs_from_fixture("dbo.usp_sequentialwith")
    assert "silver.dimproduct" in writes
    assert "dbo.config" in writes
    assert "bronze.product" in reads


def test_multi_level_cte():
    writes, reads = _refs_from_fixture("dbo.usp_loadwithmulticte")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_case_when():
    writes, reads = _refs_from_fixture("dbo.usp_loadwithcase")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_if_else_dual_merge():
    writes, reads = _refs_from_fixture("dbo.usp_conditionalmerge")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_try_catch():
    writes, reads = _refs_from_fixture("dbo.usp_trycatchload")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads
    assert "dbo.config" in writes


def test_correlated_subquery():
    writes, reads = _refs_from_fixture("dbo.usp_correlatedsubquery")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_update_with_join():
    writes, reads = _refs_from_fixture("dbo.usp_simpleupdate")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_delete_with_where():
    writes, reads = _refs_from_fixture("dbo.usp_simpledelete")
    assert "silver.dimproduct" in writes


def test_delete_top():
    writes, reads = _refs_from_fixture("dbo.usp_deletetop")
    assert "silver.dimproduct" in writes


def test_truncate_only():
    writes, reads = _refs_from_fixture("dbo.usp_truncateonly")
    assert "silver.dimproduct" in writes


def test_select_into():
    writes, reads = _refs_from_fixture("dbo.usp_selectinto")
    assert "silver.dimproduct_staging" in writes
    assert "bronze.product" in reads


def test_right_outer_join():
    writes, reads = _refs_from_fixture("dbo.usp_rightouterjoin")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_subquery_in_where():
    writes, reads = _refs_from_fixture("dbo.usp_subqueryinwhere")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_window_function():
    writes, reads = _refs_from_fixture("dbo.usp_windowfunction")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


@pytest.mark.parametrize(
    ("proc_name", "expected_writes", "expected_reads"),
    [
        ("dbo.usp_loadwithleftjoin", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_unionall", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_union", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_intersect", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_except", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_unionallincte", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_innerjoin", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_fullouterjoin", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_crossjoin", ["dbo.config"], ["bronze.product"]),
        ("dbo.usp_crossapply", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_outerapply", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_selfjoin", ["dbo.config"], ["bronze.product"]),
        ("dbo.usp_derivedtable", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_scalarsubquery", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_existssubquery", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_notexistssubquery", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_insubquery", ["silver.dimproduct"], ["bronze.product", "dbo.config"]),
        ("dbo.usp_notinsubquery", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_recursivecte", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_updatewithcte", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_deletewithcte", ["silver.dimproduct"], []),
        ("dbo.usp_mergewithcte", ["silver.dimproduct"], ["bronze.product"]),
        ("dbo.usp_groupingsets", ["dbo.config"], ["bronze.product"]),
        ("dbo.usp_cube", ["dbo.config"], ["bronze.product"]),
        ("dbo.usp_rollup", ["dbo.config"], ["bronze.product"]),
        ("dbo.usp_pivot", ["dbo.config"], ["bronze.product"]),
        ("dbo.usp_unpivot", ["dbo.config"], ["bronze.product"]),
    ],
)
def test_ref_extraction_for_missing_statement_patterns(
    proc_name: str,
    expected_writes: list[str],
    expected_reads: list[str],
) -> None:
    writes, reads = _refs_from_fixture(proc_name)
    assert writes == expected_writes
    assert reads == expected_reads


@pytest.mark.parametrize(
    "sql",
    [
        "SET NOCOUNT ON",
        "DECLARE @x INT",
        "RETURN",
        "PRINT 'hello'",
        "RAISERROR ('bad', 16, 1)",
        "THROW 50000, 'bad', 1",
        "BEGIN TRANSACTION",
        "COMMIT",
        "ROLLBACK",
    ],
)
def test_skip_only_statements_classify_as_skip(sql: str) -> None:
    stmt = sqlglot.parse_one(sql, dialect="tsql", error_level=sqlglot.ErrorLevel.WARN)
    assert classify_statement(stmt) == "skip"


def test_truncate_plus_insert_tracks_both_write_operations() -> None:
    writes, reads = _refs_from_proc("""
        TRUNCATE TABLE silver.DimProduct;
        INSERT INTO silver.DimProduct (ProductKey)
        SELECT ProductID FROM bronze.Product;
    """)
    write_ops = _ops_from_proc("""
        TRUNCATE TABLE silver.DimProduct;
        INSERT INTO silver.DimProduct (ProductKey)
        SELECT ProductID FROM bronze.Product;
    """)
    assert writes == ["silver.dimproduct"]
    assert reads == ["bronze.product"]
    assert write_ops == {"silver.dimproduct": ["TRUNCATE", "INSERT"]}


def test_while_loop():
    writes, reads = _refs_from_fixture("dbo.usp_whileloop")
    assert "bronze.product" in writes  # DELETE FROM bronze.Product
    assert "dbo.config" in writes


def test_nested_control_flow():
    writes, reads = _refs_from_fixture("dbo.usp_nestedcontrolflow")
    assert "silver.dimproduct" in writes
    assert "dbo.config" in writes
    assert "bronze.product" in reads


def test_drop_index_truncate_merge_create_index():
    """Full reload pattern with DROP/CREATE INDEX works via body parsing."""
    writes, reads = _refs_from_proc("""
        DROP INDEX IX_Name ON silver.DimProduct;
        TRUNCATE TABLE silver.DimProduct;
        MERGE INTO silver.DimProduct AS tgt
        USING bronze.Product AS src ON tgt.ProductKey = src.ProductID
        WHEN NOT MATCHED THEN
            INSERT (ProductKey, Name) VALUES (src.ProductID, src.Name);
        CREATE INDEX IX_Name ON silver.DimProduct (Name);
    """)
    assert "silver.dimproduct" in writes
    assert reads == ["bronze.product"]


def test_cte_with_subquery():
    writes, reads = _refs_from_proc("""
        WITH active_customers AS (
            SELECT CustomerKey, FirstName
            FROM bronze.Customer
            WHERE CustomerKey IN (
                SELECT DISTINCT CustomerID FROM bronze.SalesOrder
            )
        ),
        with_geo AS (
            SELECT ac.CustomerKey, ac.FirstName, g.Country
            FROM active_customers ac
            JOIN bronze.Geography g ON ac.CustomerKey = g.CustomerKey
        )
        INSERT INTO silver.DimActiveCustomer (CustomerKey, FirstName, Country)
        SELECT CustomerKey, FirstName, Country FROM with_geo;
    """)
    assert writes == ["silver.dimactivecustomer"]
    assert "bronze.customer" in reads
    assert "bronze.salesorder" in reads
    assert "bronze.geography" in reads


# ── EXEC patterns ─────────────────────────────────────────────────────────────


def test_exec_simple_has_exec_flag():
    entry = _catalog().procedures["dbo.usp_execsimple"]
    from shared.loader import extract_refs
    refs = extract_refs(entry)
    assert refs.needs_llm is False
    assert refs.writes_to == []
    assert refs.reads_from == []


def test_exec_dynamic_has_exec_flag():
    entry = _catalog().procedures["dbo.usp_execdynamic"]
    from shared.loader import extract_refs
    refs = extract_refs(entry)
    assert refs.needs_llm is True


def test_exec_sp_executesql_has_exec_flag():
    entry = _catalog().procedures["dbo.usp_execspexecutesql"]
    from shared.loader import extract_refs
    refs = extract_refs(entry)
    assert refs.needs_llm is True


def test_exec_sp_executesql_literal_is_not_llm():
    raw_ddl = _make_proc("""
        EXEC sp_executesql N'INSERT INTO silver.DimProduct (ProductAlternateKey)
            SELECT CAST(ProductID AS NVARCHAR(25)) FROM bronze.Product';
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(raw_ddl)
    refs = collect_refs_from_statements(stmts)
    assert needs_llm is False
    assert refs.writes_to == []
    assert refs.reads_from == []


def test_deterministic_proc_no_exec_flag():
    entry = _catalog().procedures["dbo.usp_loaddimproduct"]
    from shared.loader import extract_refs
    refs = extract_refs(entry)
    assert refs.needs_llm is False


@pytest.mark.parametrize(
    ("sql", "expected_reason"),
    [
        ("EXEC [schema].[proc]", "static_exec"),
        ("EXEC proc @result OUTPUT", "static_exec"),
        ("EXEC @rc = proc", "static_exec"),
        ("EXEC OtherDB.dbo.proc", "cross_db_exec"),
        ("EXEC [Server].db.dbo.proc", "linked_server_exec"),
        ("EXEC ('INSERT INTO ' + @table)", "dynamic_sql_variable"),
    ],
)
def test_exec_routing_flags_cover_missing_variants(sql: str, expected_reason: str) -> None:
    flags = scan_routing_flags(sql)
    assert expected_reason in flags["routing_reasons"]

    if expected_reason == "dynamic_sql_variable":
        assert flags["needs_llm"] is True
    else:
        assert flags["needs_enrich"] is True


# ── Per-block error handling ──────────────────────────────────────────────────


def test_parse_error_does_not_abort_file(tmp_path):
    sql = (
        "CREATE PROCEDURE silver.usp_good\n"
        "AS\nBEGIN\n"
        "    INSERT INTO silver.Target (Col) SELECT Col FROM bronze.Source;\n"
        "END;\nGO\n"
        "CREATE PROCEDURE silver.usp_bad\n"
        "AS\nBEGIN\n"
        "    SOME TOTALLY INVALID SQL THAT CANNOT PARSE;\n"
        "END;\nGO\n"
    )
    ddl_dir = tmp_path / "ddl"
    ddl_dir.mkdir()
    (ddl_dir / "procedures.sql").write_text(sql)
    catalog = load_directory(tmp_path)
    assert "silver.usp_good" in catalog.procedures
    assert "silver.usp_bad" in catalog.procedures
    assert catalog.procedures["silver.usp_bad"].parse_error is not None
    assert catalog.procedures["silver.usp_good"].parse_error is None
