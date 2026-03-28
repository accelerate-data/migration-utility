"""Tests for extract_refs and _parse_body_statements.

Covers all DML patterns that sqlglot handles deterministically, plus
control flow patterns that require the two-pass strategy.

SQL fixtures live in fixtures/discover/flat/*.sql.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from shared.loader import (
    _collect_refs_from_statements,
    _parse_body_statements,
    load_directory,
)

FIXTURES = Path(__file__).parent / "fixtures" / "discover" / "flat"


def _make_proc(body: str) -> str:
    return f"CREATE PROCEDURE silver.test_proc\nAS\nBEGIN\n{body}\nEND;"


def _refs_from_proc(body: str) -> tuple[list[str], list[str]]:
    raw_ddl = _make_proc(body)
    stmts = _parse_body_statements(raw_ddl)
    refs = _collect_refs_from_statements(stmts)
    return refs.writes_to, refs.reads_from


def _refs_from_fixture(proc_name: str) -> tuple[list[str], list[str]]:
    catalog = load_directory(FIXTURES)
    entry = catalog.procedures[proc_name]
    stmts = _parse_body_statements(entry.raw_ddl)
    refs = _collect_refs_from_statements(stmts)
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


def test_correlated_subquery():
    writes, reads = _refs_from_fixture("dbo.usp_correlatedsubquery")
    assert "silver.dimproduct" in writes
    assert "bronze.product" in reads


def test_delete_top():
    """DELETE TOP correctly identifies the target table, not the TOP pseudo-table."""
    writes, reads = _refs_from_proc("""
        DELETE TOP (1000) FROM silver.StagingTable
        WHERE BatchDate < DATEADD(DAY, -7, GETDATE());
    """)
    assert writes == ["silver.stagingtable"]


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


# ── Inline tests for patterns not in fixtures ────────────────────────────────


def test_left_outer_join():
    writes, reads = _refs_from_proc("""
        INSERT INTO silver.FactSales (SalesKey, CustomerName)
        SELECT s.SalesKey, c.FirstName
        FROM bronze.Sales s
        LEFT OUTER JOIN bronze.Customer c ON s.CustomerKey = c.CustomerKey;
    """)
    assert writes == ["silver.factsales"]
    assert "bronze.sales" in reads
    assert "bronze.customer" in reads


def test_right_outer_join():
    writes, reads = _refs_from_proc("""
        INSERT INTO silver.FactReturns (ReturnKey, ProductName)
        SELECT r.ReturnKey, p.Name
        FROM bronze.Returns r
        RIGHT OUTER JOIN bronze.Product p ON r.ProductKey = p.ProductKey;
    """)
    assert writes == ["silver.factreturns"]
    assert "bronze.returns" in reads
    assert "bronze.product" in reads


def test_subquery_in_where():
    writes, reads = _refs_from_proc("""
        INSERT INTO silver.HighValueOrders (OrderKey, Amount)
        SELECT OrderID, TotalDue
        FROM bronze.SalesOrder
        WHERE TotalDue > (SELECT AVG(TotalDue) FROM bronze.SalesOrder);
    """)
    assert writes == ["silver.highvalueorders"]
    assert reads == ["bronze.salesorder"]


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
    (tmp_path / "procedures.sql").write_text(sql)
    catalog = load_directory(tmp_path)
    assert "silver.usp_good" in catalog.procedures
    assert "silver.usp_bad" in catalog.procedures
    assert catalog.procedures["silver.usp_bad"].parse_error is not None
    assert catalog.procedures["silver.usp_good"].parse_error is None
