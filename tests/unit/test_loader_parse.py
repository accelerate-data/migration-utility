"""Unit tests for loader_parse.py — classify_statement and parse_body_statements.

Per-pattern coverage for docs/design/coverage-matrix/scoping.md.
Pattern numbers correspond to docs/design/coverage-matrix/statement-inventory.md.
"""

from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp

from shared.loader_parse import (
    _strip_trailing_sql_comments,
    classify_statement,
    parse_body_statements,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def _parse_one(sql: str) -> object:
    result = sqlglot.parse_one(sql, dialect="tsql")
    assert result is not None
    return result


def _wrap_proc(body: str) -> str:
    return f"CREATE PROCEDURE dbo.usp_Test\nAS\nBEGIN\n{body}\nEND"


# ── classify_statement: migrate patterns ─────────────────────────────────────


def test_classify_insert_select() -> None:
    """#1 INSERT ... SELECT → migrate."""
    assert classify_statement(_parse_one("INSERT INTO silver.T (C) SELECT 1")) == "migrate"


def test_classify_update_with_join() -> None:
    """#2 UPDATE with join → migrate."""
    assert classify_statement(_parse_one(
        "UPDATE d SET d.C = s.C FROM silver.T d JOIN bronze.S s ON d.Id = s.Id"
    )) == "migrate"


def test_classify_delete_with_where() -> None:
    """#3 DELETE with WHERE → migrate."""
    assert classify_statement(_parse_one("DELETE FROM silver.T WHERE C IS NULL")) == "migrate"


def test_classify_delete_top() -> None:
    """#4 DELETE TOP → migrate."""
    assert classify_statement(_parse_one("DELETE TOP (500) FROM silver.T WHERE C < 0")) == "migrate"


def test_classify_merge_into() -> None:
    """#7 MERGE INTO → migrate."""
    assert classify_statement(_parse_one(
        "MERGE INTO silver.T AS tgt USING bronze.S AS src ON tgt.Id = src.Id "
        "WHEN MATCHED THEN UPDATE SET tgt.C = src.C "
        "WHEN NOT MATCHED THEN INSERT (C) VALUES (src.C)"
    )) == "migrate"


def test_classify_select_into() -> None:
    """#8 SELECT INTO → migrate."""
    assert classify_statement(_parse_one("SELECT C INTO silver.T FROM bronze.S")) == "migrate"


def test_classify_case_when_in_insert() -> None:
    """#12 CASE WHEN inside INSERT SELECT → migrate (outer node is Insert)."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT CASE WHEN C IS NULL THEN 'X' ELSE C END FROM bronze.S"
    )) == "migrate"


def test_classify_left_outer_join_in_insert() -> None:
    """#13 LEFT OUTER JOIN inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT s.C FROM bronze.S s LEFT OUTER JOIN dbo.Cfg c ON s.Id = c.Id"
    )) == "migrate"


def test_classify_right_outer_join_in_insert() -> None:
    """#14 RIGHT OUTER JOIN inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT s.C FROM dbo.Cfg c RIGHT OUTER JOIN bronze.S s ON c.Id = s.Id"
    )) == "migrate"


def test_classify_subquery_in_where() -> None:
    """#15 Subquery in WHERE → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S WHERE Id > (SELECT AVG(Id) FROM bronze.S)"
    )) == "migrate"


def test_classify_correlated_subquery() -> None:
    """#16 Correlated subquery → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT p.C FROM bronze.S p "
        "WHERE p.Id = (SELECT MAX(p2.Id) FROM bronze.S p2 WHERE p2.C = p.C)"
    )) == "migrate"


def test_classify_window_functions_in_insert() -> None:
    """#17 Window functions inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM "
        "(SELECT C, ROW_NUMBER() OVER (ORDER BY Id) AS rn FROM bronze.S) t WHERE rn <= 10"
    )) == "migrate"


def test_classify_union_all_in_insert() -> None:
    """#19 UNION ALL inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S1 UNION ALL SELECT C FROM bronze.S2"
    )) == "migrate"


def test_classify_union_in_insert() -> None:
    """#20 UNION inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S1 UNION SELECT C FROM bronze.S2"
    )) == "migrate"


def test_classify_intersect_in_insert() -> None:
    """#21 INTERSECT inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S1 INTERSECT SELECT C FROM bronze.S2"
    )) == "migrate"


def test_classify_except_in_insert() -> None:
    """#22 EXCEPT inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S1 EXCEPT SELECT C FROM bronze.S2"
    )) == "migrate"


def test_classify_inner_join_in_insert() -> None:
    """#24 Explicit INNER JOIN inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT s.C FROM bronze.S s INNER JOIN dbo.Cfg c ON s.Id = c.Id"
    )) == "migrate"


def test_classify_cross_join_in_insert() -> None:
    """#26 CROSS JOIN inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT s.C FROM bronze.S s CROSS JOIN dbo.Cfg c"
    )) == "migrate"


def test_classify_outer_apply_in_insert() -> None:
    """#28 OUTER APPLY inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT s.C FROM bronze.S s "
        "OUTER APPLY (SELECT TOP 1 Val FROM dbo.Cfg) c"
    )) == "migrate"


def test_classify_derived_table_in_insert() -> None:
    """#30 Derived table in FROM → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM (SELECT C FROM bronze.S WHERE Id > 0) t"
    )) == "migrate"


def test_classify_exists_subquery_in_insert() -> None:
    """#32 EXISTS subquery → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S s "
        "WHERE EXISTS (SELECT 1 FROM dbo.Cfg c WHERE c.Id = s.Id)"
    )) == "migrate"


def test_classify_not_exists_subquery_in_insert() -> None:
    """#33 NOT EXISTS subquery → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S s "
        "WHERE NOT EXISTS (SELECT 1 FROM dbo.Cfg c WHERE c.Id = s.Id)"
    )) == "migrate"


def test_classify_in_subquery_in_insert() -> None:
    """#34 IN subquery → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S "
        "WHERE Id IN (SELECT Id FROM dbo.Cfg)"
    )) == "migrate"


def test_classify_not_in_subquery_in_insert() -> None:
    """#35 NOT IN subquery → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S "
        "WHERE Id NOT IN (SELECT Id FROM dbo.Cfg)"
    )) == "migrate"


def test_classify_grouping_sets_in_insert() -> None:
    """#40 GROUPING SETS inside INSERT SELECT → migrate."""
    assert classify_statement(_parse_one(
        "INSERT INTO silver.T (C) SELECT C FROM bronze.S GROUP BY GROUPING SETS ((C), ())"
    )) == "migrate"


# ── classify_statement: CTE-prefixed DML (patterns parsed via body) ───────────


def test_parse_body_single_cte_insert() -> None:
    """#9 Single CTE wrapping INSERT: body produces Insert statements."""
    ddl = _wrap_proc("""
    WITH ranked AS (SELECT Id, C FROM bronze.S)
    INSERT INTO silver.T (C) SELECT C FROM ranked;
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert needs_llm is False


def test_parse_body_multi_level_cte_insert() -> None:
    """#10 Multi-level CTE wrapping INSERT: body produces Insert statements."""
    ddl = _wrap_proc("""
    WITH base AS (SELECT Id, C FROM bronze.S),
    filtered AS (SELECT Id, C FROM base WHERE C IS NOT NULL)
    INSERT INTO silver.T (C) SELECT C FROM filtered;
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert needs_llm is False


def test_parse_body_sequential_with_blocks() -> None:
    """#11 Sequential WITH blocks: both INSERT statements extracted."""
    ddl = _wrap_proc("""
    WITH base AS (SELECT Id, C FROM bronze.S)
    INSERT INTO silver.T (C) SELECT C FROM base;

    WITH extra AS (SELECT Id, C FROM bronze.S WHERE C IS NOT NULL)
    INSERT INTO silver.T (C) SELECT C FROM extra;
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types


def test_parse_body_recursive_cte_insert() -> None:
    """#36 Recursive CTE: body produces Insert statement."""
    ddl = _wrap_proc("""
    WITH org AS (
        SELECT 0 AS Lvl, ManagerID AS ParentId, EmployeeID AS NodeId
        FROM bronze.Employee WHERE ManagerID IS NULL
        UNION ALL
        SELECT o.Lvl + 1, e.ManagerID, e.EmployeeID
        FROM bronze.Employee e JOIN org o ON e.ManagerID = o.NodeId
    )
    INSERT INTO silver.T (Lvl, ParentId, NodeId)
    SELECT Lvl, ParentId, NodeId FROM org;
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert needs_llm is False


def test_parse_body_update_with_cte_prefix() -> None:
    """#37 UPDATE with CTE prefix: body produces Update statement."""
    ddl = _wrap_proc("""
    WITH src AS (SELECT Id, C FROM bronze.S WHERE C IS NOT NULL)
    UPDATE silver.T SET T.C = src.C FROM silver.T JOIN src ON T.Id = src.Id;
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Update" in types
    assert needs_llm is False


def test_parse_body_delete_with_cte_prefix() -> None:
    """#38 DELETE with CTE prefix: body produces Delete statement."""
    ddl = _wrap_proc("""
    WITH old AS (SELECT Id FROM silver.T WHERE C IS NULL)
    DELETE FROM silver.T WHERE Id IN (SELECT Id FROM old);
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Delete" in types
    assert needs_llm is False


def test_parse_body_merge_with_cte_source() -> None:
    """#39 MERGE with CTE source: body produces Merge statement."""
    ddl = _wrap_proc("""
    WITH src AS (SELECT Id, C FROM bronze.S)
    MERGE INTO silver.T AS tgt USING src ON tgt.Id = src.Id
    WHEN MATCHED THEN UPDATE SET tgt.C = src.C
    WHEN NOT MATCHED THEN INSERT (C) VALUES (src.C);
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Merge" in types
    assert needs_llm is False


def test_parse_body_allows_trailing_comment_after_end() -> None:
    """Trailing comments after END should not prevent body extraction."""
    ddl = (
        "CREATE PROCEDURE dbo.test_proc\n"
        "AS\n"
        "BEGIN\n"
        "    SELECT 1;\n"
        "END\n"
        "-- generated by tooling\n"
    )
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    assert len(stmts) == 1
    assert needs_llm is False


def test_strip_trailing_sql_comments_removes_same_line_and_block_comments() -> None:
    sql = (
        "CREATE PROCEDURE dbo.test_proc\n"
        "AS\n"
        "BEGIN\n"
        "    SELECT 1;\n"
        "END; -- repeated -- -- -- comment\n"
        "/* repeated *//* repeated *//* trailing block */\n"
    )

    assert _strip_trailing_sql_comments(sql) == (
        "CREATE PROCEDURE dbo.test_proc\n"
        "AS\n"
        "BEGIN\n"
        "    SELECT 1;\n"
        "END;"
    )


# ── classify_statement: skip patterns ────────────────────────────────────────


def test_classify_truncate_table_is_skip() -> None:
    """#5 TRUNCATE TABLE → skip."""
    assert classify_statement(_parse_one("TRUNCATE TABLE silver.T")) == "skip"


def test_classify_set_is_skip() -> None:
    """S1 SET statement → skip."""
    assert classify_statement(_parse_one("SET NOCOUNT ON")) == "skip"


# ── parse_body_statements: control flow patterns ─────────────────────────────


def test_parse_body_if_else_flattens_both_branches() -> None:
    """#45 IF/ELSE: both branches flattened into migrate statements."""
    ddl = _wrap_proc("""
    IF EXISTS (SELECT 1 FROM dbo.Cfg WHERE K = 'x')
    BEGIN
        INSERT INTO silver.T (C) SELECT C FROM bronze.S;
    END
    ELSE
    BEGIN
        UPDATE silver.T SET C = s.C FROM silver.T d JOIN bronze.S s ON d.Id = s.Id;
    END
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert "Update" in types
    assert needs_llm is False


def test_parse_body_try_catch_flattens_both_branches() -> None:
    """#46 TRY/CATCH: TRY and CATCH branches both flattened."""
    ddl = _wrap_proc("""
    BEGIN TRY
        INSERT INTO silver.T (C) SELECT C FROM bronze.S;
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.Cfg (K, V) SELECT 'error', ERROR_MESSAGE();
    END CATCH
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert needs_llm is False


def test_parse_body_while_loop_flattens_body() -> None:
    """#47 WHILE loop: loop body extracted as migrate statements."""
    ddl = _wrap_proc("""
    WHILE EXISTS (SELECT 1 FROM bronze.S WHERE Processed = 0)
    BEGIN
        INSERT INTO silver.T (C) SELECT TOP (100) C FROM bronze.S WHERE Processed = 0;
    END
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert needs_llm is False


def test_parse_body_nested_control_flow_all_branches() -> None:
    """#48 Nested control flow (IF inside TRY/CATCH): all leaf DML extracted."""
    ddl = _wrap_proc("""
    SET NOCOUNT ON;
    BEGIN TRY
        IF EXISTS (SELECT 1 FROM dbo.Cfg WHERE K = 'full_reload')
        BEGIN
            TRUNCATE TABLE silver.T;
            INSERT INTO silver.T (C) SELECT C FROM bronze.S;
        END
        ELSE
        BEGIN
            MERGE INTO silver.T AS tgt USING bronze.S AS src ON tgt.Id = src.Id
            WHEN MATCHED THEN UPDATE SET tgt.C = src.C
            WHEN NOT MATCHED THEN INSERT (C) VALUES (src.C);
        END
    END TRY
    BEGIN CATCH
        INSERT INTO dbo.Cfg (K, V) SELECT 'error', ERROR_MESSAGE();
    END CATCH
    """)
    stmts, needs_llm, _seg_err = parse_body_statements(ddl)
    types = {type(s).__name__ for s in stmts}
    assert "Insert" in types
    assert "Merge" in types
    assert needs_llm is False
