# Kimball Fixture — Procedure Coverage Matrix

Maps each stored procedure to its primary pattern category. All procedures exist in all three dialects (SQL Server, Oracle, PostgreSQL) with semantic equivalence.

## Procedure → Pattern Category

| # | Procedure | Target Table(s) | Primary Patterns | Eval pattern tags |
|---|---|---|---|---|
| 1 | `usp_load_dim_customer` | `dim_customer` | MERGE SCD2, CTE (view-backed), CASE, EXEC call | `merge`, `cte`, `exec-chain` |
| 2 | `usp_load_dim_product` | `dim_product` | MERGE SCD2, multi-level CTE, INNER JOINs | `merge` |
| 3 | `usp_load_dim_employee` | `dim_employee` | MERGE SCD1 (upsert), self-join, COALESCE, scalar subquery | *(gap — self-join, correlated-subquery)* |
| 4 | `usp_load_dim_product_category` | `dim_product_category` | MERGE SCD2, EXISTS change detection | `merge` |
| 5 | `usp_load_dim_address_and_credit_card` | `dim_address`, `dim_credit_card` | Multi-table MERGE, OUTER APPLY / LATERAL | `truncate-insert-outer-apply` |
| 6 | `usp_load_fct_sales_daily` | `fct_sales` | INSERT…SELECT, multi-JOIN, EXISTS filter, IF/ELSE, TRY/CATCH | `insert-select`, `not-exists`, `if-else`, `try-catch` |
| 7 | `usp_load_fct_sales_historical` | `fct_sales` | TRUNCATE+INSERT, WHILE batch loop, intentional multi-writer | `while-loop` |
| 8 | `usp_load_fct_sales_summary` | `fct_sales_summary` | TRUNCATE+INSERT, ROLLUP, GROUPING() | `rollup` |
| 9 | `usp_load_fct_sales_by_channel` | `fct_sales_by_channel` | TRUNCATE+INSERT, CTE with UNION ALL, aggregation | `union-all-in-cte` |
| 10 | `usp_load_rpt_customer_lifetime_value` | `rpt_customer_lifetime_value` | TRUNCATE+INSERT, view-backed (vw_customer_360), scalar subquery | `insert-select`, `correlated-subquery` |
| 11 | `usp_load_rpt_product_performance` | `rpt_product_performance` | TRUNCATE+INSERT, view-backed (vw_enriched_sales), LAG, ROW_NUMBER | `window-fn` |
| 12 | `usp_load_rpt_sales_by_territory` | `rpt_sales_by_territory` | TRUNCATE+INSERT, CROSS JOIN scaffold, LEFT JOIN, RANK | `cross-join` |
| 13 | `usp_load_rpt_employee_hierarchy` | `rpt_employee_hierarchy` | TRUNCATE+INSERT, recursive CTE, scalar subquery | `recursive-cte` |
| 14 | `usp_load_rpt_sales_by_category` | `rpt_sales_by_category` | TRUNCATE+INSERT, GROUPING SETS, GROUPING() | `grouping-sets` |
| 15 | `usp_load_rpt_channel_pivot` | `rpt_channel_pivot` | TRUNCATE+INSERT, PIVOT (SS/ORA) / conditional aggregation (PG) | `pivot` |
| 16 | `usp_load_rpt_returns_analysis` | `rpt_returns_analysis` | TRUNCATE+INSERT, LEFT JOIN, NOT EXISTS | `not-exists` |
| 17 | `usp_load_rpt_customer_segments` | `rpt_customer_segments` | TRUNCATE+INSERT, EXCEPT / MINUS, INTERSECT, UNION | `intersect`, `except`, `union-all` |
| 18 | `usp_load_rpt_address_coverage` | `rpt_address_coverage` | TRUNCATE+INSERT, NOT IN / EXCEPT gap analysis | `not-in`, `except` |
| 19 | `usp_load_gold_agg_batch` | `rpt_product_margin`, `rpt_date_sales_rollup` | CUBE, ROLLUP, EXEC chain | `cube`, `rollup`, `exec-chain` |
| 20 | `usp_exec_orchestrator_full_load` | (all via EXEC calls) | Static EXEC chain, full pipeline orchestration | `exec-orchestrator`, `static-sp-exec` |

## Pattern Category Summary

| # | Category | Patterns Covered | Procs |
|---|---|---|---|
| 1 | Standard load | INSERT…SELECT, TRUNCATE+INSERT, MERGE SCD1, MERGE SCD2, MERGE+CTE | #2–#9 |
| 2 | CTE | Single, multi-level, UNION ALL in CTE, recursive | #1, #2, #9, #11, #13 |
| 3 | Join | INNER, LEFT, SELF, CROSS, OUTER APPLY/LATERAL, derived table, correlated subquery | #1, #2, #3, #5, #6, #12, #17, #18 |
| 4 | Set operations | UNION ALL, UNION, EXCEPT/MINUS, INTERSECT | #9, #17 |
| 5 | Subquery | Scalar, EXISTS, NOT EXISTS, IN, NOT IN | #3, #4, #6, #10, #13, #16 |
| 6 | Aggregate | GROUPING SETS, CUBE, ROLLUP | #8, #14, #19 |
| 7 | Pivot | PIVOT, UNPIVOT (SS/ORA) / conditional aggregation (PG) | #15 |
| 8 | Control flow | IF/ELSE, TRY/CATCH, WHILE, nested | #6, #7 |
| 9 | Exec chains | Static EXEC, params, EXECUTE IMMEDIATE | #1, #19, #20 |
| 10 | View-backed | Procs reading from complex views | #1, #10, #11 |
| 11 | Coverage matrix | This document | — |

## Dialect-Specific Notes

| Pattern | SQL Server | Oracle | PostgreSQL |
|---|---|---|---|
| PIVOT | Native `PIVOT` | Native `PIVOT` | Conditional aggregation (`CASE WHEN`) |
| EXCEPT | `EXCEPT` | `MINUS` | `EXCEPT` |
| APPLY | `OUTER APPLY` | `LEFT JOIN … LATERAL` | `LEFT JOIN … LATERAL … ON TRUE` |
| Recursive CTE | `WITH cte AS` | `WITH cte AS` | `WITH RECURSIVE cte AS` |
| Batch loop | `WHILE` | Loop iteration | Loop iteration |
| Error handling | `BEGIN TRY/CATCH END` | `EXCEPTION WHEN OTHERS` | Nested `BEGIN … EXCEPTION … END` |
| EXEC | `EXEC dbo.proc` | `proc()` / `EXECUTE IMMEDIATE` | `CALL proc()` |
| Booleans | `BIT` (0/1) | `NUMBER(1)` (0/1) | `BOOLEAN` → normalized to 0/1 |

## Eval Coverage Gaps

Patterns present in the Kimball fixture but not yet covered by a `promptfooconfig.yaml` scenario:

| Gap | Kimball proc(s) | Suggested pattern tag |
|---|---|---|
| Self-join (employee hierarchy SCD1) | #3 | `self-join` |
| Multi-table MERGE (two target tables in one proc) | #5 | `multi-table-merge` |
| MERGE SCD2 with multi-level CTE | #1, #2, #4 | `merge-cte` |
| GROUPING() function inside ROLLUP/GROUPING SETS | #8, #14 | `grouping-fn` |
| PIVOT (dialect-specific) | #15 | already covered: `pivot` |

All other Kimball patterns (`insert-select`, `truncate-insert`, `union-all-in-cte`, `recursive-cte`, `window-fn`, `cross-join`, `not-exists`, `not-in`, `intersect`, `except`, `cube`, `rollup`, `grouping-sets`, `while-loop`, `if-else`, `try-catch`, `exec-orchestrator`, `static-sp-exec`) have a corresponding eval scenario in `tests/evals/promptfooconfig.yaml`.

## Multi-Writer Cases (Known Conflicts)

| Tables | Writers | Resolution |
|---|---|---|
| `fct_sales` | #6 (daily incremental) + #7 (historical TRUNCATE+INSERT) | Orchestrator calls #6 then #7; final state is historical full rebuild |
| `rpt_product_margin`, `rpt_date_sales_rollup` | #19 (single proc writes both) | Two tables, one proc — both are compared in parity |
