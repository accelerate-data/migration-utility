# Refactoring Coverage

Current automated statement coverage for the refactoring phase. The phase boundary includes DML extraction (sub-agent A), CTE restructuring (sub-agent B), and equivalence comparison via `compare_two_sql`.

| # | Statement | Unit | Integration | Promptfoo |
|---|---|---|---|---|
| 1 | `INSERT ... SELECT` | Yes | Yes | Yes |
| 2 | `UPDATE` with join | N/A | Yes | Yes |
| 3 | `DELETE` with `WHERE` | N/A | Yes | Yes |
| 4 | `DELETE TOP` | N/A | N/A | N/A |
| 5 | `TRUNCATE TABLE` | N/A | N/A | N/A |
| 6 | `TRUNCATE` + `INSERT` | N/A | N/A | Yes |
| 7 | `MERGE INTO` | Yes | Yes | Yes |
| 8 | `SELECT INTO` | N/A | N/A | Yes |
| 9 | Single CTE | N/A | N/A | Yes |
| 10 | Multi-level CTE | N/A | N/A | Yes |
| 11 | Sequential `WITH` blocks | N/A | N/A | N/A |
| 12 | `CASE WHEN` | N/A | Yes | Yes |
| 13 | `LEFT OUTER JOIN` | N/A | Yes | Yes |
| 17 | Window functions | N/A | Yes | Yes |
| 19 | `UNION ALL` | N/A | Yes | Yes |
| 20 | `UNION` | N/A | N/A | N/A |
| 21 | `INTERSECT` | N/A | Yes | N/A |
| 22 | `EXCEPT` | N/A | Yes | N/A |
| 24 | Explicit `INNER JOIN` | N/A | N/A | N/A |
| 25 | `FULL OUTER JOIN` | N/A | N/A | N/A |
| 26 | `CROSS JOIN` | N/A | Yes | N/A |
| 27 | `CROSS APPLY` | N/A | N/A | N/A |
| 28 | `OUTER APPLY` | N/A | Yes | Yes |
| 30 | Derived table in `FROM` | N/A | N/A | Yes |
| 31 | Scalar subquery in `SELECT` | N/A | Yes | N/A |
| 32 | `EXISTS` subquery | N/A | Yes | N/A |
| 33 | `NOT EXISTS` subquery | N/A | Yes | N/A |
| 36 | Recursive CTE | N/A | N/A | N/A |
| 37 | `UPDATE` with CTE prefix | N/A | Yes | Yes |
| 38 | `DELETE` with CTE prefix | N/A | N/A | Yes |
| 39 | `MERGE` with CTE source | N/A | N/A | Yes |
| 40 | `GROUPING SETS` | N/A | Yes | Yes |
| 43 | `PIVOT` | N/A | Yes | Yes |
| 44 | `UNPIVOT` | N/A | Yes | N/A |
| 45 | `IF / ELSE` control flow | N/A | N/A | Yes |
| 47 | `WHILE` loop | N/A | N/A | Yes |
| 55 | Cross-database `EXEC` | N/A | N/A | Yes |
| 56 | Linked-server `EXEC` | N/A | N/A | Yes |
| 58 | Dynamic `sp_executesql` | N/A | N/A | Yes |
| 59 | `EXEC (@sql)` | N/A | N/A | Yes |
| 60 | `EXEC ('...' + @var)` | N/A | N/A | Yes |

## Test layers

- **Unit**: `tests/unit/test_refactor.py` — symmetric_diff, context assembly, catalog write, CLI commands (19 tests)
- **Integration**: `tests/unit/test_compare_sql_integration.py` — `compare_two_sql` against live SQL Server with 30 scenarios covering all DML patterns, identity columns, FK constraints, NULL/MONEY handling, transaction rollback
- **Promptfoo**: `tests/evals/packages/refactoring-sql/` — 24 skill scenarios testing LLM extraction and CTE restructuring; `tests/evals/packages/cmd-refactor/` — 4 command orchestration scenarios
