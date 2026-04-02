# Statement Inventory

Canonical statement list for the phase coverage matrices in this directory. Pattern numbers come from [T-SQL Parse Classification](../tsql-parse-classification/README.md).

## Deterministic

| # | Statement | Classification |
|---|---|---|
| 1 | `INSERT ... SELECT` | Deterministic |
| 2 | `UPDATE` with join | Deterministic |
| 3 | `DELETE` with `WHERE` | Deterministic |
| 4 | `DELETE TOP` | Deterministic |
| 5 | `TRUNCATE TABLE` | Deterministic |
| 6 | `TRUNCATE` + `INSERT` | Deterministic |
| 7 | `MERGE INTO` | Deterministic |
| 8 | `SELECT INTO` | Deterministic |
| 9 | Single CTE | Deterministic |
| 10 | Multi-level CTE | Deterministic |
| 11 | Sequential `WITH` blocks | Deterministic |
| 12 | `CASE WHEN` | Deterministic |
| 13 | `LEFT OUTER JOIN` | Deterministic |
| 14 | `RIGHT OUTER JOIN` | Deterministic |
| 15 | Subquery in `WHERE` | Deterministic |
| 16 | Correlated subquery | Deterministic |
| 17 | Window functions | Deterministic |
| 19 | `UNION ALL` | Deterministic |
| 20 | `UNION` | Deterministic |
| 21 | `INTERSECT` | Deterministic |
| 22 | `EXCEPT` | Deterministic |
| 23 | `UNION ALL` in CTE branch | Deterministic |
| 24 | Explicit `INNER JOIN` | Deterministic |
| 25 | `FULL OUTER JOIN` | Deterministic |
| 26 | `CROSS JOIN` | Deterministic |
| 27 | `CROSS APPLY` | Deterministic |
| 28 | `OUTER APPLY` | Deterministic |
| 29 | Self-join | Deterministic |
| 30 | Derived table in `FROM` | Deterministic |
| 31 | Scalar subquery in `SELECT` | Deterministic |
| 32 | `EXISTS` subquery | Deterministic |
| 33 | `NOT EXISTS` subquery | Deterministic |
| 34 | `IN` subquery | Deterministic |
| 35 | `NOT IN` subquery | Deterministic |
| 36 | Recursive CTE | Deterministic |
| 37 | `UPDATE` with CTE prefix | Deterministic |
| 38 | `DELETE` with CTE prefix | Deterministic |
| 39 | `MERGE` with CTE source | Deterministic |
| 40 | `GROUPING SETS` | Deterministic |
| 41 | `CUBE` | Deterministic |
| 42 | `ROLLUP` | Deterministic |
| 43 | `PIVOT` | Deterministic |
| 44 | `UNPIVOT` | Deterministic |

## Enrichment-Resolved

| # | Statement | Classification |
|---|---|---|
| 49 | `EXEC proc` | Deterministic after enrichment |
| 50 | `EXEC [schema].[proc]` | Deterministic after enrichment |
| 51 | `EXEC proc` with params | Deterministic after enrichment |
| 52 | `EXEC proc` with `OUTPUT` | Deterministic after enrichment |
| 53 | `EXECUTE proc` keyword form | Deterministic after enrichment |
| 54 | `EXEC @rc = proc` | Deterministic after enrichment |
| 57 | Static `sp_executesql` | Deterministic after DMF resolution |

## Claude-Assisted

| # | Statement | Classification |
|---|---|---|
| 45 | `IF / ELSE` control flow | Claude-assisted |
| 46 | `TRY / CATCH` | Claude-assisted |
| 47 | `WHILE` loop | Claude-assisted |
| 48 | Nested control flow | Claude-assisted |
| 55 | Cross-database `EXEC` | Claude-assisted / out of scope |
| 56 | Linked-server `EXEC` | Claude-assisted / out of scope |
| 58 | Dynamic `sp_executesql` | Claude-assisted |
| 59 | `EXEC (@sql)` | Claude-assisted |
| 60 | `EXEC ('...' + @var)` | Claude-assisted |

## Skip-Only

| Statement | Classification |
|---|---|
| `SET` | Skip |
| `DECLARE` | Skip |
| `RETURN` | Skip |
| `PRINT` | Skip |
| `RAISERROR` | Skip |
| `THROW` | Skip |
| `BEGIN / COMMIT / ROLLBACK` | Skip |
| `DROP / CREATE INDEX` | Skip |
