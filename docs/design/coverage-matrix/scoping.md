# Scoping Coverage

Current automated statement coverage for the scoping phase. The phase boundary here includes deterministic writer discovery, statement classification surfaced by `discover`, routing flags, and enrichment-aware resolution used by scoping.

| # | Statement | Unit | Integration | Promptfoo |
|---|---|---|---|---|
| 1 | `INSERT ... SELECT` | Yes |  |  |
| 2 | `UPDATE` with join | Yes |  |  |
| 3 | `DELETE` with `WHERE` | Yes |  |  |
| 4 | `DELETE TOP` | Yes |  |  |
| 5 | `TRUNCATE TABLE` | Yes |  |  |
| 6 | `TRUNCATE` + `INSERT` |  |  | Yes |
| 7 | `MERGE INTO` | Yes |  | Yes |
| 8 | `SELECT INTO` | Yes |  |  |
| 9 | Single CTE | Yes |  |  |
| 10 | Multi-level CTE | Yes |  |  |
| 11 | Sequential `WITH` blocks | Yes |  |  |
| 12 | `CASE WHEN` | Yes |  |  |
| 13 | `LEFT OUTER JOIN` |  |  |  |
| 14 | `RIGHT OUTER JOIN` | Yes |  |  |
| 15 | Subquery in `WHERE` | Yes |  |  |
| 16 | Correlated subquery | Yes |  |  |
| 17 | Window functions | Yes |  |  |
| 19 | `UNION ALL` |  |  |  |
| 20 | `UNION` |  |  |  |
| 21 | `INTERSECT` |  |  |  |
| 22 | `EXCEPT` |  |  |  |
| 23 | `UNION ALL` in CTE branch |  |  |  |
| 24 | Explicit `INNER JOIN` |  |  |  |
| 25 | `FULL OUTER JOIN` |  |  |  |
| 26 | `CROSS JOIN` |  |  |  |
| 27 | `CROSS APPLY` |  |  |  |
| 28 | `OUTER APPLY` |  |  |  |
| 29 | Self-join |  |  |  |
| 30 | Derived table in `FROM` |  |  |  |
| 31 | Scalar subquery in `SELECT` |  |  |  |
| 32 | `EXISTS` subquery |  |  |  |
| 33 | `NOT EXISTS` subquery |  |  |  |
| 34 | `IN` subquery |  |  |  |
| 35 | `NOT IN` subquery |  |  |  |
| 36 | Recursive CTE |  |  |  |
| 37 | `UPDATE` with CTE prefix |  |  |  |
| 38 | `DELETE` with CTE prefix |  |  |  |
| 39 | `MERGE` with CTE source |  |  |  |
| 40 | `GROUPING SETS` |  |  |  |
| 41 | `CUBE` |  |  |  |
| 42 | `ROLLUP` |  |  |  |
| 43 | `PIVOT` |  |  |  |
| 44 | `UNPIVOT` |  |  |  |
| 49 | `EXEC proc` | Yes |  | Yes |
| 50 | `EXEC [schema].[proc]` |  |  |  |
| 51 | `EXEC proc` with params | Yes |  |  |
| 52 | `EXEC proc` with `OUTPUT` |  |  |  |
| 53 | `EXECUTE proc` keyword form | Yes |  |  |
| 54 | `EXEC @rc = proc` |  |  |  |
| 57 | Static `sp_executesql` |  |  |  |
| 45 | `IF / ELSE` control flow | Yes |  |  |
| 46 | `TRY / CATCH` | Yes |  |  |
| 47 | `WHILE` loop | Yes |  |  |
| 48 | Nested control flow | Yes |  |  |
| 55 | Cross-database `EXEC` |  |  | Yes |
| 56 | Linked-server `EXEC` |  |  |  |
| 58 | Dynamic `sp_executesql` | Yes |  | Yes |
| 59 | `EXEC (@sql)` | Yes |  |  |
| 60 | `EXEC ('...' + @var)` |  |  |  |
| S1 | `SET` |  |  |  |
| S2 | `DECLARE` |  |  |  |
| S3 | `RETURN` |  |  |  |
| S4 | `PRINT` |  |  |  |
| S5 | `RAISERROR` |  |  |  |
| S6 | `THROW` |  |  |  |
| S7 | `BEGIN / COMMIT / ROLLBACK` |  |  |  |
| S8 | `DROP / CREATE INDEX` |  |  |  |
