# Scoping Coverage

Current automated statement coverage for the scoping phase. The phase boundary here includes deterministic writer discovery, statement classification surfaced by `discover`, routing flags, and enrichment-aware resolution used by scoping.

| # | Statement | Unit | Integration | Promptfoo |
|---|---|---|---|---|
| 1 | `INSERT ... SELECT` | Yes | N/A | N/A |
| 2 | `UPDATE` with join | Yes | N/A | N/A |
| 3 | `DELETE` with `WHERE` | Yes | N/A | N/A |
| 4 | `DELETE TOP` | Yes | N/A | N/A |
| 5 | `TRUNCATE TABLE` | Yes | N/A | N/A |
| 6 | `TRUNCATE` + `INSERT` | Gap | N/A | Yes |
| 7 | `MERGE INTO` | Yes | N/A | Yes |
| 8 | `SELECT INTO` | Yes | N/A | N/A |
| 9 | Single CTE | Yes | N/A | N/A |
| 10 | Multi-level CTE | Yes | N/A | N/A |
| 11 | Sequential `WITH` blocks | Yes | N/A | N/A |
| 12 | `CASE WHEN` | Yes | N/A | N/A |
| 13 | `LEFT OUTER JOIN` | Gap | N/A | N/A |
| 14 | `RIGHT OUTER JOIN` | Yes | N/A | N/A |
| 15 | Subquery in `WHERE` | Yes | N/A | N/A |
| 16 | Correlated subquery | Yes | N/A | N/A |
| 17 | Window functions | Yes | N/A | N/A |
| 19 | `UNION ALL` | Gap | N/A | N/A |
| 20 | `UNION` | Gap | N/A | N/A |
| 21 | `INTERSECT` | Gap | N/A | N/A |
| 22 | `EXCEPT` | Gap | N/A | N/A |
| 23 | `UNION ALL` in CTE branch | Gap | N/A | N/A |
| 24 | Explicit `INNER JOIN` | Gap | N/A | N/A |
| 25 | `FULL OUTER JOIN` | Gap | N/A | N/A |
| 26 | `CROSS JOIN` | Gap | N/A | N/A |
| 27 | `CROSS APPLY` | Gap | N/A | N/A |
| 28 | `OUTER APPLY` | Gap | N/A | N/A |
| 29 | Self-join | Gap | N/A | N/A |
| 30 | Derived table in `FROM` | Gap | N/A | N/A |
| 31 | Scalar subquery in `SELECT` | Gap | N/A | N/A |
| 32 | `EXISTS` subquery | Gap | N/A | N/A |
| 33 | `NOT EXISTS` subquery | Gap | N/A | N/A |
| 34 | `IN` subquery | Gap | N/A | N/A |
| 35 | `NOT IN` subquery | Gap | N/A | N/A |
| 36 | Recursive CTE | Gap | N/A | N/A |
| 37 | `UPDATE` with CTE prefix | Gap | N/A | N/A |
| 38 | `DELETE` with CTE prefix | Gap | N/A | N/A |
| 39 | `MERGE` with CTE source | Gap | N/A | N/A |
| 40 | `GROUPING SETS` | Gap | N/A | N/A |
| 41 | `CUBE` | Gap | N/A | N/A |
| 42 | `ROLLUP` | Gap | N/A | N/A |
| 43 | `PIVOT` | Gap | N/A | N/A |
| 44 | `UNPIVOT` | Gap | N/A | N/A |
| 49 | `EXEC proc` | Yes | N/A | Yes |
| 50 | `EXEC [schema].[proc]` | Gap | N/A | N/A |
| 51 | `EXEC proc` with params | Yes | N/A | N/A |
| 52 | `EXEC proc` with `OUTPUT` | Gap | N/A | N/A |
| 53 | `EXECUTE proc` keyword form | Yes | N/A | N/A |
| 54 | `EXEC @rc = proc` | Gap | N/A | N/A |
| 57 | Static `sp_executesql` | Gap | N/A | N/A |
| 45 | `IF / ELSE` control flow | Yes | N/A | N/A |
| 46 | `TRY / CATCH` | Yes | N/A | N/A |
| 47 | `WHILE` loop | Yes | N/A | N/A |
| 48 | Nested control flow | Yes | N/A | N/A |
| 55 | Cross-database `EXEC` | Gap | N/A | Yes |
| 56 | Linked-server `EXEC` | Gap | N/A | N/A |
| 58 | Dynamic `sp_executesql` | Yes | N/A | Yes |
| 59 | `EXEC (@sql)` | Yes | N/A | N/A |
| 60 | `EXEC ('...' + @var)` | Gap | N/A | N/A |
| S1 | `SET` | Gap | N/A | N/A |
| S2 | `DECLARE` | Gap | N/A | N/A |
| S3 | `RETURN` | Gap | N/A | N/A |
| S4 | `PRINT` | Gap | N/A | N/A |
| S5 | `RAISERROR` | Gap | N/A | N/A |
| S6 | `THROW` | Gap | N/A | N/A |
| S7 | `BEGIN / COMMIT / ROLLBACK` | Gap | N/A | N/A |
| S8 | `DROP / CREATE INDEX` | Gap | N/A | N/A |
