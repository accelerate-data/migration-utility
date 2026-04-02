# Profiling Coverage

Current automated statement coverage for the profiling phase. The phase boundary here includes profile context assembly and Promptfoo evaluation of the profiling skill.

| # | Statement | Unit | Integration | Promptfoo |
|---|---|---|---|---|
| 1 | `INSERT ... SELECT` | Yes | N/A | N/A |
| 2 | `UPDATE` with join | N/A | N/A | N/A |
| 3 | `DELETE` with `WHERE` | N/A | N/A | N/A |
| 4 | `DELETE TOP` | N/A | N/A | N/A |
| 5 | `TRUNCATE TABLE` | N/A | N/A | N/A |
| 6 | `TRUNCATE` + `INSERT` | N/A | N/A | Yes |
| 7 | `MERGE INTO` | Yes | N/A | Gap |
| 8 | `SELECT INTO` | N/A | N/A | N/A |
| 9 | Single CTE | N/A | N/A | N/A |
| 10 | Multi-level CTE | N/A | N/A | N/A |
| 11 | Sequential `WITH` blocks | N/A | N/A | N/A |
| 12 | `CASE WHEN` | N/A | N/A | N/A |
| 13 | `LEFT OUTER JOIN` | N/A | N/A | N/A |
| 14 | `RIGHT OUTER JOIN` | N/A | N/A | N/A |
| 15 | Subquery in `WHERE` | N/A | N/A | N/A |
| 16 | Correlated subquery | N/A | N/A | N/A |
| 17 | Window functions | N/A | N/A | N/A |
| 19 | `UNION ALL` | N/A | N/A | N/A |
| 20 | `UNION` | N/A | N/A | N/A |
| 21 | `INTERSECT` | N/A | N/A | N/A |
| 22 | `EXCEPT` | N/A | N/A | N/A |
| 23 | `UNION ALL` in CTE branch | N/A | N/A | N/A |
| 24 | Explicit `INNER JOIN` | N/A | N/A | N/A |
| 25 | `FULL OUTER JOIN` | N/A | N/A | N/A |
| 26 | `CROSS JOIN` | N/A | N/A | N/A |
| 27 | `CROSS APPLY` | N/A | N/A | N/A |
| 28 | `OUTER APPLY` | N/A | N/A | N/A |
| 29 | Self-join | N/A | N/A | N/A |
| 30 | Derived table in `FROM` | N/A | N/A | N/A |
| 31 | Scalar subquery in `SELECT` | N/A | N/A | N/A |
| 32 | `EXISTS` subquery | N/A | N/A | N/A |
| 33 | `NOT EXISTS` subquery | N/A | N/A | N/A |
| 34 | `IN` subquery | N/A | N/A | N/A |
| 35 | `NOT IN` subquery | N/A | N/A | N/A |
| 36 | Recursive CTE | N/A | N/A | N/A |
| 37 | `UPDATE` with CTE prefix | N/A | N/A | N/A |
| 38 | `DELETE` with CTE prefix | N/A | N/A | N/A |
| 39 | `MERGE` with CTE source | N/A | N/A | N/A |
| 40 | `GROUPING SETS` | N/A | N/A | N/A |
| 41 | `CUBE` | N/A | N/A | N/A |
| 42 | `ROLLUP` | N/A | N/A | N/A |
| 43 | `PIVOT` | N/A | N/A | N/A |
| 44 | `UNPIVOT` | N/A | N/A | N/A |
| 49 | `EXEC proc` | Yes | N/A | Gap |
| 50 | `EXEC [schema].[proc]` | N/A | N/A | N/A |
| 51 | `EXEC proc` with params | N/A | N/A | N/A |
| 52 | `EXEC proc` with `OUTPUT` | N/A | N/A | N/A |
| 53 | `EXECUTE proc` keyword form | N/A | N/A | N/A |
| 54 | `EXEC @rc = proc` | N/A | N/A | N/A |
| 57 | Static `sp_executesql` | N/A | N/A | N/A |
| 45 | `IF / ELSE` control flow | N/A | N/A | N/A |
| 46 | `TRY / CATCH` | N/A | N/A | N/A |
| 47 | `WHILE` loop | N/A | N/A | N/A |
| 48 | Nested control flow | N/A | N/A | N/A |
| 55 | Cross-database `EXEC` | N/A | N/A | Gap |
| 56 | Linked-server `EXEC` | N/A | N/A | N/A |
| 58 | Dynamic `sp_executesql` | Gap | N/A | Gap |
| 59 | `EXEC (@sql)` | N/A | N/A | N/A |
| 60 | `EXEC ('...' + @var)` | N/A | N/A | N/A |
| S1 | `SET` | N/A | N/A | N/A |
| S2 | `DECLARE` | N/A | N/A | N/A |
| S3 | `RETURN` | N/A | N/A | N/A |
| S4 | `PRINT` | N/A | N/A | N/A |
| S5 | `RAISERROR` | N/A | N/A | N/A |
| S6 | `THROW` | N/A | N/A | N/A |
| S7 | `BEGIN / COMMIT / ROLLBACK` | N/A | N/A | N/A |
| S8 | `DROP / CREATE INDEX` | N/A | N/A | N/A |
