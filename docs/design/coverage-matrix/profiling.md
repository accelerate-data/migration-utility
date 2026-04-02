# Profiling Coverage

Current automated statement coverage for the profiling phase. The phase boundary here includes profile context assembly and Promptfoo evaluation of the profiling skill.

| # | Statement | Unit | Integration | Promptfoo |
|---|---|---|---|---|
| 1 | `INSERT ... SELECT` | Yes |  |  |
| 2 | `UPDATE` with join |  |  |  |
| 3 | `DELETE` with `WHERE` |  |  |  |
| 4 | `DELETE TOP` |  |  |  |
| 5 | `TRUNCATE TABLE` |  |  |  |
| 6 | `TRUNCATE` + `INSERT` |  |  | Yes |
| 7 | `MERGE INTO` | Yes |  |  |
| 8 | `SELECT INTO` |  |  |  |
| 9 | Single CTE |  |  |  |
| 10 | Multi-level CTE |  |  |  |
| 11 | Sequential `WITH` blocks |  |  |  |
| 12 | `CASE WHEN` |  |  |  |
| 13 | `LEFT OUTER JOIN` |  |  |  |
| 14 | `RIGHT OUTER JOIN` |  |  |  |
| 15 | Subquery in `WHERE` |  |  |  |
| 16 | Correlated subquery |  |  |  |
| 17 | Window functions |  |  |  |
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
| 49 | `EXEC proc` |  |  |  |
| 50 | `EXEC [schema].[proc]` |  |  |  |
| 51 | `EXEC proc` with params |  |  |  |
| 52 | `EXEC proc` with `OUTPUT` |  |  |  |
| 53 | `EXECUTE proc` keyword form |  |  |  |
| 54 | `EXEC @rc = proc` |  |  |  |
| 57 | Static `sp_executesql` |  |  |  |
| 45 | `IF / ELSE` control flow |  |  |  |
| 46 | `TRY / CATCH` |  |  |  |
| 47 | `WHILE` loop |  |  |  |
| 48 | Nested control flow |  |  |  |
| 55 | Cross-database `EXEC` |  |  |  |
| 56 | Linked-server `EXEC` |  |  |  |
| 58 | Dynamic `sp_executesql` |  |  |  |
| 59 | `EXEC (@sql)` |  |  |  |
| 60 | `EXEC ('...' + @var)` |  |  |  |
| S1 | `SET` |  |  |  |
| S2 | `DECLARE` |  |  |  |
| S3 | `RETURN` |  |  |  |
| S4 | `PRINT` |  |  |  |
| S5 | `RAISERROR` |  |  |  |
| S6 | `THROW` |  |  |  |
| S7 | `BEGIN / COMMIT / ROLLBACK` |  |  |  |
| S8 | `DROP / CREATE INDEX` |  |  |  |
