# Test Generation Coverage

Current automated statement coverage for the test-generator phase. The phase boundary here includes branch enumeration, fixture synthesis, and writing of test specs.

| # | Statement | Unit | Integration | Promptfoo |
|---|---|---|---|---|
| 1 | `INSERT ... SELECT` | N/A | N/A | Gap |
| 2 | `UPDATE` with join | N/A | N/A | Gap |
| 3 | `DELETE` with `WHERE` | N/A | N/A | Gap |
| 4 | `DELETE TOP` | N/A | N/A | Gap |
| 5 | `TRUNCATE TABLE` | N/A | N/A | Gap |
| 6 | `TRUNCATE` + `INSERT` | N/A | N/A | Gap |
| 7 | `MERGE INTO` | N/A | N/A | Yes |
| 8 | `SELECT INTO` | N/A | N/A | Gap |
| 9 | Single CTE | N/A | N/A | Gap |
| 10 | Multi-level CTE | N/A | N/A | Gap |
| 11 | Sequential `WITH` blocks | N/A | N/A | Gap |
| 12 | `CASE WHEN` | N/A | N/A | Gap |
| 13 | `LEFT OUTER JOIN` | N/A | N/A | Gap |
| 14 | `RIGHT OUTER JOIN` | N/A | N/A | Gap |
| 15 | Subquery in `WHERE` | N/A | N/A | Gap |
| 16 | Correlated subquery | N/A | N/A | Gap |
| 17 | Window functions | N/A | N/A | Gap |
| 19 | `UNION ALL` | N/A | N/A | Gap |
| 20 | `UNION` | N/A | N/A | Gap |
| 21 | `INTERSECT` | N/A | N/A | Gap |
| 22 | `EXCEPT` | N/A | N/A | Gap |
| 23 | `UNION ALL` in CTE branch | N/A | N/A | Gap |
| 24 | Explicit `INNER JOIN` | N/A | N/A | Gap |
| 25 | `FULL OUTER JOIN` | N/A | N/A | Gap |
| 26 | `CROSS JOIN` | N/A | N/A | Gap |
| 27 | `CROSS APPLY` | N/A | N/A | Gap |
| 28 | `OUTER APPLY` | N/A | N/A | Gap |
| 29 | Self-join | N/A | N/A | Gap |
| 30 | Derived table in `FROM` | N/A | N/A | Gap |
| 31 | Scalar subquery in `SELECT` | N/A | N/A | Gap |
| 32 | `EXISTS` subquery | N/A | N/A | Gap |
| 33 | `NOT EXISTS` subquery | N/A | N/A | Gap |
| 34 | `IN` subquery | N/A | N/A | Gap |
| 35 | `NOT IN` subquery | N/A | N/A | Gap |
| 36 | Recursive CTE | N/A | N/A | Gap |
| 37 | `UPDATE` with CTE prefix | N/A | N/A | Gap |
| 38 | `DELETE` with CTE prefix | N/A | N/A | Gap |
| 39 | `MERGE` with CTE source | N/A | N/A | Gap |
| 40 | `GROUPING SETS` | N/A | N/A | Gap |
| 41 | `CUBE` | N/A | N/A | Gap |
| 42 | `ROLLUP` | N/A | N/A | Gap |
| 43 | `PIVOT` | N/A | N/A | Gap |
| 44 | `UNPIVOT` | N/A | N/A | Gap |
| 49 | `EXEC proc` | N/A | N/A | Yes |
| 50 | `EXEC [schema].[proc]` | N/A | N/A | Gap |
| 51 | `EXEC proc` with params | N/A | N/A | Gap |
| 52 | `EXEC proc` with `OUTPUT` | N/A | N/A | Gap |
| 53 | `EXECUTE proc` keyword form | N/A | N/A | Gap |
| 54 | `EXEC @rc = proc` | N/A | N/A | Gap |
| 57 | Static `sp_executesql` | N/A | N/A | Gap |
| 45 | `IF / ELSE` control flow | N/A | N/A | Gap |
| 46 | `TRY / CATCH` | N/A | N/A | Gap |
| 47 | `WHILE` loop | N/A | N/A | Gap |
| 48 | Nested control flow | N/A | N/A | Gap |
| 55 | Cross-database `EXEC` | N/A | N/A | Gap |
| 56 | Linked-server `EXEC` | N/A | N/A | Gap |
| 58 | Dynamic `sp_executesql` | N/A | N/A | Yes |
| 59 | `EXEC (@sql)` | N/A | N/A | Gap |
| 60 | `EXEC ('...' + @var)` | N/A | N/A | Gap |
| S1 | `SET` | N/A | N/A | N/A |
| S2 | `DECLARE` | N/A | N/A | N/A |
| S3 | `RETURN` | N/A | N/A | N/A |
| S4 | `PRINT` | N/A | N/A | N/A |
| S5 | `RAISERROR` | N/A | N/A | N/A |
| S6 | `THROW` | N/A | N/A | N/A |
| S7 | `BEGIN / COMMIT / ROLLBACK` | N/A | N/A | N/A |
| S8 | `DROP / CREATE INDEX` | N/A | N/A | N/A |
