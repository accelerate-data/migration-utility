# T-SQL Statement Classification

When `discover show` returns `statements: null` (claude_assisted procs), read `raw_ddl` and classify each statement as `migrate` or `skip`.

## migrate — becomes the dbt model

| Statement type | Example | Notes |
|---|---|---|
| INSERT...SELECT | `INSERT INTO silver.T SELECT ... FROM bronze.S` | Target table = write target, source tables = reads. The SELECT may contain UNION ALL, UNION, INTERSECT, EXCEPT, PIVOT, UNPIVOT, GROUPING SETS, CUBE, ROLLUP, CROSS APPLY, OUTER APPLY, or any join/subquery variant — classify the outer INSERT as migrate regardless of SELECT complexity. Often preceded by `TRUNCATE TABLE` (which is skip) as a truncate-and-reload pattern. |
| UPDATE | `UPDATE silver.T SET col = val FROM silver.T JOIN bronze.S ON ...` | Target table = write target |
| DELETE | `DELETE FROM silver.T WHERE ...` | Target table = write target. Includes `DELETE TOP (N)` variant. |
| MERGE | `MERGE INTO silver.T USING bronze.S ON ...` | Target = write, USING source = read |
| SELECT INTO | `SELECT col INTO silver.T FROM bronze.S` | Creates new table — write target |
| CTE + INSERT | `WITH cte AS (...) INSERT INTO silver.T SELECT FROM cte` | The DML at the end is the migrate statement |
| CTE + UPDATE | `WITH cte AS (...) UPDATE t SET col = cte.val FROM silver.T t JOIN cte ON ...` | CTE defines the source; UPDATE is the migrate statement |
| CTE + DELETE | `WITH cte AS (...) DELETE FROM silver.T WHERE id IN (SELECT id FROM cte)` | CTE defines the filter; DELETE is the migrate statement |
| CTE + MERGE | `WITH src AS (...) MERGE INTO silver.T USING src ON ...` | CTE defines the USING source; MERGE is the migrate statement |
| EXEC / EXECUTE (static) | `EXEC dbo.usp_Load` or `EXECUTE dbo.usp_Load` | Follow the called proc — run `discover show` on it to get its refs. `EXECUTE` is the unabbreviated form of `EXEC` — classify identically. |
| EXEC (bracketed) | `EXEC [silver].[usp_Load]` | Same as static — bracket notation is just quoting |
| EXEC with params | `EXEC dbo.usp_Load @Mode = 1` | Follow the called proc; parameters don't change classification |
| EXEC with OUTPUT | `EXEC dbo.usp_Load @Result OUTPUT` | Follow the called proc; OUTPUT param doesn't change classification |
| EXEC with return | `EXEC @rc = dbo.usp_Load` | Follow the called proc; return variable prefix doesn't change classification |
| EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | **Flag as error** — 3-part name is out of scope for this migration |
| EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | **Flag as error** — 4-part name (linked server) is out of scope |
| sp_executesql (literal) | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | SQL is visible in the string literal — classify the embedded DML directly |
| sp_executesql (variable) | `EXEC sp_executesql @sql` | Read variable assignments to find the SQL string and classify the embedded DML |
| EXEC (dynamic) | `EXEC (@sql)` / `EXEC ('INSERT INTO ' + @table)` | Read variable assignments to find the SQL string and classify the embedded DML |

## skip — operational overhead, dbt handles or ignores

| Statement type | Example | Notes |
|---|---|---|
| SET | `SET NOCOUNT ON`, `SET XACT_ABORT ON` | Session config |
| TRUNCATE | `TRUNCATE TABLE silver.T` | Load pattern (truncate + insert) — dbt incremental handles this |
| DROP/CREATE INDEX | `DROP INDEX ix_1 ON silver.T` | Index management — dbt post-hooks or manual |
| DECLARE | `DECLARE @i INT = 0` | Variable declaration |
| PRINT | `PRINT 'Loading...'` | Logging |
| RAISERROR / THROW | `RAISERROR('Error', 16, 1)` | Error handling |
| BEGIN/COMMIT/ROLLBACK | `BEGIN TRAN ... COMMIT` | Transaction control — dbt manages transactions |
| IF EXISTS (check only) | `IF EXISTS (SELECT 1 FROM dbo.T)` | Condition check, not a data read for the model |
| RETURN | `RETURN 0` | Early exit — no data operation |

## Reading control flow

When the proc has IF/ELSE, TRY/CATCH, or WHILE:

1. **Trace all branches** — DML may appear in both the IF and ELSE paths, or inside TRY only
2. **Classify each DML statement** in every branch using the tables above
3. **Union the write targets** across all branches — the proc may write to different tables depending on the path
4. **WHILE loops** — the DML inside is the same as outside a loop, just repeated. Classify normally.

## Reading dynamic SQL

When the proc builds SQL in a variable and executes it:

1. Find the `DECLARE @sql` and trace all assignments (`SET @sql = ...`, `SET @sql = @sql + ...`)
2. Reconstruct the SQL string from the concatenation
3. Classify the reconstructed SQL using the migrate/skip tables above
4. If the target table is in a variable (`@table`), note it as unresolvable — report what you can determine
