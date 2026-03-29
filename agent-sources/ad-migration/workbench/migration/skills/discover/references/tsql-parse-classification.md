# T-SQL Statement Classification

When `discover show` returns `statements: null` (claude_assisted procs), read `raw_ddl` and classify each statement as `migrate` or `skip`.

## migrate — becomes the dbt model

| Statement type | Example | Notes |
|---|---|---|
| INSERT...SELECT | `INSERT INTO silver.T SELECT ... FROM bronze.S` | Target table = write target, source tables = reads |
| UPDATE | `UPDATE silver.T SET col = val FROM bronze.S` | Target table = write target |
| DELETE | `DELETE FROM silver.T WHERE ...` | Target table = write target |
| MERGE | `MERGE INTO silver.T USING bronze.S ON ...` | Target = write, USING source = read |
| SELECT INTO | `SELECT col INTO silver.T FROM bronze.S` | Creates new table — write target |
| CTE + DML | `WITH cte AS (...) INSERT INTO silver.T SELECT FROM cte` | The DML at the end is the migrate statement |
| EXEC (static) | `EXEC dbo.usp_Load` | Follow the called proc — run `discover show` on it to get its refs |
| EXEC (dynamic) | `EXEC (@sql)` / `EXEC sp_executesql @sql` | Read variable assignments to find the SQL string and classify the embedded DML |

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
