# T-SQL Pattern Reference

Classification of T-SQL patterns in stored procedures. `discover show` returns a `classification` field — `deterministic` or `claude_assisted` — that tells you which patterns are pre-resolved and which need LLM analysis of `raw_ddl`.

## Deterministic (patterns 1-22)

`discover show` returns complete `refs` and `statements`. Use them directly.

| # | Pattern | Example |
|---|---|---|
| 1 | INSERT...SELECT | `INSERT INTO silver.T SELECT ... FROM bronze.S` |
| 2 | UPDATE with JOIN | `UPDATE silver.T SET col = val FROM silver.T JOIN bronze.S ON ...` |
| 3 | DELETE with WHERE | `DELETE FROM silver.T WHERE ...` |
| 4 | DELETE TOP | `DELETE TOP (1000) FROM silver.T WHERE ...` |
| 5 | TRUNCATE TABLE | `TRUNCATE TABLE silver.T` |
| 6 | TRUNCATE + INSERT | `TRUNCATE TABLE silver.T; INSERT INTO silver.T SELECT ...` |
| 7 | MERGE INTO | `MERGE INTO silver.T USING bronze.S ON ...` |
| 8 | SELECT INTO | `SELECT col INTO silver.T FROM bronze.S` |
| 9 | CTE | `WITH cte AS (...) INSERT INTO silver.T SELECT ... FROM cte` |
| 10 | Multi-level CTE | `WITH a AS (...), b AS (SELECT FROM a ...) INSERT ...` |
| 11 | Sequential WITH blocks | First WITH populates table, second WITH reads it |
| 12 | CASE WHEN | `SELECT CASE WHEN ... END FROM ...` |
| 13 | LEFT OUTER JOIN | `SELECT ... FROM a LEFT OUTER JOIN b ON ...` |
| 14 | RIGHT OUTER JOIN | `SELECT ... FROM a RIGHT OUTER JOIN b ON ...` |
| 15 | Subquery in WHERE | `WHERE col > (SELECT AVG(col) FROM ...)` |
| 16 | Correlated subquery | `WHERE col = (SELECT MAX(...) WHERE outer.id = inner.id)` |
| 17 | Window functions | `ROW_NUMBER() OVER (...)`, `COUNT(*) OVER (PARTITION BY ...)` |
| 18 | DROP/CREATE Index | `DROP INDEX ...; CREATE INDEX ...` around DML |
| 19 | Static EXEC chain | `EXEC dbo.usp_Load` (resolved, refs include transitive targets) |
| 20 | Static EXEC bracketed | `EXEC [silver].[usp_Load]` |
| 21 | Static EXEC with params | `EXEC dbo.usp_Load @Mode = 1` |
| 22 | Static EXEC with return | `EXEC @rc = dbo.usp_Load` |

## Claude-assisted (patterns 23-34)

`discover show` returns `classification: "claude_assisted"` and `statements: null`. Read `raw_ddl` to identify writes, reads, and calls.

| # | Pattern | Example | What to look for in raw_ddl |
|---|---|---|---|
| 23 | IF/ELSE with DML | `IF EXISTS (...) INSERT ... ELSE UPDATE ...` | DML in both branches — identify all write targets |
| 24 | BEGIN TRY/CATCH | `BEGIN TRY INSERT ... END TRY BEGIN CATCH ...` | DML inside TRY block |
| 25 | WHILE loop | `WHILE @i < 3 BEGIN INSERT ... END` | DML inside loop body |
| 26 | Nested control flow | IF inside WHILE inside TRY/CATCH | Combination — trace all branches |
| 27 | EXEC(@sql) | `EXEC (@sql)` | Read the variable assignment to find the SQL string |
| 28 | EXEC concat | `EXEC ('INSERT INTO ' + @table)` | Trace string building to find target |
| 29 | sp_executesql dynamic | `EXEC sp_executesql @sql` | Read the variable assignment |
| 30 | sp_executesql static | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | Parse the SQL string literal |
| 31 | EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | Out of scope — note as external dependency |
| 32 | EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | Out of scope — note as external dependency |
| 33 | EXEC with OUTPUT | `EXEC dbo.usp_Load @Result OUTPUT` | Follow the called proc |
| 34 | EXECUTE keyword | `EXECUTE dbo.usp_Load` | Same as EXEC |

## Known Limitations

- **Dynamic SQL**: `EXEC (@sql)` and `sp_executesql @var` — the SQL is constructed at runtime. Read the variable assignments in `raw_ddl` to determine what tables are targeted.
- **IF condition tables**: Tables in `IF EXISTS (SELECT ... FROM table)` are condition checks, not data sources for the model.
