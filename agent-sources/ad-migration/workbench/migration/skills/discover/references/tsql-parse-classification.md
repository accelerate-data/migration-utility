# T-SQL Pattern Reference

Exhaustive classification of T-SQL patterns in stored procedure migration. Each pattern is routed to either the deterministic sqlglot path or the claude-assisted path based on the `needs_llm` flag in the proc catalog file.

## Deterministic — sqlglot (patterns 1-22)

Fully parsed by sqlglot. `extract_refs` populates `writes_to` and `reads_from`. `statements` array is available with `migrate`/`skip`/`claude` action classification.

| # | Pattern | Example | Test fixture |
|---|---|---|---|
| 1 | INSERT...SELECT | `INSERT INTO silver.T SELECT ... FROM bronze.S` | `usp_LoadDimProduct` |
| 2 | UPDATE with JOIN | `UPDATE silver.T SET col = val FROM silver.T JOIN bronze.S ON ...` | `usp_SimpleUpdate` |
| 3 | DELETE with WHERE | `DELETE FROM silver.T WHERE ...` | `usp_SimpleDelete` |
| 4 | DELETE TOP | `DELETE TOP (1000) FROM silver.T WHERE ...` | `usp_DeleteTop` |
| 5 | TRUNCATE TABLE | `TRUNCATE TABLE silver.T` | `usp_TruncateOnly` |
| 6 | TRUNCATE + INSERT | `TRUNCATE TABLE silver.T; INSERT INTO silver.T SELECT ...` | `usp_LoadDimProduct` |
| 7 | MERGE INTO | `MERGE INTO silver.T USING bronze.S ON ... WHEN MATCHED/NOT MATCHED ...` | `usp_MergeDimProduct` |
| 8 | SELECT INTO | `SELECT col INTO silver.T FROM bronze.S` | `usp_SelectInto` |
| 9 | CTE | `WITH cte AS (...) INSERT INTO silver.T SELECT ... FROM cte` | `usp_LoadWithCTE` |
| 10 | Multi-level CTE | `WITH a AS (...), b AS (SELECT FROM a ...) INSERT ...` | `usp_LoadWithMultiCTE` |
| 11 | Sequential WITH blocks | First WITH populates table, second WITH reads it | `usp_SequentialWith` |
| 12 | CASE WHEN | `SELECT CASE WHEN ... END FROM ...` | `usp_LoadWithCase` |
| 13 | LEFT OUTER JOIN | `SELECT ... FROM a LEFT OUTER JOIN b ON ...` | `usp_LoadWithLeftJoin` |
| 14 | RIGHT OUTER JOIN | `SELECT ... FROM a RIGHT OUTER JOIN b ON ...` | `usp_RightOuterJoin` |
| 15 | Subquery in WHERE | `WHERE col > (SELECT AVG(col) FROM ...)` | `usp_SubqueryInWhere` |
| 16 | Correlated subquery | `WHERE col = (SELECT MAX(...) WHERE outer.id = inner.id)` | `usp_CorrelatedSubquery` |
| 17 | Window functions | `COUNT(*) OVER (PARTITION BY ...)`, `ROW_NUMBER() OVER (...)` | `usp_WindowFunction` |
| 18 | DROP/CREATE Index | `DROP INDEX ...; CREATE INDEX ...` around DML | `usp_FullReload` (inline test) |
| 19 | IF/ELSE BEGIN END | Control flow wrapping DML — recovered via body parsing | `usp_ConditionalMerge` |
| 20 | BEGIN TRY/CATCH | Error handling wrapping DML — recovered via body parsing | `usp_TryCatchLoad` |
| 21 | WHILE BEGIN END | Loop wrapping DML — recovered via body parsing | `usp_WhileLoop` |
| 22 | Nested control flow | IF inside WHILE inside TRY/CATCH | `usp_NestedControlFlow` |

## Claude-assisted — EXEC/dynamic SQL (patterns 23-34)

These produce `Command` nodes that cannot be resolved statically. The proc's `raw_ddl` is returned by `discover show` for Claude to analyse. The proc catalog file has `needs_llm: true`.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 23 | EXEC proc | `EXEC dbo.usp_Load` | Call target is another proc | `usp_ExecSimple` |
| 24 | EXEC bracketed | `EXEC [silver].[usp_Load]` | Bracket notation | `usp_ExecBracketed` |
| 25 | EXEC with params | `EXEC dbo.usp_Load @Mode = 1` | Call target with parameters | `usp_ExecWithParams` |
| 26 | EXEC with OUTPUT | `EXEC dbo.usp_Load @Result OUTPUT` | Output param | (covered by `usp_ExecWithParams`) |
| 27 | EXECUTE keyword | `EXECUTE dbo.usp_Load` | Same as EXEC | (covered by `usp_ExecSimple`) |
| 28 | EXEC with return value | `EXEC @rc = dbo.usp_Load` | Return variable prefix | `usp_ExecWithReturn` |
| 29 | EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | 3-part name, out of scope | (flagged as cross-DB) |
| 30 | EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | 4-part name, out of scope | (flagged as cross-DB) |
| 31 | sp_executesql static | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | SQL in string literal | `usp_ExecSpExecutesql` |
| 32 | sp_executesql dynamic | `EXEC sp_executesql @sql` | Variable SQL | `usp_ExecSpExecutesql` |
| 33 | EXEC variable SQL | `EXEC (@sql)` | Runtime string | `usp_ExecDynamic` |
| 34 | EXEC concat | `EXEC ('INSERT INTO ' + @table)` | Runtime string building | (covered by `usp_ExecDynamic`) |

## Known Limitations

- **EXEC call graph**: sqlglot produces `Command` nodes for all EXEC variants. Indirect writer detection via call graph traversal is handled by `catalog-enrich` for static EXEC chains, not by sqlglot.
- **Dynamic SQL**: `EXEC (@sql)` and `sp_executesql @var` cannot be resolved statically. Only Claude can reason about runtime SQL construction.
- **IF condition tables**: Tables referenced only in `IF EXISTS (SELECT ... FROM table)` conditions are not captured in `reads_from`. These are condition checks, not data reads for the model.
