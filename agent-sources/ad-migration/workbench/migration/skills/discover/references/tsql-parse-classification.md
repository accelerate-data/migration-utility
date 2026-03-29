# T-SQL Pattern Reference

Classification of T-SQL patterns encountered in stored procedure migration. Each pattern is routed based on the `needs_llm` and `needs_enrich` flags in the proc catalog file (set during `setup-ddl` body scan).

## Deterministic — no flags set (patterns 1-18)

Refs fully resolved from catalog + DMF. `statements` array available from AST parsing.

| # | Pattern | Example | Test fixture |
|---|---|---|---|
| 1 | INSERT...SELECT | `INSERT INTO silver.T SELECT ... FROM bronze.S` | `usp_LoadDimProduct` |
| 2 | UPDATE with JOIN | `UPDATE silver.T SET col = val FROM silver.T JOIN bronze.S ON ...` | `usp_SimpleUpdate` |
| 3 | DELETE with WHERE | `DELETE FROM silver.T WHERE ...` | `usp_SimpleDelete` |
| 4 | DELETE TOP | `DELETE TOP (1000) FROM silver.T WHERE ...` | `usp_DeleteTop` |
| 5 | TRUNCATE TABLE | `TRUNCATE TABLE silver.T` | `usp_TruncateOnly` |
| 6 | TRUNCATE + INSERT | `TRUNCATE TABLE silver.T; INSERT INTO silver.T SELECT ...` | `usp_LoadDimProduct` |
| 7 | MERGE INTO | `MERGE INTO silver.T USING bronze.S ON ...` | `usp_MergeDimProduct` |
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

## Needs enrichment — `needs_enrich: true` (patterns 19-22)

DMF has refs but `catalog-enrich` resolves additional write targets (SELECT INTO, TRUNCATE, static EXEC chains). After enrichment, refs are complete and `needs_enrich` is flipped to `false`.

| # | Pattern | Example | What enrichment adds | Test fixture |
|---|---|---|---|---|
| 19 | SELECT INTO | `SELECT col INTO silver.T FROM bronze.S` | Target table not in DMF | `usp_SelectInto` |
| 20 | TRUNCATE TABLE | `TRUNCATE TABLE silver.T` | TRUNCATE target not in DMF | `usp_TruncateOnly` |
| 21 | Static EXEC chain | `EXEC dbo.usp_Load` | Transitive write targets via BFS | `usp_ExecSimple` |
| 22 | Nested EXEC chain | `EXEC dbo.usp_Step1` → `EXEC dbo.usp_Step2` → writes | Multi-hop BFS | `usp_ExecBracketed` |

## Claude-assisted — `needs_llm: true` (patterns 23-34)

Cannot be resolved offline. The proc's `raw_ddl` is returned by `discover show` for the LLM to analyse. Catalog refs may be partial.

| # | Pattern | Example | Why LLM needed | Test fixture |
|---|---|---|---|---|
| 23 | IF/ELSE with DML | `IF EXISTS (...) INSERT ... ELSE UPDATE ...` | Branches may have different write targets | `usp_ConditionalMerge` |
| 24 | BEGIN TRY/CATCH | `BEGIN TRY INSERT ... END TRY BEGIN CATCH ... END CATCH` | sqlglot cannot parse TRY/CATCH blocks | `usp_TryCatchLoad` |
| 25 | WHILE loop | `WHILE @i < 3 BEGIN INSERT ... END` | sqlglot cannot parse WHILE blocks | `usp_WhileLoop` |
| 26 | Nested control flow | IF inside WHILE inside TRY/CATCH | Combination of above | `usp_NestedControlFlow` |
| 27 | EXEC(@sql) | `EXEC (@sql)` | Runtime string — unresolvable offline | `usp_ExecDynamic` |
| 28 | EXEC concat | `EXEC ('INSERT INTO ' + @table)` | Runtime string building | (covered by `usp_ExecDynamic`) |
| 29 | sp_executesql dynamic | `EXEC sp_executesql @sql` | Variable SQL | `usp_ExecSpExecutesql` |
| 30 | sp_executesql static | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | SQL in string literal | `usp_ExecSpExecutesql` |
| 31 | EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | 3-part name, out of scope | (flagged as cross-DB) |
| 32 | EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | 4-part name, out of scope | (flagged as cross-DB) |
| 33 | EXEC with return value | `EXEC @rc = dbo.usp_Load` | Return variable prefix | `usp_ExecWithReturn` |
| 34 | EXEC with params | `EXEC dbo.usp_Load @Mode = 1` | Call target with parameters | `usp_ExecWithParams` |

## Known Limitations

- **Dynamic SQL**: `EXEC (@sql)` and `sp_executesql @var` cannot be resolved offline. Only the LLM can reason about runtime SQL construction.
- **IF condition tables**: Tables referenced only in `IF EXISTS (SELECT ... FROM table)` conditions are not captured in reads. These are condition checks, not data reads for the model.
- **Static EXEC after enrichment**: `catalog-enrich` resolves transitive write targets for static EXEC chains. If enrichment hasn't run, these procs will have incomplete refs.
