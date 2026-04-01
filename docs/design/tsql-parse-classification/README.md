# T-SQL Parse Classification

Exhaustive classification of T-SQL patterns encountered in stored procedure migration. Each pattern is routed to either the deterministic sqlglot path or the Claude-assisted path.

## Architecture

```text
Stored Procedure DDL
        │
        ▼
  parse_body_statements()
        │
        ▼
  Single sqlglot.parse() call
        │
        ├── All statements are real AST nodes → classification: "deterministic"
        │     extract_refs() populates writes_to / reads_from
        │
        └── Any Command or If nodes found → classification: "claude_assisted"
              needs_llm = True; raw_ddl preserved for Claude to analyse
```

## Decision Rule

After body parsing, check the statement list:

- **No `Command` or `If` nodes** → deterministic. `extract_refs` handles everything.
- **Any `exp.Command` or `exp.If` nodes** → Claude-assisted. The proc contains dynamic SQL, procedure calls, or control flow that sqlglot cannot fully parse. `raw_ddl` is passed to Claude for analysis.

The `discover show` command signals this via the `classification` field:

- `classification: "deterministic"` + populated `statements` → fully parsed
- `classification: "claude_assisted"` → needs LLM reasoning from raw DDL

Note: `parse_error` is a separate orthogonal field that records whether the `CREATE PROCEDURE` block itself failed to parse at DDL load time.

## Pattern Classification

### Deterministic — sqlglot (patterns 1-45)

These patterns are fully parsed by sqlglot. `extract_refs` populates `writes_to` and `reads_from` without any LLM involvement.

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
| 18 | DROP/CREATE INDEX | `DROP INDEX ...; CREATE INDEX ...` around DML | inline test |
| 19 | UNION ALL | `INSERT INTO ... SELECT ... FROM ... UNION ALL SELECT ... FROM` | `usp_UnionAll` |
| 20 | UNION | `INSERT INTO ... SELECT ... FROM ... UNION SELECT ... FROM` | `usp_Union` |
| 21 | INTERSECT | `INSERT INTO ... SELECT ... FROM ... INTERSECT SELECT ... FROM ...` | `usp_Intersect` |
| 22 | EXCEPT | `INSERT INTO ... SELECT ... FROM ... EXCEPT SELECT ... FROM ...` | `usp_Except` |
| 23 | UNION ALL in CTE branch | `WITH cte AS (SELECT ... UNION ALL SELECT ...) INSERT INTO ...` | `usp_UnionAllInCTE` |
| 24 | INNER JOIN (explicit) | `SELECT ... FROM ... INNER JOIN ... ON ...` | `usp_InnerJoin` |
| 25 | FULL OUTER JOIN | `SELECT ... FROM ... FULL OUTER JOIN ... ON ...` | `usp_FullOuterJoin` |
| 26 | CROSS JOIN | `SELECT ... FROM ... CROSS JOIN ...` | `usp_CrossJoin` |
| 27 | CROSS APPLY | `SELECT ... FROM ... CROSS APPLY ...` | `usp_CrossApply` |
| 28 | OUTER APPLY | `SELECT ... FROM a OUTER APPLY (SELECT TOP ... FROM b WHERE b.id = a.id)` | `usp_OuterApply` |
| 29 | Self-join | `SELECT ... FROM ... JOIN ... ON ... = ...` | `usp_SelfJoin` |
| 30 | Derived table in FROM | `SELECT ... FROM (SELECT ... FROM ...) sub JOIN ... ON ...` | `usp_DerivedTable` |
| 31 | Scalar subquery in SELECT | `SELECT (SELECT MAX(col) FROM ... WHERE ...) AS val FROM ...` | `usp_ScalarSubquery` |
| 32 | EXISTS subquery | `WHERE EXISTS (SELECT 1 FROM ... WHERE ...)` | `usp_ExistsSubquery` |
| 33 | NOT EXISTS subquery | `WHERE NOT EXISTS (SELECT 1 FROM bronze.S WHERE ...)` | `usp_NotExistsSubquery` |
| 34 | IN subquery | `WHERE id IN (SELECT id FROM ... WHERE ...)` | `usp_InSubquery` |
| 35 | NOT IN subquery | `WHERE ... NOT IN (SELECT ... FROM ... WHERE ...)` | `usp_NotInSubquery` |
| 36 | Recursive CTE | `WITH cte AS (SELECT ... UNION ALL SELECT ... FROM cte WHERE ...)` | `usp_RecursiveCTE` |
| 37 | UPDATE with CTE prefix | `WITH cte AS (...) UPDATE ... SET ... FROM cte WHERE ...` | `usp_UpdateWithCTE` |
| 38 | DELETE with CTE prefix | `WITH cte AS (...) DELETE FROM ... WHERE ... IN (SELECT ... FROM cte)` | `usp_DeleteWithCTE` |
| 39 | MERGE with CTE source | `WITH src AS (...) MERGE INTO ... USING ... ON ...` | `usp_MergeWithCTE` |
| 40 | GROUPING SETS | `SELECT ... GROUP BY GROUPING SETS ((a, b), (a), ())` | `usp_GroupingSets` |
| 41 | CUBE | `SELECT ... GROUP BY CUBE (a, b, c)` | `usp_Cube` |
| 42 | ROLLUP | `SELECT ... GROUP BY ROLLUP (year, month, day)` | `usp_Rollup` |
| 43 | PIVOT | `SELECT ... FROM ... PIVOT (SUM(val) FOR col IN (...)) pvt` | `usp_Pivot` |
| 44 | UNPIVOT | `SELECT ... FROM ... UNPIVOT (val FOR col IN (...)) unpvt` | `usp_Unpivot` |

### Claude-assisted — control flow and EXEC (patterns 45-60)

These patterns produce `Command` or `If` nodes that cannot be fully resolved statically. The proc's `raw_ddl` is passed to Claude for analysis.

**Control flow patterns (45-48):** sqlglot parses `IF`, `TRY/CATCH`, and `WHILE` blocks into `Command` or `If` AST nodes. DML nested inside these blocks is not reliably extracted. The proc is flagged `needs_llm = True` and Claude reads the raw DDL.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 45 | IF/ELSE BEGIN END | Control flow wrapping DML | `exp.If` node — DML inside branches not reliably extracted | `usp_ConditionalMerge` |
| 46 | BEGIN TRY/CATCH | Error handling wrapping DML | `exp.Command` node — DML inside not extracted | `usp_TryCatchLoad` |
| 47 | WHILE BEGIN END | Loop wrapping DML | `exp.Command` node — DML inside not extracted | `usp_WhileLoop` |
| 48 | Nested control flow | IF inside WHILE inside TRY/CATCH | Multiple Command/If nodes — DML inside not extracted | `usp_NestedControlFlow` |

**EXEC/dynamic SQL patterns (49-60):** These produce `Command` nodes for procedure calls or dynamic SQL that cannot be resolved statically.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 49 | EXEC proc | `EXEC dbo.usp_Load` | Call target is another proc — need to follow the graph | `usp_ExecSimple` |
| 50 | EXEC bracketed | `EXEC [silver].[usp_Load]` | Same — bracket notation | `usp_ExecBracketed` |
| 51 | EXEC with params | `EXEC dbo.usp_Load @Mode = 1` | Call target with parameters | `usp_ExecWithParams` |
| 52 | EXEC with OUTPUT | `EXEC dbo.usp_Load @Result OUTPUT` | Call target with output param | (covered by `usp_ExecWithParams`) |
| 53 | EXECUTE keyword | `EXECUTE dbo.usp_Load` | Same as EXEC | (covered by `usp_ExecSimple`) |
| 54 | EXEC with return value | `EXEC @rc = dbo.usp_Load` | Return variable prefix | `usp_ExecWithReturn` |
| 55 | EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | 3-part name, out of scope | (flagged as cross-DB error) |
| 56 | EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | 4-part name, out of scope | (flagged as cross-DB error) |
| 57 | sp_executesql static | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | SQL in string literal | `usp_ExecSpExecutesql` |
| 58 | sp_executesql dynamic | `EXEC sp_executesql @sql` | Variable SQL | `usp_ExecSpExecutesql` |
| 59 | EXEC variable SQL | `EXEC (@sql)` | Runtime string | `usp_ExecDynamic` |
| 60 | EXEC concat | `EXEC ('INSERT INTO ' + @table)` | Runtime string building | (covered by `usp_ExecDynamic`) |

## TRUNCATE Split Behavior

`classify_statement` returns `action: "skip"` for `exp.TruncateTable`, but `_collect_write_refs` independently traverses `TruncateTable` nodes and adds them to `writes_to` with operation `"TRUNCATE"`. This means TRUNCATE appears in `writes_to` for dependency tracking but is excluded from the `migrate` statement list. The split is intentional: the table relationship matters for scoping, but TRUNCATE itself does not translate to dbt SQL.

## Known Limitations

- **EXEC call graph**: sqlglot produces `Command` nodes for all EXEC variants. The `calls` field in `ObjectRefs` is always empty at the AST layer. However, `catalog_enrich.py` extracts EXEC-based call edges via `_extract_calls()` and materializes indirect write targets into catalog files. The enrichment path compensates for this limitation post-parse.
- **Dynamic SQL**: `EXEC (@sql)` and `sp_executesql @var` cannot be resolved statically. Only Claude can reason about what SQL might be constructed at runtime.
- **IF condition tables**: Tables referenced only in `IF EXISTS (SELECT ... FROM table)` conditions are not captured in `reads_from`. These are condition checks, not data reads for the model.
