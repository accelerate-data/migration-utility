# T-SQL Parse Classification

Exhaustive classification of T-SQL patterns encountered in stored procedure migration. Each pattern is routed to the deterministic sqlglot path, the enrichment path, or the Claude-assisted path.

## Architecture

Classification is driven by catalog routing flags set at setup-ddl time, not by AST node types at query time.

```text
Stored Procedure DDL
        │
        ▼
  scan_routing_flags(definition)          ← catalog.py, runs during setup-ddl
        │
        ▼
  catalog/procedures/<proc>.json          ← persisted flags: needs_llm, needs_enrich
        │
        ├── needs_llm = False → discover show: "deterministic"
        │     extract_refs() may still parse body for statement breakdown
        │
        └── needs_llm = True  → discover show: "claude_assisted"
              statements = null; raw_ddl preserved for Claude
```

Procs with `needs_enrich = True` (but `needs_llm = False`) are deterministic — `catalog_enrich.py` resolves their references post-parse via BFS call graph traversal.

## Decision Rule

`scan_routing_flags()` in `catalog.py` applies two regexes to the procedure definition text:

- **`_NEEDS_LLM_RE`** matches: `EXEC(@sql)` (dynamic execution), `BEGIN TRY`, `WHILE`, `IF` → sets `needs_llm = True` → classification: `claude_assisted`
- **`_NEEDS_ENRICH_RE`** matches: static `EXEC dbo.proc` (excludes `sp_executesql` and dynamic `EXEC(@var)`), `SELECT INTO`, `TRUNCATE` → sets `needs_enrich = True` → classification: `deterministic` (enriched by `catalog_enrich.py`)
- **Neither flag** → pure sqlglot deterministic path. `extract_refs` handles everything.

`discover show` maps these flags to the `classification` field:

- `classification: "deterministic"` + populated `statements` → fully parsed (with or without enrichment)
- `classification: "claude_assisted"` → needs LLM reasoning from raw DDL

Note: `parse_error` is a separate orthogonal field that records whether the `CREATE PROCEDURE` block itself failed to parse at DDL load time.

## Pattern Classification

### Deterministic — sqlglot (patterns 1-44)

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
| 18 | *(removed — see Skip-only table)* | | |
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

### Skip-only — no pattern number, no ref extraction (deterministic path)

These statements are parsed by sqlglot but classified as `skip` — they don't produce refs and aren't migrated to dbt SQL.

| Statement type | Example | Notes |
|---|---|---|
| SET | `SET NOCOUNT ON`, `SET XACT_ABORT ON` | Session config |
| DECLARE | `DECLARE @i INT = 0` | Variable declaration |
| RETURN | `RETURN 0` | Early exit — no data operation |
| PRINT | `PRINT 'Loading...'` | Logging |
| RAISERROR | `RAISERROR('Error', 16, 1)` | Error handling |
| THROW | `THROW 50001, 'msg', 1` | Error handling (modern syntax) |
| BEGIN/COMMIT/ROLLBACK | `BEGIN TRAN ... COMMIT` | Transaction control — dbt manages transactions |
| DROP/CREATE INDEX | `DROP INDEX ix_1 ON silver.T; CREATE INDEX ...` | Index management — `classify_statement` returns `skip` for both `exp.Drop` and `exp.Create` (kind=INDEX) |

Note: `IF EXISTS (SELECT 1 FROM dbo.T)` is **not** skip-only. The `\bIF\b` pattern in `_NEEDS_LLM_RE` matches all `IF` statements including `IF EXISTS`, so procs containing `IF EXISTS` are classified `claude_assisted`. See control flow patterns 45-48 below.

### Enrichment-resolved — deterministic after `catalog_enrich.py` (patterns 49-57)

These patterns produce `Command` nodes in sqlglot but set `needs_enrich = True` (not `needs_llm`), so they remain **deterministic** in `discover show`. After setup-ddl, `catalog_enrich.py` resolves their references via BFS call graph traversal and materializes indirect write targets into catalog files.

**Static EXEC patterns (49-54):** `_NEEDS_ENRICH_RE` matches static procedure calls. The call target is known at parse time — enrichment follows the graph.

| # | Pattern | Example | Resolution | Test fixture |
|---|---|---|---|---|
| 49 | EXEC proc | `EXEC dbo.usp_Load` | `catalog_enrich.py` BFS call graph | `usp_ExecSimple` |
| 50 | EXEC bracketed | `EXEC [silver].[usp_Load]` | Same — bracket notation | `usp_ExecBracketed` |
| 51 | EXEC with params | `EXEC dbo.usp_Load @Mode = 1` | Same — params don't affect target | `usp_ExecWithParams` |
| 52 | EXEC with OUTPUT | `EXEC dbo.usp_Load @Result OUTPUT` | Same — output param | (covered by `usp_ExecWithParams`) |
| 53 | EXECUTE keyword | `EXECUTE dbo.usp_Load` | Same as EXEC | (covered by `usp_ExecSimple`) |
| 54 | EXEC with return value | `EXEC @rc = dbo.usp_Load` | Same — return variable prefix | `usp_ExecWithReturn` |

**sp_executesql with static SQL (57):** `sp_executesql` is excluded from both `_NEEDS_LLM_RE` and `_NEEDS_ENRICH_RE` by negative lookahead. DMF resolves the embedded SQL.

| # | Pattern | Example | Resolution | Test fixture |
|---|---|---|---|---|
| 57 | sp_executesql static | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | DMF resolves embedded SQL | `usp_ExecSpExecutesql` |

### Claude-assisted — control flow and dynamic SQL (patterns 45-48, 55-56, 58-60)

These patterns set `needs_llm = True` via `_NEEDS_LLM_RE` or are out of scope. The proc's `raw_ddl` is passed to Claude for analysis.

**Control flow patterns (45-48):** `_NEEDS_LLM_RE` matches `IF`, `BEGIN TRY`, and `WHILE`. DML nested inside these blocks is not reliably extracted by sqlglot.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 45 | IF/ELSE BEGIN END | Control flow wrapping DML | `\bIF\b` → `needs_llm` — DML inside branches not reliably extracted | `usp_ConditionalMerge` |
| 46 | BEGIN TRY/CATCH | Error handling wrapping DML | `\bBEGIN\s+TRY\b` → `needs_llm` — DML inside not extracted | `usp_TryCatchLoad` |
| 47 | WHILE BEGIN END | Loop wrapping DML | `\bWHILE\b` → `needs_llm` — DML inside not extracted | `usp_WhileLoop` |
| 48 | Nested control flow | IF inside WHILE inside TRY/CATCH | Multiple `needs_llm` matches — DML inside not extracted | `usp_NestedControlFlow` |

Note: `IF EXISTS (SELECT 1 FROM ...)` also matches `\bIF\b` and sets `needs_llm = True`. The regex does not distinguish simple condition checks from control flow branches that wrap DML.

**Cross-database/linked server EXEC (55-56):** Out of scope — flagged as errors.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 55 | EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | 3-part name, out of scope | (flagged as cross-DB error) |
| 56 | EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | 4-part name, out of scope | (flagged as cross-DB error) |

**Dynamic SQL patterns (58-60):** Runtime-constructed SQL that cannot be resolved statically.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 58 | sp_executesql dynamic | `EXEC sp_executesql @sql` | Variable SQL — cannot resolve statically | `usp_ExecSpExecutesql` |
| 59 | EXEC variable SQL | `EXEC (@sql)` | `\bEXEC\s*\(` → `needs_llm` — runtime string | `usp_ExecDynamic` |
| 60 | EXEC concat | `EXEC ('INSERT INTO ' + @table)` | Same — runtime string building | (covered by `usp_ExecDynamic`) |

## TRUNCATE Split Behavior

`classify_statement` returns `action: "skip"` for `exp.TruncateTable`, but `_collect_write_refs` independently traverses `TruncateTable` nodes and adds them to `writes_to` with operation `"TRUNCATE"`. This means TRUNCATE appears in `writes_to` for dependency tracking but is excluded from the `migrate` statement list. The split is intentional: the table relationship matters for scoping, but TRUNCATE itself does not translate to dbt SQL.

## Migrate vs Skip Classification

When a proc is `claude_assisted`, Claude reads `raw_ddl` and classifies each statement as **migrate** (becomes dbt model SQL) or **skip** (operational overhead that dbt handles or ignores). The full classification reference is at `plugins/migration/skills/analyzing-object/references/tsql-parse-classification.md`.

## Known Limitations

- **EXEC call graph**: sqlglot produces `Command` nodes for all EXEC variants. The `calls` field in `ObjectRefs` is always empty at the AST layer. However, `catalog_enrich.py` extracts EXEC-based call edges via `_extract_calls()` and materializes indirect write targets into catalog files. The enrichment path compensates for this limitation post-parse (see patterns 49-54).
- **Dynamic SQL**: `EXEC (@sql)` and `sp_executesql @var` cannot be resolved statically. Only Claude can reason about what SQL might be constructed at runtime (see patterns 58-60).
- **sp_executesql @var routing gap**: `sp_executesql` is excluded from both `_NEEDS_LLM_RE` and `_NEEDS_ENRICH_RE`, so `EXEC sp_executesql @sql` (pattern 58) sets neither flag. The SQL is genuinely dynamic, but the proc will be classified `deterministic`. This case requires manual review or a future regex refinement.
- **IF condition tables**: Tables referenced only in `IF EXISTS (SELECT ... FROM table)` conditions are not captured in `reads_from`. These are condition checks, not data reads for the model. However, the proc is still classified `claude_assisted` because `\bIF\b` matches `_NEEDS_LLM_RE`.
