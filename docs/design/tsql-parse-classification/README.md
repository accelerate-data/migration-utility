# T-SQL Parse Classification

Exhaustive classification of T-SQL patterns encountered in stored procedure migration. Each pattern is routed to either the deterministic sqlglot path or the Claude-assisted path.

## Architecture

```text
Stored Procedure DDL
        │
        ▼
  _parse_body_statements()
        │
        ├── Pass 1: recursive Command re-parsing
        │     strips BEGIN/END/ELSE/IF/WHILE/TRY/CATCH from Command nodes
        │     re-parses to recover DML
        │
        ├── Pass 2: full control flow stripping (fallback)
        │     catches statements sqlglot drops inside IF <cond> BEGIN...END
        │
        ▼
  Parsed statement list
        │
        ├── All statements are real AST nodes → Deterministic
        │     extract_refs() populates writes_to / reads_from
        │
        └── Any EXEC Command nodes remain → Claude-assisted
              raw_ddl preserved for Claude to analyse
```

## Decision Rule

After two-pass body parsing, check the statement list:

- **No Command nodes** → deterministic. `extract_refs` handles everything.
- **Command nodes with EXEC/EXECUTE** → Claude-assisted. The proc contains dynamic SQL or procedure calls that sqlglot cannot parse. `raw_ddl` is passed to Claude for analysis.

The `show` command signals this via:

- `parse_error: null` + populated `refs` → deterministic
- `parse_error` set → Claude-assisted

## Pattern Classification

### Deterministic — sqlglot (patterns 1-20)

These patterns are fully parsed by sqlglot after body extraction. `extract_refs` populates `writes_to` and `reads_from` without any LLM involvement.

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
| 18 | DROP/CREATE INDEX | `DROP INDEX ...; CREATE INDEX ...` around DML | `usp_FullReload` (inline test) |
| 19 | IF/ELSE BEGIN END | Control flow wrapping DML — two-pass recovers both branches | `usp_ConditionalMerge` |
| 20 | BEGIN TRY/CATCH | Error handling wrapping DML — two-pass recovers both blocks | `usp_TryCatchLoad` |
| 21 | WHILE BEGIN END | Loop wrapping DML — two-pass recovers loop body | `usp_WhileLoop` |
| 22 | Nested control flow | IF inside WHILE inside TRY/CATCH — recursive + fallback strip | `usp_NestedControlFlow` |

### Claude-assisted — EXEC/dynamic SQL (patterns 23-34)

These patterns produce Command nodes that cannot be resolved statically. The proc's `raw_ddl` is passed to Claude for analysis. Claude reads the proc body, follows call graphs, and determines write targets.

| # | Pattern | Example | Why Claude | Test fixture |
|---|---|---|---|---|
| 23 | EXEC proc | `EXEC dbo.usp_Load` | Call target is another proc — need to follow the graph | `usp_ExecSimple` |
| 24 | EXEC bracketed | `EXEC [silver].[usp_Load]` | Same — bracket notation | `usp_ExecBracketed` |
| 25 | EXEC with params | `EXEC dbo.usp_Load @Mode = 1` | Call target with parameters | `usp_ExecWithParams` |
| 26 | EXEC with OUTPUT | `EXEC dbo.usp_Load @Result OUTPUT` | Call target with output param | (covered by `usp_ExecWithParams`) |
| 27 | EXECUTE keyword | `EXECUTE dbo.usp_Load` | Same as EXEC | (covered by `usp_ExecSimple`) |
| 28 | EXEC with return value | `EXEC @rc = dbo.usp_Load` | Return variable prefix | `usp_ExecWithReturn` |
| 29 | EXEC cross-database | `EXEC OtherDB.dbo.usp_Load` | 3-part name, out of scope | (flagged as cross-DB error) |
| 30 | EXEC linked server | `EXEC [Server].db.dbo.usp_Load` | 4-part name, out of scope | (flagged as cross-DB error) |
| 31 | sp_executesql static | `EXEC sp_executesql N'INSERT INTO dbo.T ...'` | SQL in string literal | `usp_ExecSpExecutesql` |
| 32 | sp_executesql dynamic | `EXEC sp_executesql @sql` | Variable SQL | `usp_ExecSpExecutesql` |
| 33 | EXEC variable SQL | `EXEC (@sql)` | Runtime string | `usp_ExecDynamic` |
| 34 | EXEC concat | `EXEC ('INSERT INTO ' + @table)` | Runtime string building | (covered by `usp_ExecDynamic`) |

## How the Two-Pass Strategy Works

sqlglot v25.34.1 cannot parse T-SQL control flow (`IF`, `BEGIN/END`, `ELSE`, `WHILE`, `TRY/CATCH`) as standalone blocks. These become `Command` nodes.

**Pass 1 — Recursive Command re-parsing:**

1. Parse the proc body with `sqlglot.parse()`
2. For each `Command` node, strip leading control flow keywords (`BEGIN`, `END`, `ELSE`, `IF ...`, `WHILE ...`, `BEGIN TRY`, etc.)
3. Re-parse the stripped text
4. Repeat recursively (max depth 5) until no more Commands can be reduced

**Pass 2 — Full control flow stripping (fallback):**

1. Strip all control flow keywords from the raw body text using regex
2. Parse the remaining flat DML statements
3. Merge with pass 1 results, deduplicating by SQL text

This catches statements that sqlglot drops inside `IF <condition> BEGIN...END` blocks, where the IF node swallows `BEGIN` as its true branch and the INSERT between BEGIN and END is lost.

## Known Limitations

- **EXEC call graph**: sqlglot produces `Command` nodes for all EXEC variants. The `calls` field in `ObjectRefs` is always empty. Indirect writer detection via call graph traversal does not work for EXEC-based orchestrators.
- **Dynamic SQL**: `EXEC (@sql)` and `sp_executesql @var` cannot be resolved statically. Only Claude can reason about what SQL might be constructed at runtime.
- **IF condition tables**: Tables referenced only in `IF EXISTS (SELECT ... FROM table)` conditions are not captured in `reads_from` after control flow stripping. These are condition checks, not data reads for the model.
