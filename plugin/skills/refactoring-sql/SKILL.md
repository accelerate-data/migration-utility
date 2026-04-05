---
name: refactoring-sql
description: >
  Refactors raw T-SQL stored procedure SQL into an import/logical/final CTE
  pattern. Proves semantic equivalence by executing both original and refactored
  SQL in the sandbox against test-spec fixtures and comparing result sets.
  Invoke when the user asks to "refactor SQL", "restructure to CTEs", or
  "prepare SQL for migration".
user-invocable: true
argument-hint: "<schema.table>"
---

# Refactoring SQL

Restructure a stored procedure's SQL into import/logical/final CTEs while proving the refactored SQL produces identical results. The refactored SQL stays in T-SQL — dbt Jinja conversion happens in the downstream `generating-model` skill.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing. The writer is read from the catalog scoping section (`catalog/tables/<table>.json` -> `scoping.selected_writer`).

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <table_fqn> refactor
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context \
  --table <table_fqn>
```

Read the output JSON. It contains:

- `proc_body` -- full original procedure SQL
- `profile` -- classification, keys, watermark, PII answers
- `statements` -- resolved statement list with action (migrate/skip) and SQL
- `columns` -- target table column list
- `source_tables` -- tables read by the writer
- `test_spec` -- full test-spec JSON with fixtures and expect.rows
- `sandbox` -- sandbox database metadata

Record the `writer` field -- this is the procedure FQN used in the equivalence audit.

## Step 2: Analyse the procedure

Examine `proc_body` and `statements` (where `action == migrate`) to understand the transformation logic:

| Pattern | What to look for |
|---|---|
| Direct INSERT...SELECT | Source tables, joins, filters |
| Temp table chains | #temp creation, population, then read |
| MERGE statements | USING clause, MATCHED/NOT MATCHED arms |
| Cursor-based loops | DECLARE CURSOR, FETCH, processing logic |
| Dynamic SQL | sp_executesql with constructed queries |
| Conditional branches | IF/ELSE that alter INSERT behavior |

Map out the data flow: which source tables are read, what transformations are applied, and what ends up in the target table.

## Step 3: Generate refactored CTE SQL

Restructure the `migrate` statements into a single SELECT using the **import CTE -> logical CTE -> final CTE** pattern. The output must be a standalone T-SQL SELECT statement (no CREATE PROCEDURE, no INSERT INTO target).

### Import CTEs

One CTE per source table, reading all needed columns:

```sql
WITH source_customers AS (
    SELECT *
    FROM [bronze].[Customer]
),

dim_product AS (
    SELECT *
    FROM [silver].[DimProduct]
),
```

Rules:

- One CTE per source table in `source_tables`
- Name the CTE descriptively after the source table
- Use the original bracket-quoted table references (not dbt Jinja -- that comes later)
- `SELECT *` or select only the columns used downstream

### Logical CTEs

One transformation step per CTE:

```sql
customers_with_region AS (
    SELECT
        c.CustomerKey,
        c.FirstName,
        g.Country AS Region
    FROM source_customers c
    LEFT JOIN source_geography g
        ON c.GeographyKey = g.GeographyKey
),

filtered_customers AS (
    SELECT *
    FROM customers_with_region
    WHERE Region IS NOT NULL
),
```

Rules:

- Each CTE does one thing: join, filter, aggregate, or transform
- CTE names describe the transformation
- Preserve the original SQL semantics exactly (same joins, filters, aggregations)
- Keep T-SQL syntax (ISNULL, CONVERT, etc.) -- do not convert to ANSI yet
- Replace procedure parameters with literal defaults or comments noting the parameter
- Flatten nested subqueries into sequential CTEs

### Final CTE and SELECT

```sql
final AS (
    SELECT
        CustomerKey,
        FirstName,
        Region
    FROM filtered_customers
)

SELECT * FROM final
```

The final SELECT must produce the same columns and rows as the original procedure would write to the target table.

### Handling complex patterns

| Pattern | Refactoring approach |
|---|---|
| Temp table chains | Each #temp becomes a logical CTE |
| MERGE with USING clause | Extract USING as a CTE, final SELECT captures MERGE output |
| Multiple INSERTs to same target | UNION ALL in a logical CTE |
| Cursor loops | Rewrite as set-based operations (JOIN, window functions) |
| Dynamic SQL | Inline the constructed query |

## Step 4: Write to staging file

Write the refactored SQL to a temporary file:

```bash
mkdir -p .staging
```

Write the CTE SQL to `.staging/refactored.sql` using the Write tool.

## Step 5: Equivalence audit

Run the comparison CLI which handles the full lifecycle (seed fixtures, run original proc, wipe target, run refactored SQL, compute symmetric diff) inside a rolled-back transaction:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --procedure <writer_fqn> \
  --refactored-sql-file .staging/refactored.sql \
  --spec test-specs/<table_fqn>.json \
  --target-table <table_fqn>
```

Read the output JSON. For each scenario:

- `equivalent: true` -- refactored SQL produces identical rows to original proc
- `equivalent: false` -- `a_minus_b` shows rows in original but not refactored, `b_minus_a` shows the reverse

### Self-correction loop (max 3 iterations)

If any scenario fails (`equivalent: false`):

1. Analyse the diff: which rows differ and why (missing join, wrong filter, dropped column, type mismatch)
2. Revise the CTE SQL to fix the semantic gap
3. Rewrite `.staging/refactored.sql`
4. Re-run `compare-sql`
5. Repeat up to 3 times total

After 3 failed iterations, set `status` to `partial` and report the remaining diffs to the user.

## Step 6: Write to catalog

After audit passes (or after max iterations with partial status):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --refactored-sql-file .staging/refactored.sql \
  --status ok
```

Use `--status partial` if the audit did not fully pass. Use `--status error` if refactoring could not proceed at all.

Clean up the staging file:

```bash
rm -rf .staging
```

## Step 7: Report to user

Present:

1. The refactored CTE SQL (full text)
2. CTE structure summary (import CTEs, logical CTEs, final)
3. Equivalence audit results (per-scenario pass/fail)
4. Any remaining diffs if status is partial

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Tell user which prerequisite is missing |
| `refactor context` | 2 | IO/parse error. Surface the error message |
| `refactor write` | 1 | Validation failure. Tell user to check the refactored SQL |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | All scenarios failed. Enter self-correction loop |
