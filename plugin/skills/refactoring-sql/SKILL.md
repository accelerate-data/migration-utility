---
name: refactoring-sql
description: >
  Refactors raw T-SQL stored procedure SQL into an import/logical/final CTE
  pattern. Uses two isolated sub-agents to avoid context pollution: one extracts
  the core SELECT from the proc, the other restructures it into CTEs. Proves
  equivalence via semantic review and, when available, sandbox execution. Invoke
  when the user asks to "refactor SQL", "restructure to CTEs", or "prepare SQL
  for migration".
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Refactoring SQL

## Arguments

`$ARGUMENTS` is the fully-qualified object name (table or view). Ask the user if missing.

## Contracts

Use these schema contracts when you write refactor data:

- table refactor: `../../lib/shared/schemas/table_catalog.json#/properties/refactor`
- view refactor: `../../lib/shared/schemas/view_catalog.json#/properties/refactor`

Do not invent fields. If `refactor write` rejects the payload, fix the payload and retry.

## Compare decision

Decide whether to run executable `compare-sql` using this order:

1. If the caller explicitly says to skip `compare-sql`, skip it.
2. Otherwise check sandbox availability:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status
```

1. If sandbox status succeeds, run `compare-sql`.
2. If sandbox status fails, do not block. Fall back to semantic review only.

When you skip executable compare:

- do not ask for approval
- do not block on missing sandbox access
- still run semantic review
- write refactor evidence with `--no-compare-required`

This path should normally persist `status: partial`, not `ok`.

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> refactor
```

If `ready` is `false`, stop and report the returned `code` and `reason`.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context \
  --table <table_fqn>
```

If `refactor context` fails because a prerequisite is missing, return `status: "error"` with
code `CONTEXT_PREREQUISITE_MISSING`. If it fails due to IO or parse issues, return
`status: "error"` with code `CONTEXT_IO_ERROR`. Do not surface raw labels such as
`no_writer_configured` in the result JSON.

Read `object_type` from the output to know which path you are on:

**Table (`object_type: "table"`):**

- `proc_body` — full original procedure SQL
- `writer` — procedure FQN
- `statements` — resolved statement list with action (migrate/skip) and SQL
- `columns` — target table column list
- `source_tables` — tables read by the writer
- `test_spec` — full test-spec JSON with fixtures and expect.rows
- `sandbox` — sandbox database metadata
- `writer_ddl_slice` — present for multi-table writers; use this as the SQL to refactor instead of the full `proc_body`

**View (`object_type: "view"` or `"mv"`):**

- `view_sql` — original view SQL body; this is the ground truth for sub-agent A
- `columns`, `source_tables`, `test_spec`, `sandbox` — same as table path
- No `writer`, `proc_body`, or `statements` — absent for views
- Write-back via `refactor write` auto-detects the view and writes to the view catalog

## Step 2: Launch two sub-agents in parallel

Launch both sub-agents simultaneously. They must not see each other's output. Both agents use [references/sp-migration-ref.md](references/sp-migration-ref.md) for DML extraction and CTE restructuring rules.

### Sub-agent A: Extract core SELECT

Launch a sub-agent with this prompt. For tables, include `proc_body`, `statements`, and `columns` (or `writer_ddl_slice` if present). For views, include `view_sql` and `columns` in place of `proc_body`/`statements`.

```text
You are extracting the core transformation logic from a T-SQL stored procedure
as a pure SELECT statement.

Read the references/sp-migration-ref.md reference for extraction rules per DML type.

Procedure body:
<proc_body>

Resolved statements (action=migrate only):
<statements>

Target table columns:
<columns>

Instructions:
1. Identify the DML pattern(s) in the migrate statements (INSERT...SELECT, MERGE,
   UPDATE, DELETE, temp table chains, cursor loops, dynamic SQL)
2. Apply the extraction rules from references/sp-migration-ref.md for each pattern
3. Produce a single pure T-SQL SELECT statement that returns exactly the rows
   and columns the procedure would write to the target table
4. Keep T-SQL syntax (ISNULL, CONVERT, etc.) -- no dialect conversion
5. Replace procedure parameters with literal defaults where possible

Return ONLY the extracted SELECT SQL, nothing else.
```

The sub-agent writes the result to `.staging/<table_fqn>-extracted.sql`.

### Sub-agent B: Refactor into CTEs

Launch a sub-agent with this prompt. For tables, include `proc_body`, `statements`, `columns`, `source_tables`, and `profile` (or `writer_ddl_slice` if present). For views, include `view_sql`, `columns`, and `source_tables` in place of `proc_body`/`statements`.

```text
You are restructuring a T-SQL stored procedure into a clean CTE-based SELECT
following the import/logical/final CTE pattern.

Procedure body:
<proc_body>

Resolved statements (action=migrate only):
<statements>

Target table columns:
<columns>

Source tables:
<source_tables>

Profile:
<profile>

Instructions:
1. Analyse the procedure's data flow: source tables read, transformations applied,
   target table written
2. Restructure into import CTE -> logical CTE -> final CTE pattern:

   Import CTEs: One per source table. SELECT * (or needed columns) from the
   bracket-quoted table reference. Name descriptively after the source.

   Logical CTEs: One transformation step per CTE. Each does one thing: join,
   filter, aggregate, or transform. Names describe the transformation.

   Final CTE: Assembles the final column list matching the target table.

3. End with: SELECT * FROM final
4. Keep T-SQL syntax (ISNULL, CONVERT, etc.) -- no dialect conversion
5. Replace procedure parameters with literal defaults where possible
6. Flatten nested subqueries into sequential CTEs
7. Temp tables become logical CTEs
8. Cursor loops become set-based operations (window functions, JOINs)

Return ONLY the refactored CTE SELECT SQL, nothing else.
```

The sub-agent writes the result to `.staging/<table_fqn>-refactored.sql`.

## Step 3: Semantic review

After both sub-agents complete, launch a third isolated sub-agent to validate semantic equivalence between the extracted SQL and the refactored SQL.

Inputs:

- extracted SQL (`.staging/<table_fqn>-extracted.sql`)
- refactored SQL (`.staging/<table_fqn>-refactored.sql`)
- target columns
- source tables

The sub-agent must return exactly one JSON object with this shape:

```json
{
  "passed": true,
  "checks": {
    "source_tables": { "passed": true, "summary": "..." },
    "output_columns": { "passed": true, "summary": "..." },
    "joins": { "passed": true, "summary": "..." },
    "filters": { "passed": true, "summary": "..." },
    "aggregation_grain": { "passed": true, "summary": "..." }
  },
  "issues": [
    {
      "code": "EQUIVALENCE_PARTIAL",
      "message": "Refactored SQL drops the inactive-customer filter from the extracted SQL.",
      "severity": "warning"
    }
  ]
}
```

Rules for the semantic-review sub-agent:

- compare extracted SQL to refactored SQL, not to dbt expectations
- use only these checks: source tables, output columns, joins, filters, aggregation grain
- `issues[]` must use diagnostics-style entries
- if any check fails, `passed` must be `false`

Write the JSON to `.staging/<table_fqn>-semantic-review.json`.

**If semantic review fails (`passed: false`):** do not proceed to the equivalence audit. Skip to Step 5 and persist with `--no-compare-required`. Status will be `partial`.

## Step 4: Equivalence audit

When sandbox status succeeds and the caller did not say to skip `compare-sql`, run the comparison CLI:

```bash
mkdir -p .staging

uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --sql-a-file .staging/<table_fqn>-extracted.sql \
  --sql-b-file .staging/<table_fqn>-refactored.sql \
  --spec test-specs/<table_fqn>.json
```

Write the JSON output to `.staging/<table_fqn>-compare.json`.

Read the output JSON. For each scenario:

- `equivalent: true` — refactored CTE SQL produces identical rows to extracted core SELECT
- `equivalent: false` — `a_minus_b` shows rows in A (extracted) but not B (refactored), `b_minus_a` shows the reverse

### Self-correction loop (max 3 iterations)

If any scenario fails (`equivalent: false`):

1. Analyse the diff: which rows differ and why (missing join, wrong filter, dropped column, type mismatch)
2. Revise **only the refactored CTE SQL** (sub-agent B's output) to fix the semantic gap. The extracted SQL (sub-agent A's output) is the ground truth — never modify it.
3. Rewrite `.staging/<table_fqn>-refactored.sql`
4. Rerun semantic review and write the updated result to `.staging/<table_fqn>-semantic-review.json`
5. Rerun `compare-sql`
6. Repeat up to 3 times total

After 3 failed iterations, proceed to Step 5 with the partial result.

## Step 5: Write to catalog

### When sandbox comparison ran and passed

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/<table_fqn>-extracted.sql \
  --refactored-sql-file .staging/<table_fqn>-refactored.sql \
  --semantic-review-file .staging/<table_fqn>-semantic-review.json \
  --compare-sql-file .staging/<table_fqn>-compare.json
```

### When sandbox was unavailable or semantic review failed

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/<table_fqn>-extracted.sql \
  --refactored-sql-file .staging/<table_fqn>-refactored.sql \
  --semantic-review-file .staging/<table_fqn>-semantic-review.json \
  --no-compare-required
```

Do not invent or override the status.

## Step 6: Clean up and report

Delete the staging files after `refactor write` succeeds:

```bash
rm -f .staging/<table_fqn>-extracted.sql \
       .staging/<table_fqn>-refactored.sql \
       .staging/<table_fqn>-semantic-review.json \
       .staging/<table_fqn>-compare.json
```

Report:

1. Extracted core SELECT (sub-agent A output)
2. Refactored CTE SQL (sub-agent B output)
3. CTE structure summary (import CTEs, logical CTEs, final)
4. Semantic review verdict (passed/failed, any issues)
5. Equivalence audit results (per-scenario pass/fail, or skipped with reason)
6. Final persisted status

## References

- [references/sp-migration-ref.md](references/sp-migration-ref.md) — DML extraction rules per statement type (INSERT, MERGE, UPDATE, etc.) and CTE restructuring patterns for sub-agents A and B

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Return `status: "error"` with code `CONTEXT_PREREQUISITE_MISSING` and mention the missing prerequisite in the message |
| `refactor context` | 2 | IO/parse error. Return `status: "error"` with code `CONTEXT_IO_ERROR` and surface the error message |
| `refactor write` | 1 | Validation failure. Fix payload and retry once |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | All scenarios failed. Enter self-correction loop |
