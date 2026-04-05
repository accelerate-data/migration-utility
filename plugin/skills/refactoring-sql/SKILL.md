---
name: refactoring-sql
description: >
  Refactors raw T-SQL stored procedure SQL into an import/logical/final CTE
  pattern. Uses two isolated sub-agents to avoid context pollution: one extracts
  the core SELECT from the proc, the other restructures it into CTEs. Proves
  equivalence via sandbox execution. Invoke when the user asks to "refactor SQL",
  "restructure to CTEs", or "prepare SQL for migration".
user-invocable: true
argument-hint: "<schema.table>"
---

@sp-migration-ref.md

# Refactoring SQL

Restructure a stored procedure's SQL into import/logical/final CTEs while proving the refactored SQL produces identical results. Uses two isolated sub-agents to produce independent outputs, then compares them in the sandbox. The output stays in T-SQL — dbt Jinja conversion happens in the downstream `generating-model` skill.

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

Record the `writer` field -- this is the procedure FQN.

## Step 2: Launch two sub-agents in parallel

Launch both sub-agents simultaneously. They must not see each other's output -- this prevents context pollution so the equivalence comparison is meaningful.

### Sub-agent A: Extract core SELECT

Launch a sub-agent with this prompt (include the full `proc_body`, `statements`, and `columns` from context):

```text
You are extracting the core transformation logic from a T-SQL stored procedure
as a pure SELECT statement.

Read the sp-migration-ref.md reference for extraction rules per DML type.

Procedure body:
<proc_body>

Resolved statements (action=migrate only):
<statements>

Target table columns:
<columns>

Instructions:
1. Identify the DML pattern(s) in the migrate statements (INSERT...SELECT, MERGE,
   UPDATE, DELETE, temp table chains, cursor loops, dynamic SQL)
2. Apply the extraction rules from sp-migration-ref.md for each pattern
3. Produce a single pure T-SQL SELECT statement that returns exactly the rows
   and columns the procedure would write to the target table
4. Keep T-SQL syntax (ISNULL, CONVERT, etc.) -- no dialect conversion
5. Replace procedure parameters with literal defaults where possible

Return ONLY the extracted SELECT SQL, nothing else.
```

The sub-agent writes the result to `.staging/extracted.sql`.

### Sub-agent B: Refactor into CTEs

Launch a sub-agent with this prompt (include the full `proc_body`, `statements`, `columns`, `source_tables`, and `profile` from context):

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

The sub-agent writes the result to `.staging/refactored.sql`.

## Step 3: Equivalence audit

After both sub-agents complete, run the comparison CLI which seeds fixtures, executes both SELECTs, and computes symmetric diff inside a rolled-back transaction:

```bash
mkdir -p .staging

uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --sql-a-file .staging/extracted.sql \
  --sql-b-file .staging/refactored.sql \
  --spec test-specs/<table_fqn>.json
```

Read the output JSON. For each scenario:

- `equivalent: true` -- refactored CTE SQL produces identical rows to extracted core SELECT
- `equivalent: false` -- `a_minus_b` shows rows in A (extracted) but not B (refactored), `b_minus_a` shows the reverse

### Self-correction loop (max 3 iterations)

If any scenario fails (`equivalent: false`):

1. Analyse the diff: which rows differ and why (missing join, wrong filter, dropped column, type mismatch)
2. Revise **only the refactored CTE SQL** (sub-agent B's output) to fix the semantic gap. The extracted SQL (sub-agent A's output) is the ground truth -- never modify it.
3. Rewrite `.staging/refactored.sql`
4. Re-run `compare-sql`
5. Repeat up to 3 times total

After 3 failed iterations, set `status` to `partial` and report the remaining diffs to the user.

## Step 4: Write to catalog

After audit passes (or after max iterations with partial status):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/extracted.sql \
  --refactored-sql-file .staging/refactored.sql \
  --status ok
```

Use `--status partial` if the audit did not fully pass. Use `--status error` if refactoring could not proceed at all.

Clean up the staging files:

```bash
rm -rf .staging
```

## Step 5: Report to user

Present:

1. The extracted core SELECT (sub-agent A output)
2. The refactored CTE SQL (sub-agent B output)
3. CTE structure summary (import CTEs, logical CTEs, final)
4. Equivalence audit results (per-scenario pass/fail)
5. Any remaining diffs if status is partial

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Tell user which prerequisite is missing |
| `refactor context` | 2 | IO/parse error. Surface the error message |
| `refactor write` | 1 | Validation failure. Tell user to check the SQL |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | All scenarios failed. Enter self-correction loop |
