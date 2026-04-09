---
name: refactoring-sql
description: >
  Refactors raw T-SQL stored procedure SQL into an import/logical/final CTE
  pattern. Uses two isolated sub-agents to avoid context pollution: one extracts
  the core SELECT from the proc, the other restructures it into CTEs. Proves
  equivalence via sandbox execution. Invoke when the user asks to "refactor SQL",
  "restructure to CTEs", or "prepare SQL for migration".
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Refactoring SQL

Restructure a stored procedure's SQL into import/logical/final CTEs while proving the refactored SQL produces identical results. Uses two isolated sub-agents to produce independent outputs, then compares them in the sandbox. The output stays in T-SQL — dbt Jinja conversion happens in the downstream `generating-model` skill.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing. The writer is read from the catalog scoping section (`catalog/tables/<table>.json` -> `scoping.selected_writer`).

## Schema discipline

Whenever this skill writes structured JSON back to the catalog, treat the schemas in `../../lib/shared/schemas/` as the contract:

- table refactor: `table_catalog.json#/properties/refactor`
- view refactor: `view_catalog.json#/properties/refactor`

Do not invent field names or omit required fields. If `refactor write` returns a validation error, fix the SQL or payload and retry the command.

Use the canonical `/refactor` surfaced code list in `../../lib/shared/refactor_error_codes.md`. Do not define a competing public error-code list in this skill.

## Harness mode

When the caller explicitly says there is no live sandbox, to skip `compare-sql`, or to use logical equivalence checks instead, treat that as harness mode.

In harness mode:

- Do not ask for confirmation about skipping sandbox execution.
- Do not block on missing sandbox availability.
- Use the available context, extracted SQL, and refactored SQL to perform a logical equivalence review instead of running `compare-sql`.
- Continue to `refactor write` with `partial` only if logical differences remain unresolved after the allowed retry loop.

## Before invoking

Check stage readiness:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> refactor
```

If `ready` is `false`, report the failing `code` and `reason` to the user and stop.

If harness mode is active and readiness fails only because test generation is incomplete, continue with the available test-spec and use logical equivalence checks instead of blocking on `compare-sql`.

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

**Multi-table-writer:** If `writer_ddl_slice` is present in the context, the writer is a multi-table proc. Refactor `writer_ddl_slice` — it is the portion of the proc that writes to this table. The full `proc_body` is for reference only.

**View detection:** If the context output contains `object_type = "view"` or `"mv"`, the FQN refers to a view. In this case:

- `view_sql` contains the original view SQL body (this is the ground truth — sub-agent A uses it directly instead of extracting from a procedure body)
- There is no `writer`, `proc_body`, or `statements` — these fields are absent for views
- The equivalence audit via `compare-sql` is unchanged: sql_a = original view SQL, sql_b = refactored CTE SQL
- Write-back via `refactor write` auto-detects the view and writes to the view catalog

## Step 1.5: Check for existing dbt models

Before launching sub-agents, check if existing dbt models can inform the CTE structure:

1. For each FQN in `source_tables` from the context output:
   - Check if `dbt/models/staging/stg_<source_table_name>.sql` exists (lowercase, schema stripped)
   - If it exists, read the file content
2. Check if `dbt/models/marts/<target_model_name>.sql` exists (lowercase, schema stripped from the target FQN)
   - If it exists, read the file content

If any existing models are found, pass them to Sub-Agent B as additional context (see below). Sub-Agent A is unaffected — it always produces ground truth from the original SQL.

## Step 2: Launch two sub-agents in parallel

Launch both sub-agents simultaneously. They must not see each other's output -- this prevents context pollution so the equivalence comparison is meaningful. Both agents use [references/sp-migration-ref.md](references/sp-migration-ref.md) for DML extraction and CTE restructuring rules.

### Sub-agent A: Extract core SELECT

Launch a sub-agent with this prompt (include the full `proc_body`, `statements`, and `columns` from context):

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

   Import CTEs: One per source table. Prefer explicit column selection from the
   bracket-quoted table reference. Only use SELECT * when there is no existing
   staging model and the source is being passed through unchanged. Name
   descriptively after the source.

   Logical CTEs: One transformation step per CTE. Each does one thing: join,
   filter, aggregate, or transform. Names describe the transformation.

   Final CTE: Assembles the final column list matching the target table. This
   must always be an explicit `final` CTE, even when it only selects from the
   previous logical CTE.

3. End with: SELECT * FROM final. Do not treat the final projection as
   implicit.
4. Keep T-SQL syntax (ISNULL, CONVERT, etc.) -- no dialect conversion
5. Replace procedure parameters with literal defaults where possible
6. Flatten nested subqueries into sequential CTEs
7. Temp tables become logical CTEs
8. Cursor loops become set-based operations (window functions, JOINs)

Return ONLY the refactored CTE SELECT SQL, nothing else.
```

If Step 1.5 found existing dbt models, append this to Sub-Agent B's prompt:

```text
Existing staging models (align your import CTE column names with these):
<for each existing stg model>
File: stg_<name>.sql
---
<file content>
---
</for each>

<if mart model exists>
Existing mart model (use as guidance for final CTE column ordering):
File: <mart_model_name>.sql
---
<file content>
---
</if>

When an existing staging model defines specific column names, aliases, or
casts, use those same names in your import CTE rather than SELECT *.
This is a hard requirement: do not use SELECT * in import CTEs when a staging
model exists for that source.
When an existing mart model exists, align your final CTE's column list
with its SELECT output.
```

The sub-agent writes the result to `.staging/<table_fqn>-refactored.sql`.

## Step 3: Equivalence audit

After both sub-agents complete, run the comparison CLI which seeds fixtures, executes both SELECTs, and returns the difference in rows:

```bash
mkdir -p .staging

uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --sql-a-file .staging/<table_fqn>-extracted.sql \
  --sql-b-file .staging/<table_fqn>-refactored.sql \
  --spec test-specs/<table_fqn>.json
```

Read the output JSON. For each scenario:

- `equivalent: true` -- refactored CTE SQL produces identical rows to extracted core SELECT
- `equivalent: false` -- `a_minus_b` shows rows in A (extracted) but not B (refactored), `b_minus_a` shows the reverse

### Self-correction loop (max 3 iterations)

If any scenario fails (`equivalent: false`):

1. Analyse the diff: which rows differ and why (missing join, wrong filter, dropped column, type mismatch)
2. Revise **only the refactored CTE SQL** (sub-agent B's output) to fix the semantic gap. The extracted SQL (sub-agent A's output) is the ground truth -- never modify it.
3. Rewrite `.staging/<table_fqn>-refactored.sql`
4. Re-run `compare-sql`
5. Repeat up to 3 times total

After 3 failed iterations, set `status` to `partial` and report the remaining diffs to the user.

## Step 4: Write to catalog

After audit passes (or after max iterations with partial status):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/<table_fqn>-extracted.sql \
  --refactored-sql-file .staging/<table_fqn>-refactored.sql
```

The CLI determines status from the content — do not pass `--status`.

Clean up the staging files:

```bash
rm -f .staging/<table_fqn>-extracted.sql .staging/<table_fqn>-refactored.sql
```

## Step 5: Report to user

Present:

1. The extracted core SELECT (sub-agent A output)
2. The refactored CTE SQL (sub-agent B output)
3. CTE structure summary (import CTEs, logical CTEs, final)
4. Equivalence audit results (per-scenario pass/fail)
5. Any remaining diffs if status is partial

## References

- [references/sp-migration-ref.md](references/sp-migration-ref.md) — DML extraction rules per statement type (INSERT, MERGE, UPDATE, etc.) and CTE restructuring patterns for sub-agents A and B

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Tell user which prerequisite is missing |
| `refactor context` | 2 | IO/parse error. Surface the error message |
| `refactor write` | 1 | Validation failure. Tell user to check the SQL |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | All scenarios failed. Enter self-correction loop |
