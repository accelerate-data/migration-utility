---
name: refactoring-sql
description: >
  Use when a stored procedure, view, or materialized view must be rewritten into
  import/logical/final CTEs for migration, especially when semantic equivalence
  must be proven with semantic review and optional sandbox compare-sql.
user-invocable: false
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Refactoring SQL

Refactor source SQL into an import/logical/final CTE pattern, keep the extracted SQL as the ground truth, and persist only proof-backed results.

## Arguments

`$ARGUMENTS` is the fully-qualified object name (table or view). Ask the user if missing.

## When to Use

Use this skill when:

- a stored procedure, view, or materialized view must be restructured into import/logical/final CTEs before downstream model generation
- semantic equivalence must be checked before persisting the `refactor` section
- sandbox execution may be unavailable and the run must degrade cleanly to semantic-review-only proof

Do not use this skill when:

- the readiness guard for `refactor` fails
- the task is generating dbt models or test specs rather than restructuring source SQL
- the object already has a proof-backed refactor and the user did not ask for a rerun

## Quick Reference

| Item | Rule |
|---|---|
| Readiness guard | `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> refactor` |
| Context assembly | `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context --table <table_fqn>` |
| Sandbox check | `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status` |
| Compare behavior | If caller skips compare, or sandbox is unavailable, do semantic review only and persist with `--no-compare-required` |
| Statuses | `ok`, `partial`, `error` |
| Surfaced codes | Use only `../../lib/shared/refactor_error_codes.md` |
| Ground truth | Never modify the extracted SQL during self-correction |

The persisted `refactor` section has this shape:

```json
{
  "status": "ok | partial | error",
  "extracted_sql": "pure SELECT extracted from DML",
  "refactored_sql": "WITH ... CTE-structured SELECT",
  "semantic_review": {
    "passed": true,
    "checks": {
      "source_tables": { "passed": true, "summary": "..." },
      "output_columns": { "passed": true, "summary": "..." },
      "joins": { "passed": true, "summary": "..." },
      "filters": { "passed": true, "summary": "..." },
      "aggregation_grain": { "passed": true, "summary": "..." }
    },
    "issues": []
  },
  "compare_sql": {
    "required": true,
    "executed": true,
    "passed": true,
    "scenarios_total": 3,
    "scenarios_passed": 3,
    "failed_scenarios": []
  },
  "warnings": [],
  "errors": []
}
```

Do not invent fields. If `refactor write` rejects the payload, fix the payload and retry.

## Implementation

### Step 1: Run the guard

Run the readiness guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> refactor
```

If `ready` is `false`, stop and report the returned `code` and `reason`.

### Step 2: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context \
  --table <table_fqn>
```

If `refactor context` fails because a prerequisite is missing, return `status: "error"` with
code `CONTEXT_PREREQUISITE_MISSING`. If it fails due to IO or parse issues, return
`status: "error"` with code `CONTEXT_IO_ERROR`. Do not surface raw labels such as
`no_writer_configured` in the result JSON.

Read `object_type` from the output:

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
- `profile`, `columns`, `source_tables`, `test_spec`, `sandbox` — same as table path
- No `writer`, `proc_body`, or `statements` — absent for views
- Write-back via `refactor write` auto-detects the view and writes to the view catalog

### Step 3: Create staging files and launch extraction/refactor agents

Create the staging directory before either sub-agent writes to it:

```bash
mkdir -p .staging
```

Launch both sub-agents simultaneously. They must not see each other's output. Both agents use [references/routine-migration-ref.md](references/routine-migration-ref.md) for DML extraction and CTE restructuring rules. Substitute placeholders in the prompt templates with actual values from Step 1 context.

### Sub-agent A: Extract core SELECT

Launch a sub-agent using the prompt template in [references/sub-agent-prompts.md -- Sub-agent A](references/sub-agent-prompts.md).

- For tables: include `proc_body`, `statements`, and `columns`. If `writer_ddl_slice` is present, use it in place of the full `proc_body`.
- For views: include `view_sql` and `columns` in place of `proc_body`/`statements`.

The sub-agent writes the result to `.staging/<table_fqn>-extracted.sql`.

### Sub-agent B: Refactor into CTEs

Launch a sub-agent using the prompt template in [references/sub-agent-prompts.md -- Sub-agent B](references/sub-agent-prompts.md).

- For tables: include `proc_body`, `statements`, `columns`, `source_tables`, and `profile`. If `writer_ddl_slice` is present, use it in place of the full `proc_body`.
- For views: include `view_sql`, `columns`, `source_tables`, and `profile` in place of `proc_body`/`statements`.

The sub-agent writes the result to `.staging/<table_fqn>-refactored.sql`.

### Step 4: Run semantic review

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

### Step 5: Decide whether to run compare-sql

Use this order:

1. If the caller explicitly says to skip `compare-sql`, skip it.
2. Otherwise run:

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

### Step 6: Equivalence audit

When sandbox status succeeds and the caller did not say to skip `compare-sql`, run the comparison CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --sql-a-file .staging/<table_fqn>-extracted.sql \
  --sql-b-file .staging/<table_fqn>-refactored.sql \
  --spec test-specs/<table_fqn>.json > .staging/<table_fqn>-compare.json
```

The compare spec path is the on-disk test spec under `test-specs/<table_fqn>.json`. The `test_spec` object returned by `refactor context` is for inspection, not a replacement for the CLI `--spec` file argument.

Write the JSON output to `.staging/<table_fqn>-compare.json`.

Read the output JSON. For each scenario:

- `equivalent: true` — refactored CTE SQL produces identical rows to extracted core SELECT
- `equivalent: false` — `a_minus_b` shows rows in A (extracted) but not B (refactored), `b_minus_a` shows the reverse

### Self-correction loop (max 3 iterations)

If any scenario fails (`equivalent: false`):

1. Analyse the diff: which rows differ and why (missing join, wrong filter, dropped column, type mismatch)
2. Revise **only the refactored CTE SQL** (sub-agent B's output) to fix the semantic gap. The extracted SQL is the ground truth.
3. Rewrite `.staging/<table_fqn>-refactored.sql`
4. Rerun semantic review and write the updated result to `.staging/<table_fqn>-semantic-review.json`
5. Rerun `compare-sql`
6. Repeat up to 3 times total

After 3 failed iterations, proceed to Step 5 with the partial result.

### Step 7: Write to catalog

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

### Step 8: Clean up and report

Delete the staging directory after `refactor write` succeeds:

```bash
rm -rf .staging
```

Report:

1. Extracted core SELECT (sub-agent A output)
2. Refactored CTE SQL (sub-agent B output)
3. CTE structure summary (import CTEs, logical CTEs, final)
4. Semantic review verdict (passed/failed, any issues)
5. Equivalence audit results (per-scenario pass/fail, or skipped with reason)
6. Final persisted status

## Common Mistakes

- Writing to `.staging` before creating it. Create the directory first.
- Editing extracted SQL during self-correction. Only refactored SQL may change.
- Comparing refactored SQL to dbt expectations. Compare extracted SQL to refactored SQL.
- Blocking on sandbox access when semantic-review-only fallback is allowed.
- Inventing surfaced error codes or result fields outside the canonical `/refactor` schema.

## References

- [references/routine-migration-ref.md](references/routine-migration-ref.md) — dialect-routed DML extraction and CTE restructuring rules
- [references/sub-agent-prompts.md](references/sub-agent-prompts.md) — Prompt templates for sub-agent A (extract core SELECT) and sub-agent B (refactor into CTEs)

## Schema discipline

Use the canonical `/refactor` surfaced code list in [`../../lib/shared/refactor_error_codes.md`](../../lib/shared/refactor_error_codes.md).

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Return `status: "error"` with code `CONTEXT_PREREQUISITE_MISSING` and mention the missing prerequisite in the message |
| `refactor context` | 2 | IO/parse error. Return `status: "error"` with code `CONTEXT_IO_ERROR` and surface the error message |
| `refactor write` | 1 | Validation failure. Fix payload and retry once |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | One or more scenarios failed. Enter self-correction loop |
