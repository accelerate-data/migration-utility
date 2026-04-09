---
name: refactoring-sql
description: >
  Refactors table-producing procedure SQL or view SQL into a pure T-SQL CTE
  query. Extract the table-producing SQL, restructure it, prove semantic
  equivalence, and persist the result through the shared refactor CLI.
user-invocable: true
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Refactoring SQL

Refactor one table or view into a pure T-SQL CTE query. Do four things:

1. extract the table-producing SQL
2. rewrite it into import/logical/final CTE form
3. prove equivalence with semantic review and, when available, executable compare
4. persist the refactor result to catalog

Stay in T-SQL. Do not generate dbt SQL here.

## Arguments

`$ARGUMENTS` is the fully qualified object name. Ask only if it is missing.

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

## Step 1 — Read deterministic context

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> refactor
```

If `ready` is `false`, stop and report the returned `code` and `reason`.

Then run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor context \
  --table <table_fqn>
```

Use the returned context as the source of truth.

Important fields:

- `writer`
- `proc_body`
- `writer_ddl_slice` for multi-table writers
- `view_sql` for views
- `statements`
- `columns`
- `source_tables`
- `test_spec`
- `sandbox`

Rules:

- If `writer_ddl_slice` is present, that is the SQL you are refactoring for this table.
- If `object_type` is `view` or `mv`, use `view_sql` as the ground truth SQL.
- Do not refactor the whole procedure body when the context already gives you the table-specific slice.

## Step 2 — Produce the two SQL artifacts

Create `.staging/` if needed.

Write these files:

- `.staging/<table_fqn>-extracted.sql`
- `.staging/<table_fqn>-refactored.sql`

Use two isolated sub-agents:

### Sub-agent A — extracted SQL

Task:

- derive one pure T-SQL `SELECT`
- return the rows and columns the procedure writes to the target table
- preserve T-SQL syntax
- never emit write keywords

Use:

- `writer_ddl_slice` when present
- otherwise the table-producing logic from `proc_body` plus `statements`

Return SQL only.

### Sub-agent B — refactored SQL

Task:

- rewrite the extracted logic into import/logical/final CTE form
- keep the same output semantics
- end with `select * from final`
- preserve T-SQL syntax
- never emit write keywords

If existing dbt staging or mart models are present on disk, you may use them as naming guidance only. Do not let them change the refactor semantics.

Return SQL only.

## Step 3 — Semantic review

Launch a third isolated sub-agent after both SQL files exist.

Inputs:

- extracted SQL
- refactored SQL
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
- use only these checks:
  - source tables
  - output columns
  - joins
  - filters
  - aggregation grain
- `issues[]` must use diagnostics-style entries
- if any check fails, `passed` must be `false`

Write the JSON to:

- `.staging/<table_fqn>-semantic-review.json`

## Step 4 — Executable compare

When sandbox status succeeds and the caller did not tell you to skip `compare-sql`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness compare-sql \
  --sql-a-file .staging/<table_fqn>-extracted.sql \
  --sql-b-file .staging/<table_fqn>-refactored.sql \
  --spec test-specs/<table_fqn>.json
```

Write the JSON output to:

- `.staging/<table_fqn>-compare.json`

If compare fails:

1. inspect the scenario diffs
2. revise only `.staging/<table_fqn>-refactored.sql`
3. rerun semantic review
4. rerun compare

Retry at most 3 times.

Do not modify the extracted SQL during this loop.

## Step 5 — Persist

Run `refactor write` with the evidence you produced.

When sandbox status succeeds and the caller did not tell you to skip `compare-sql`:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/<table_fqn>-extracted.sql \
  --refactored-sql-file .staging/<table_fqn>-refactored.sql \
  --semantic-review-file .staging/<table_fqn>-semantic-review.json \
  --compare-sql-file .staging/<table_fqn>-compare.json
```

When the caller told you to skip `compare-sql`, or sandbox status failed:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor write \
  --table <table_fqn> \
  --extracted-sql-file .staging/<table_fqn>-extracted.sql \
  --refactored-sql-file .staging/<table_fqn>-refactored.sql \
  --semantic-review-file .staging/<table_fqn>-semantic-review.json \
  --no-compare-required
```

The CLI derives the final status. Do not invent or override it in the skill.

## Step 6 — Clean up and report

Delete the staging files after `refactor write` succeeds.

Report briefly:

- extracted SQL written
- refactored SQL written
- semantic review verdict
- compare result when available
- final persisted status

## Error handling

- `refactor context` exit `1`: prerequisite missing, report the returned reason
- `refactor context` exit `2`: IO or parse error, surface it directly
- `test-harness compare-sql` failure: retry within the allowed loop, then persist `partial`
- `refactor write` exit `1`: payload or validation failure, fix and retry once
- `refactor write` exit `2`: IO failure, surface it directly

## References

- [references/sp-migration-ref.md](references/sp-migration-ref.md)
