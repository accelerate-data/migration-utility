---
name: refactoring-sql
description: >
  Refactors raw T-SQL stored procedure SQL into an import/logical/final CTE
  pattern. Invokes /extracting-sql and /restructuring-sql in parallel as isolated
  forked sub-skills, then proves equivalence via sandbox execution. Invoke when
  the user asks to "refactor SQL", "restructure to CTEs", or "prepare SQL for
  migration".
user-invocable: true
argument-hint: "<schema.table>"
---

# Refactoring SQL

Restructure a stored procedure's SQL into import/logical/final CTEs while proving the refactored SQL produces identical results. Delegates to two forked sub-skills that produce independent outputs, then compares them in the sandbox. The output stays in T-SQL — dbt Jinja conversion happens in the downstream `generating-model` skill.

## Arguments

`$ARGUMENTS` is the fully-qualified table name. Ask the user if missing. The writer is read from the catalog scoping section (`catalog/tables/<table>.json` -> `scoping.selected_writer`).

## Before invoking

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <table_fqn> refactor
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Step 1: Prepare staging

```bash
mkdir -p .staging
```

## Step 2: Invoke sub-skills in parallel

Invoke `/extracting-sql $ARGUMENTS` and `/restructuring-sql $ARGUMENTS` in parallel. The `context: fork` on each skill runs them in isolated contexts with no shared conversation history — this is required so the equivalence comparison is meaningful.

Wait for both to complete. Each sub-skill assembles its own context and writes its output:

- `.staging/extracted.sql` — core SELECT produced by `/extracting-sql`
- `.staging/refactored.sql` — CTE-structured SELECT produced by `/restructuring-sql`

## Step 3: Equivalence audit

After both sub-agents complete, run the comparison CLI which seeds fixtures, executes both SELECTs, and returns the difference in rows:

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
2. Revise **only the refactored CTE SQL** (`.staging/refactored.sql`) to fix the semantic gap. The extracted SQL (`.staging/extracted.sql`) is the ground truth -- never modify it.
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

1. The extracted core SELECT (`.staging/extracted.sql`)
2. The refactored CTE SQL (`.staging/refactored.sql`)
3. CTE structure summary (import CTEs, logical CTEs, final)
4. Equivalence audit results (per-scenario pass/fail)
5. Any remaining diffs if status is partial

## References

- [references/sp-migration-ref.md](references/sp-migration-ref.md) — DML extraction rules per statement type (INSERT, MERGE, UPDATE, etc.) and CTE restructuring patterns; used by `/extracting-sql` and `/restructuring-sql`

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `refactor context` | 1 | Missing catalog/profile/test-spec. Tell user which prerequisite is missing |
| `refactor context` | 2 | IO/parse error. Surface the error message |
| `refactor write` | 1 | Validation failure. Tell user to check the SQL |
| `refactor write` | 2 | IO error. Surface the error message |
| `test-harness compare-sql` | 1 | All scenarios failed. Enter self-correction loop |
