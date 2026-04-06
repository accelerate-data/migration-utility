---
name: refactor
description: >
  SQL refactoring command. Restructures stored procedure SQL into CTE pattern
  with equivalence audit. Delegates per-table refactoring to the
  /refactoring-sql skill.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Refactor

Restructure stored procedure SQL into import/logical/final CTEs with a self-correcting audit loop proving equivalence. Launches one sub-agent per table in parallel, each running `/refactoring-sql`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- `manifest.json` must have `sandbox.database`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell user to run `/setup-sandbox`.
- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status`. If not found, fail all items with `SANDBOX_NOT_RUNNING`.

Per-item guards are checked by the skill via `migrate-util guard`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) when it finishes.

## Pipeline

### Step 1 -- Setup

1. Generate run slug: `refactor-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters).
2. Run the `git-checkpoints` skill with the run slug as the argument. If it returns a worktree path, use that path as the working directory for all file writes and git operations in this run.
   If not on `main` (git-checkpoints returns empty), check for existing worktrees. If any exist, list them as options alongside creating a new one and ask the user to pick. If none exist, create a new worktree and branch per `.claude/rules/git-workflow.md`.
3. Generate a run epoch: seconds since Unix epoch. All run artifacts use this as a filename suffix.

### Step 2 -- Refactor per table

**Single-table path (1 table):** Run `/refactoring-sql` directly in the current conversation -- do not launch a sub-agent. After the skill completes, write the item result JSON to `.migration-runs/<schema.table>.<epoch>.json`.

If the item status is `error`, immediately revert any catalog changes:

```bash
git checkout -- catalog/tables/<item_id>.json
```

If the item status is not `error`, auto-commit and push: run `/commit catalog/tables/<item_id>.json`.

Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the /refactoring-sql skill for <schema.table>.
The worktree is at <worktree-path>.
Write the item result JSON to .migration-runs/<schema.table>.<epoch>.json.

After writing the result:
- If status == "error": run `git checkout -- catalog/tables/<item_id>.json`.
- If status != "error": invoke the /commit command with catalog/tables/<item_id>.json

On failure, write result with status: "error" and error details, then revert as above.
Return the item result JSON.
```

The skill writes the refactored CTE SQL into the catalog `refactor` section.

### Step 3 -- Summarize

1. Read each `.migration-runs/<schema.table>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   refactor complete -- N tables processed

     ok  silver.DimCustomer    3 CTEs, all scenarios passed
     ~   silver.DimProduct     partial (2/5 scenarios passed)
     x   silver.DimDate        error (TEST_SPEC_NOT_FOUND)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. Ask the user:

   > All successful items have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr refactor <comma-separated list of successfully processed tables>`.
   After the PR is created or updated, tell the user the PR URL, branch, and worktree path.

6. Suggest running `/status` to see overall migration readiness across all tables.

## Item Result Schema

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "output": {
    "cte_count": 5,
    "import_ctes": ["source_customers", "dim_product"],
    "logical_ctes": ["customers_with_region", "filtered_customers"],
    "scenarios_total": 3,
    "scenarios_passed": 3,
    "iterations": 1
  },
  "warnings": [],
  "errors": []
}
```

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing -- all items fail |
| `SANDBOX_NOT_CONFIGURED` | error | manifest.json has no `sandbox.database` |
| `SANDBOX_NOT_RUNNING` | error | sandbox-status check failed |
| `CATALOG_FILE_MISSING` | error | catalog file not found -- skip item |
| `SCOPING_NOT_COMPLETED` | error | no selected_writer -- skip item |
| `PROFILE_NOT_COMPLETED` | error | profile missing or not ok -- skip item |
| `TEST_SPEC_NOT_FOUND` | error | test-specs file not found -- skip item |
| `REFACTOR_FAILED` | error | refactoring skill pipeline failed -- skip item |
| `EQUIVALENCE_PARTIAL` | warning | not all scenarios passed after max iterations |
| `COMPARE_SQL_FAILED` | warning | sandbox execution error during comparison |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "EQUIVALENCE_PARTIAL", "message": "2/5 scenarios failed for silver.dimproduct after 3 iterations.", "item_id": "silver.dimproduct", "severity": "warning"}
```
