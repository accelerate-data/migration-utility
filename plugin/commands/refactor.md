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

Restructure stored procedure or view SQL into import/logical/final CTEs with proof-backed equivalence.

Use these status meanings:

- `ok` — semantic review passed and executable `compare-sql` passed when compare was required
- `partial` — semantic review passed but executable compare was skipped or unavailable, or some proof gaps remain
- `error` — extraction, refactor, or required proof failed

## Compare behavior

When the caller explicitly says to skip sandbox `compare-sql`, or when `test-harness sandbox-status` fails, use semantic review only and persist `partial` unless executable proof is later provided.

This changes only the proof path. Keep the normal git/worktree, commit, and PR flow.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `/add-source-tables` to manage source tables.

Per-item readiness is checked by the skill via `migrate-util ready`.

**Sandbox hint:** Before processing any items, check sandbox availability:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status
```

If the sandbox is not found (`status: "not_found"` or non-zero exit), warn the user upfront:

> ⚠️ Sandbox not available — `compare-sql` proof will be skipped. All items will receive `partial` status (semantic review only). Run `/setup-sandbox` first if you want full `ok` proofs.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) when it finishes.

## Pipeline

### Step 1 -- Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `refactor-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `refactor-silver-facts`, `refactor-customer-procs`). The full slug (including the `refactor-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name (not a worktree path): proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. Generate a run ID once for the command run: `<epoch_ms>-<random_8hex>`. Use it as the artifact suffix for every file written by this run so concurrent runs against the same fixture do not collide.

### Step 2 -- Execute refactoring (plan-driven)

Create `.migration-runs/` first if it does not already exist.

**Single-object path (1 object):** Run `/refactoring-sql` directly in the current conversation. Do not launch a sub-agent. After the skill completes, write the item result JSON to `.migration-runs/<schema.object>.<run_id>.json`.

If the current persisted refactor for the item already has `status: ok` and the user did not explicitly ask for a rerun, do not re-run the skill. Write a skip result:

```json
{"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "proof_backed_refactor_already_present"}}
```

Determine the persisted catalog path before any git step:

- table objects: `catalog/procedures/<selected_writer>.json`
- view or materialized view objects: `catalog/views/<item_id>.json`

If the item status is `error`, immediately revert that persisted catalog file:

```bash
git checkout -- <persisted-catalog-path>
```

If the item status is not `error`, run `/commit <persisted-catalog-path>`.

Then continue to Step 3.

**Multi-object path (2+ objects):** Launch one sub-agent per item in parallel.

For each object:

- if the current persisted refactor already has `status: ok` and the user did not explicitly ask for a rerun, write the same skip result immediately
- otherwise spawn one sub-agent per object for `/refactoring-sql`

**Refactor agent prompt:**

```text
Run /refactoring-sql for <schema.object>.
The working directory is <working-directory>.
Write the item result JSON to .migration-runs/<schema.object>.<run_id>.json.

Create `.migration-runs/` first if it does not already exist.

After writing the result:
- Resolve the persisted catalog path first:
  - table: `catalog/procedures/<selected_writer>.json`
  - view or materialized view: `catalog/views/<item_id>.json`
- If status == "error": run `git checkout -- <persisted-catalog-path>`.
- If status != "error": invoke the /commit command with <persisted-catalog-path>

On failure, write result with status: "error" and error details, then revert as above.

Return the item result JSON.
```

The skill writes the extracted SQL, refactored SQL, semantic-review evidence, and compare summary into the persisted catalog `refactor` section.
If one table fails, continue processing the remaining tables and then write the summary.

### Step 3 -- Summarize

1. Read each `.migration-runs/<schema.table>.<run_id>.json`.
2. Write `.migration-runs/summary.<run_id>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   refactor complete -- N tables processed

     ok  silver.DimCustomer    compare-sql passed
     ~   silver.DimProduct     semantic review passed; compare-sql skipped
     x   silver.DimDate        error (TEST_SPEC_NOT_FOUND)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. Ask the user:

   > All successful items have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr refactor <comma-separated list of successfully processed tables>`.
   After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <working-directory>  (omit this line if on the default branch)
   ```

   If on a feature branch, also tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."

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

Use the canonical `/refactor` code list in [../lib/shared/refactor_error_codes.md](../lib/shared/refactor_error_codes.md).

Each entry in `errors[]` or `warnings[]` uses this shape:

```json
{"code": "EQUIVALENCE_PARTIAL", "message": "2/5 scenarios failed for silver.dimproduct after 3 iterations.", "item_id": "silver.dimproduct", "severity": "warning"}
```
