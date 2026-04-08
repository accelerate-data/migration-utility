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

Restructure stored procedure or view SQL into import/logical/final CTEs with a self-correcting audit loop proving equivalence. For 2+ objects, runs a planning sweep that collects refactor status from the catalog, detects shared staging candidates, and presents a pre-flight table with skip/re-refactor/refactor recommendations. Execution is plan-driven with phase-based task tracking.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `/add-source-tables` to manage source tables.
- `manifest.json` must have `sandbox.database`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell user to run `/setup-sandbox`.
- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status`. If not found, fail all items with `SANDBOX_NOT_RUNNING`.

Per-item readiness is checked by the skill via `migrate-util ready`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track execution phases, not individual objects.

For **2+ objects**, create these tasks at command start:

| Task subject | Complete when |
|---|---|
| `sweep` | Plan artifact written and shared sources persisted |
| `pre-flight` | User confirms the plan |
| `execute: <description>` | That execution thread finishes (create when Claude decides each thread) |
| `summarize` | Summary written |

Thread task subjects describe the work: e.g. `execute: refactor silver.DimCustomer, silver.FactSales` or `execute: re-refactor silver.DimProduct`. Skipped objects are not tracked as tasks — they appear in the summary only.

For **single-object runs**: create one `execute: refactor <fqn>` task and one `summarize` task. No sweep or pre-flight tasks.

## Pipeline

### Step 1 -- Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `refactor-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `refactor-silver-facts`, `refactor-customer-procs`). The full slug (including the `refactor-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name (not a worktree path): proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. Generate a run epoch: seconds since Unix epoch. All run artifacts use this as a filename suffix.

### Step 1b -- Planning sweep (2+ objects only)

Run the sweep CLI to collect refactor status and dbt model existence for each object:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" refactor sweep \
  --tables <fqn1> --tables <fqn2> ... \
  --project-root <working-directory>
```

Read the output JSON. It contains per-object signals and a `shared_staging_candidates` list. Write the plan artifact to `.migration-runs/refactor-sweep.<epoch>.json`.

Present a pre-flight table to the user:

```text
Object                       Status    Staging  Mart   Action
───────────────────────────────────────────────────────────────
silver.DimCustomer           ok        1/1      yes    skip
silver.DimProduct            partial   0/2      no     re-refactor
silver.FactSales             —         1/3      no     refactor

Shared staging candidates: bronze.CustomerRaw (2 SPs)

Proceed? (y/n/edit)
```

- On `y`: proceed to Step 2.
- On `edit`: user specifies per-object action overrides (e.g. `silver.DimCustomer: refactor`). Update the plan artifact to reflect the override before proceeding.
- On `n`: abort the run.

### Step 2 -- Execute refactoring (plan-driven)

**Single-object path (1 object):** Run `/refactoring-sql` directly in the current conversation -- do not launch a sub-agent. After the skill completes, write the item result JSON to `.migration-runs/<schema.object>.<epoch>.json`.

If the item status is `error`, immediately revert any catalog changes:

```bash
git checkout -- catalog/tables/<item_id>.json
```

If the item status is not `error`, auto-commit and push: run `/commit catalog/tables/<item_id>.json`.

Then continue to Step 3.

**Multi-object path (2+ objects):** Read the sweep plan. For each object:

- `recommended_action: "skip"` — write a skip result immediately (no agent needed):

  ```json
  {"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "refactor.status=ok"}}
  ```

- `recommended_action: "re-refactor"` or `"refactor"` — spawn an agent for `/refactoring-sql`

Claude decides how many agents to spawn and which objects to group into execution threads, based on shared staging relationships. Objects that share staging candidates can run fully in parallel since their shared sources are already identified.

**Refactor agent prompt:**

```text
Run the /refactoring-sql skill for <schema.object>.
The working directory is <working-directory>.
Write the item result JSON to .migration-runs/<schema.object>.<epoch>.json.

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
   After the PR is created or updated, tell the user the PR URL and branch. If on a feature branch, also include the worktree path and tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."

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
