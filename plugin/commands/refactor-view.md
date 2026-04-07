---
name: refactor-view
description: >
  View refactoring command. Converts SQL Server views to ephemeral dbt staging
  models (stg_<view_name>.sql). Resolves dependent views in topological order,
  runs an equivalence check against catalog column metadata, then compiles and
  tests the generated model.
user-invocable: true
argument-hint: "<schema.view_name>"
---

# Refactor View

Convert a SQL Server view to an ephemeral dbt staging model with an import/logical/final CTE structure. Processes dependent views first in dependency order. Checks equivalence by comparing catalog column metadata against the generated model's SELECT list.

## Guards

- `manifest.json` must exist. If missing, fail with `MANIFEST_NOT_FOUND`.
- `dbt_project.yml` must exist in the dbt project. If missing, fail with `DBT_PROJECT_MISSING` and tell user to run `/init-dbt`.
- View catalog file `catalog/views/<schema.view_name>.json` must exist. If missing, fail with `VIEW_CATALOG_MISSING` and tell user to run `/setup-ddl` first.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. After Step 2 (dependency resolution), create one task per view in the full resolved dependency set (including any that failed resolution) with status `pending`. Views that failed dependency resolution should be immediately updated to `cancelled` with the error code. For the remaining views, update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) when it finishes.

## Pipeline

### Step 1 — Setup

1. Parse `$ARGUMENTS` as `<schema.view_name>`. If missing or malformed, ask the user.
2. Generate run slug: `refactor-view-<schema>-<view_name>` (lowercase, dots → hyphens, at most 40 characters).
3. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns `"main"`: proceed without a branch or worktree. All file writes and git operations target the current directory.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run.
4. Generate a run epoch: seconds since Unix epoch. All run artifacts use this as a filename suffix.

### Step 2 — Dependency resolution

Read the view catalog at `catalog/views/<schema.view_name>.json`.

Walk `references.views.in_scope` recursively to build the full transitive dependency set:

1. Start with the requested view as the root.
2. For each view in `references.views.in_scope`, read its catalog file and add its view dependencies to the set.
3. Continue until no new views are found. Circular dependencies are impossible (SQL Server rejects them at CREATE VIEW time).

If any dependent view's catalog file is missing, fail that item with `VIEW_CATALOG_MISSING` and report which views need `/setup-ddl` re-run.

Topological sort the dependency set: leaf views (no `references.views.in_scope` entries) are processed first, the requested view last.

### Step 3 — Refactor per view (in dependency order)

**Single-view path (only the requested view, no dependencies):** Run `/refactoring-view` directly in the current conversation — do not launch a sub-agent.

**Multi-view path (2+ views):** Launch one sub-agent per view in parallel where dependency order permits (views with no remaining unprocessed dependencies can run in parallel). Each sub-agent receives:

```text
Run the /refactoring-view skill for <schema.view_name>.
The worktree is at <worktree-path>.
Write the item result JSON to .migration-runs/<schema.view_name>.<epoch>.json.

After writing the result:
- If status == "error": do not write the stg_ file.
- If status != "error": invoke the /commit command with dbt/models/staging/stg_<view_name>.sql

On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 4 — Equivalence check (per view)

After each view is successfully refactored:

1. Load `catalog/views/<schema.view_name>.json` → read `columns[]` (the resolved column list from `sys.columns`).
2. Parse the generated `dbt/models/staging/stg_<view_name>.sql` and extract the final SELECT list column names (from the `final` CTE or the last SELECT).
3. For each column in `catalog.columns`, check whether a column with the same name (case-insensitive) appears in the generated model's SELECT list.
4. Emit a warning (do not block writing) for any catalog column not found in the generated model:

   ```text
   WARNING: column '<col>' from original view is missing from stg_<view_name>.sql
   ```

### Step 5 — dbt compile + test

After all views in the dependency set are successfully written:

```bash
dbt compile --select stg_<view_name>
dbt test --select stg_<view_name>
```

Run for the originally requested view. If dependent views were generated, also compile them:

```bash
dbt compile --select stg_<dep_view_name>
```

Report any compile or test failures as errors.

### Step 6 — Summarize

1. Read each `.migration-runs/<schema.view_name>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   refactor-view complete -- N views processed

     ok  silver.vw_customer_dim    3 CTEs, equivalence check passed
     ~   silver.vw_product_base    partial (1 column missing: legacy_code)
     x   silver.vw_old_data        error (VIEW_CATALOG_MISSING)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. Ask the user:

   > All successful views have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr refactor-view <comma-separated list of successfully processed views>`.
   After the PR is created or updated, tell the user the PR URL, branch, and worktree path.

6. Suggest running `/status` to check overall migration readiness.

## Item Result Schema

```json
{
  "item_id": "<schema.view_name>",
  "status": "ok|partial|error",
  "output": {
    "cte_count": 3,
    "import_ctes": ["source_customers"],
    "logical_ctes": ["customers_filtered"],
    "final_cte": "final",
    "columns_checked": 5,
    "columns_missing": 0,
    "dbt_compile": "passed",
    "dbt_test": "passed"
  },
  "warnings": [],
  "errors": []
}
```

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing |
| `DBT_PROJECT_MISSING` | error | dbt_project.yml not found |
| `VIEW_CATALOG_MISSING` | error | catalog/views/<fqn>.json not found |
| `VIEW_REFACTOR_FAILED` | error | refactoring skill pipeline failed |
| `DBT_COMPILE_FAILED` | error | dbt compile returned non-zero exit |
| `DBT_TEST_FAILED` | error | dbt test returned non-zero exit |
| `COLUMN_MISSING` | warning | column from original view not found in generated model |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "COLUMN_MISSING", "message": "Column 'legacy_code' from original view missing from stg_vw_product_base.sql", "item_id": "silver.vw_product_base", "severity": "warning"}
```
