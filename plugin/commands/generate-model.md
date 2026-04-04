---
name: generate-model
description: >
  Batch model generation command — generates dbt models from stored procedures.
  Delegates per-item generation to the /generating-model skill with
  /reviewing-model review loop.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Model

Generate dbt models for a batch of tables. Launches one sub-agent per table in parallel, each running `migration:generating-model`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- `dbt_project.yml` must exist at `./dbt/`. If missing, fail all items with `DBT_PROJECT_MISSING`.
- `dbt/profiles.yml` must exist. If missing, fail all items with `DBT_PROFILE_MISSING` and tell the user to run `/init-dbt`.
- `dbt debug` must show "Connection test: OK". If it fails, fail all items with `DBT_CONNECTION_FAILED` and tell the user to check credentials — for SQL Server: `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD` env vars; for other adapters: update `profiles.yml` placeholder values.
- Per item: `catalog/tables/<item_id>.json` must exist. If missing, skip with `CATALOG_FILE_MISSING`.
- Per item: `scoping.selected_writer` must be set. If missing, skip with `SCOPING_NOT_COMPLETED`.
- Per item: `profile` must exist with `status: "ok"`. If missing, skip with `PROFILE_NOT_COMPLETED`.
- Per item: `test-specs/<item_id>.json` must exist. If missing, skip with `TEST_SPEC_NOT_FOUND`.

## Pipeline

### Step 1 — Setup

1. Generate run slug: `generate-model-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters).
2. Check for existing worktrees. If any exist, list them as options alongside creating a new one and ask the user to pick:
   > 1. `feature/scope-silver-dimcustomer`
   > 2. `feature/profile-silver-dimcustomer`
   > 3. **New worktree**
   If none exist, create a new worktree and branch per `.claude/rules/git-workflow.md`.
3. Generate a run epoch: seconds since Unix epoch (e.g. `1743868200`). All run artifacts use this as a filename suffix.

### Step 2 — Run migration:generating-model per table

**Single-table path (1 table):** Run `migration:generating-model` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.<epoch>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the migration:generating-model skill for <schema.table>.
The worktree is at <worktree-path>.
Skip the Step 4 user confirmation prompt and the Step 6 approval prompt — proceed automatically. Still run the full equivalence analysis in Step 4.
Equivalence warnings: proceed and write the model. Record each gap as EQUIVALENCE_GAP warning.
dbt compile/test failure: attempt up to 3 self-corrections. If still failing, write as-is with DBT_TEST_FAILED warning.
Write the item result JSON to .migration-runs/<schema.table>.<epoch>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 3 — Review Model

For each item that completed Step 2 successfully (dbt tests passing), invoke `/reviewing-model --table <item_id>`.

- If verdict is `approved`: proceed to revert/summarize.
- If verdict is `revision_requested`: re-invoke `/generating-model` for the item with the reviewer's `feedback_for_model_generator` as additional context. The model-generator must re-run `dbt test` to confirm unit tests still pass after revisions. Then re-invoke `/reviewing-model`. Maximum 2 review iterations per item.
- On review failure or max iterations reached, approve with warnings and proceed.

### Step 4 — Revert errored items

For each item with `status: "error"`, revert any files the skill may have partially written:

```bash
git checkout -- dbt/models/staging/<model_name>.sql dbt/models/staging/_<model_name>.yml
```

Derive `<model_name>` from the item_id using the same `stg_<table>` convention. Ignore errors from `git checkout` (the files may not exist yet for new models — use `rm -f` instead if the model was newly created and has no prior version).

### Step 5 — Summarize

1. Read each `.migration-runs/<schema.table>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   generate-model complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (EQUIVALENCE_GAP)
     ✗ silver.DimDate        error (PROFILE_NOT_COMPLETED)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, skip commit/PR — report errors only and stop.
5. Ask the user: commit and push? Stage only files changed by successful items (model SQL and schema YAML files). Do not stage `.migration-runs/`. Check for an existing open PR on the branch via `gh pr list --head <slug> --state open --json number,url`. If one exists, update it with `gh pr edit` instead of creating a new PR. PR body format:

   ```markdown
   ## Model Generation — N tables

   | Table | Status | Model | Materialized | dbt compile |
   |---|---|---|---|---|
   | silver.DimCustomer | ok | stg_dimcustomer | incremental | passed |
   | silver.DimProduct | partial | stg_dimproduct | table | EQUIVALENCE_GAP |
   | silver.DimDate | error | — | — | PROFILE_NOT_COMPLETED |
   ```

6. After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <worktree-path>

   Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches.
   ```

## Item Result Schema

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "output": {
    "table_ref": "<table_fqn>",
    "model_name": "<model_name>",
    "artifact_paths": {
      "model_sql": "models/staging/<model_name>.sql",
      "model_yaml": "models/staging/_<model_name>.yml"
    },
    "generated": {
      "model_sql": {
        "materialized": "<materialization>",
        "uses_watermark": true
      },
      "model_yaml": {
        "has_model_description": true,
        "schema_tests_rendered": ["..."],
        "has_unit_tests": true
      }
    },
    "execution": {
      "dbt_compile_passed": true,
      "dbt_test_passed": true,
      "self_correction_iterations": 0,
      "dbt_errors": []
    },
    "review": {
      "iterations": 1,
      "verdict": "approved|approved_with_warnings"
    },
    "warnings": [],
    "errors": []
  }
}
```

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `DBT_PROJECT_MISSING` | error | dbt_project.yml not found — all items fail |
| `DBT_PROFILE_MISSING` | error | dbt/profiles.yml not found — run `/init-dbt` — all items fail |
| `DBT_CONNECTION_FAILED` | error | `dbt debug` connection test failed — check credentials — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILE_NOT_COMPLETED` | error | profile section missing or status != ok — skip item |
| `TEST_SPEC_NOT_FOUND` | error | test-specs/\<item_id>.json not found — skip item |
| `GENERATION_FAILED` | error | `/generating-model` skill pipeline failed — skip item |
| `EQUIVALENCE_GAP` | warning | semantic gap found between proc and generated model — item proceeds as partial |
| `DBT_COMPILE_FAILED` | warning | `dbt compile` failed after retries — item proceeds as partial |
| `DBT_TEST_FAILED` | warning | `dbt test` failed after 3 self-correction iterations — item proceeds as partial |
| `REVIEW_KICKED_BACK` | warning | reviewer requested revision — item retried |
| `REVIEW_APPROVED_WITH_WARNINGS` | warning | reviewer approved with remaining issues after max iterations — item proceeds |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "EQUIVALENCE_GAP", "message": "Missing column 'legacy_flag' in generated model for silver.dimcustomer.", "item_id": "silver.dimcustomer", "severity": "warning"}
```
