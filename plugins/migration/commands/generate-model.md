---
name: generate-model
description: >
  Batch model generation command — generates dbt models from stored procedures.
  Delegates per-item generation to the /generating-model skill.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Model

Generate dbt models for a batch of tables. Launches one sub-agent per table in parallel, each running `migration:generating-model`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- `dbt_project.yml` must exist at `./dbt/`. If missing, fail all items with `DBT_PROJECT_MISSING`.
- Per item: `catalog/tables/<item_id>.json` must exist. If missing, skip with `CATALOG_FILE_MISSING`.
- Per item: `scoping.selected_writer` must be set. If missing, skip with `SCOPING_NOT_COMPLETED`.
- Per item: `profile` must exist with `status: "ok"`. If missing, skip with `PROFILE_NOT_COMPLETED`.

## Pipeline

### Step 1 — Setup

1. Read worktree base path from `.claude/rules/git-workflow.md`.
2. Generate run slug: `feature/generate-model-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters after `feature/`).
3. Create worktree: `mkdir -p <base>/feature && git worktree add <base>/<slug> -b <slug>`. If the worktree and branch already exist, reuse them — do not fail.
4. In the worktree, clear `.migration-runs/` and write `meta.json`:

```json
{
  "command": "generate-model",
  "tables": ["silver.DimCustomer", "silver.DimProduct"],
  "worktree": "../worktrees/feature/generate-model-silver-dimcustomer-silver-dimproduct",
  "started_at": "2026-04-01T12:00:00Z"
}
```

### Step 2 — Run migration:generating-model per table

Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the migration:generating-model skill for <schema.table>.
The worktree is at <worktree-path>.
Skip the Step 4 user confirmation prompt and the Step 6 approval prompt — proceed automatically. Still run the full equivalence analysis in Step 4.
Equivalence warnings: proceed and write the model. Record each gap as EQUIVALENCE_GAP warning.
dbt compile failure: attempt up to 3 self-corrections. If still failing, write as-is with DBT_COMPILE_FAILED warning.
Write the item result JSON to .migration-runs/<schema.table>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 3 — Summarize

1. Read each `.migration-runs/<schema.table>.json`.
2. Write `.migration-runs/summary.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   generate-model complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (EQUIVALENCE_GAP)
     ✗ silver.DimDate        error (PROFILE_NOT_COMPLETED)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, skip commit/PR — report errors only and stop.
5. Ask the user: commit and open PR? Stage only files changed by successful items (model SQL and schema YAML files). Do not stage `.migration-runs/`. PR body format:

   ```markdown
   ## Model Generation — N tables

   | Table | Status | Model | Materialized | dbt compile |
   |---|---|---|---|---|
   | silver.DimCustomer | ok | stg_dimcustomer | incremental | passed |
   | silver.DimProduct | partial | stg_dimproduct | table | EQUIVALENCE_GAP |
   | silver.DimDate | error | — | — | PROFILE_NOT_COMPLETED |
   ```

6. After the PR is created, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <worktree-path>

   Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches.
   ```

## Review Loops (placeholder)

Future implementation will add:

- **Code-reviewer sub-agent loop** — max 2 review iterations per item.
- **dbt self-correction bound** — max 3 compile-fix iterations per item.

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
        "schema_tests_rendered": ["..."]
      }
    },
    "execution": {
      "dbt_compile": "passed|parse_only|not_attempted|failed",
      "dbt_errors": []
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
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILE_NOT_COMPLETED` | error | profile section missing or status != ok — skip item |
| `GENERATION_FAILED` | error | `/generating-model` skill pipeline failed — skip item |
| `EQUIVALENCE_GAP` | warning | semantic gap found between proc and generated model — item proceeds as partial |
| `DBT_COMPILE_FAILED` | warning | `dbt compile` failed after retries — item proceeds as partial |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "EQUIVALENCE_GAP", "message": "Missing column 'legacy_flag' in generated model for silver.dimcustomer.", "item_id": "silver.dimcustomer", "severity": "warning"}
```
