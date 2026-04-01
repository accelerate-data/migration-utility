---
name: generating-model
description: >
  Batch model generation command — generates dbt models from stored procedures.
  Delegates per-item generation to the /generating-model skill. No approval gates.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generating Model

Generate dbt models for a batch of table/writer pairs. Delegates per-item generation to the `/generating-model` skill.

## Additional Batch-wide Guard

Before processing any items (after common guards):

- Check `dbt_project.yml` exists at `./dbt/` (or `$DBT_PROJECT_PATH`). If missing, fail **all** items with code `DBT_PROJECT_MISSING` and write output immediately.

## Additional Per-item Guards

Before running the skill for each item (after common guards):

- Check `scoping.selected_writer` is set. If missing, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.
- Check `profile` exists and `profile.status` is `"ok"`. If missing or not ok, skip this item with `PROFILE_NOT_COMPLETED` in `errors[]`.

## Pipeline

### Step 1 — Generate Model (Skill Delegation)

For each item, invoke `/generating-model <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Record Result

Write the item result to `.migration-runs/<item_id>.json`:

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
      "dbt_compile_passed": true,
      "dbt_errors": []
    },
    "warnings": [],
    "errors": []
  }
}
```

## Review Loops (placeholder)

Future implementation will add:

- **Code-reviewer sub-agent loop** — max 2 review iterations per item.
- **dbt self-correction bound** — max 3 compile-fix iterations per item.

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
