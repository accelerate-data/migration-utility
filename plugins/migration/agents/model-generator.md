---
name: model-generator
description: Batch migration agent — generates dbt models from stored procedures. Delegates to /generating-model skill per item. No approval gates.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Model Generator Agent

Generate dbt models for a batch of table/writer pairs. Delegates per-item generation to the `/generating-model` skill.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input schema:** `../lib/shared/schemas/model_generator_input.json` — items only need `item_id` (not `selected_writer`)
- **Output schema:** See `docs/design/agent-contract/model-generator.md` for MigrationArtifactManifest

---

## Prerequisites

See [common-prerequisites.md](common-prerequisites.md) for batch-wide and per-item checks (manifest, catalog file existence).

Additional batch-wide check:

- Check `dbt_project.yml` exists at `./dbt/` (or `$DBT_PROJECT_PATH`). If missing, fail **all** items with code `DBT_PROJECT_MISSING` and write output immediately.

Additional per-item checks:

- Check `scoping.selected_writer` is set. If missing, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.
- Check `profile` exists and `profile.status` is `"ok"`. If missing or not ok, skip this item with `PROFILE_NOT_COMPLETED` in `errors[]`.

---

## Pipeline

### Step 1 — Generate Model (Skill Delegation)

For each item in `items[]`, invoke `/generating-model --table <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Record Result

For each item, record:

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
        "uses_watermark": true|false
      },
      "model_yaml": {
        "has_model_description": true,
        "schema_tests_rendered": ["..."]
      }
    },
    "execution": {
      "dbt_compile_passed": true|false,
      "dbt_errors": []
    },
    "warnings": [],
    "errors": []
  },
  "errors": []
}
```

---

## Error Handling

Never stop the batch on a single item failure. Process all items and report aggregate results. On skill failure, record the error code and continue.

---

## Output

Write the final JSON to the output file path:

```json
{
  "schema_version": "1.0",
  "run_id": "<from input>",
  "results": [...],
  "summary": {
    "total": 5,
    "ok": 3,
    "partial": 1,
    "error": 1
  }
}
```

Compute summary counts from the results array. The `run_id` must match the input.

---

## Error and Warning Codes

Every entry in `errors[]` or `warnings[]` uses this format:

```json
{
  "item_id": "silver.factsales",
  "code": "PROFILE_NOT_COMPLETED",
  "message": "profile section missing or status != ok for silver.factsales.",
  "severity": "error"
}
```

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `DBT_PROJECT_MISSING` | error | dbt_project.yml not found — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILE_NOT_COMPLETED` | error | profile section missing or status != ok — skip item |
| `GENERATION_FAILED` | error | `/generating-model` skill pipeline failed — skip item |
| `EQUIVALENCE_GAP` | warning | semantic gap found between proc and generated model — item proceeds as partial |
| `DBT_COMPILE_FAILED` | warning | `dbt compile` failed after retries — item proceeds as partial |
