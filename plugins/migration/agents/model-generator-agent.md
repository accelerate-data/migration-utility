---
name: model-generator-agent
description: Batch migration agent — generates dbt models from stored procedures using profile, resolved statements, and LLM generation. No approval gates.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Model Generator Agent

Generate dbt models for a batch of table/writer pairs. Delegates dbt generation to the `/generating-model` skill per item, running in batch mode without approval gates.

Use `uv run migrate` directly for context assembly. The `/generating-model` skill handles generation, validation, and artifact writes.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input schema:** `../lib/shared/schemas/model_generator_input.json` — items only need `item_id` (not `selected_writer`)
- **Output schema:** See `docs/design/agent-contract/model-generator-agent.md` for MigrationArtifactManifest

---

## Prerequisites

Before processing items:

1. Read `manifest.json` from the current working directory for `technology` and `dialect`. If missing or unreadable, fail **all** items with code `MANIFEST_NOT_FOUND` and write output immediately.
2. Check `dbt_project.yml` exists at `./dbt/` (or `$DBT_PROJECT_PATH`). If missing, fail **all** items with code `DBT_PROJECT_MISSING` and write output immediately.

Per item, before Step 1:

- Check `catalog/tables/<item_id>.json` exists. If missing, skip this item with `CATALOG_FILE_MISSING` in `errors[]`.
- Check `scoping.selected_writer` is set. If missing, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.
- Check `profile` exists and `profile.status` is `"ok"`. If missing or not ok, skip this item with `PROFILE_NOT_COMPLETED` in `errors[]`.

---

## Pipeline

### Step 1 — Assemble Context

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" migrate context \
  --table <item_id>
```

The command reads `selected_writer` from the catalog scoping section — no `--writer` argument needed.

Parse the JSON output. If the command fails (exit code 1 or 2), record `status: "error"` with the error message and continue to the next item.

### Step 2 — Generate, Validate, and Write (Skill Delegation)

Follow the `/generating-model` skill pipeline (Steps 2–8) for this item. Skip Step 1 of the skill (context assembly) — the agent already performed it above. The skill defines the full dbt generation algorithm: model structure decisions, CTE generation, equivalence checks, schema YAML, artifact writes, and `dbt compile` validation.

**Batch overrides — do not use `AskUserQuestion`:**

- Make all model structure and materialization decisions deterministically
- Accept equivalence warnings and proceed (record as `EQUIVALENCE_GAP` warning)
- Auto-approve all artifacts — write directly without confirmation
- On `dbt compile` failure: attempt fix (max 2 attempts), then record as `DBT_COMPILE_FAILED` and continue

If any step in the skill pipeline fails, record `status: "error"` with the appropriate error code and continue to the next item.

### Step 3 — Record Result

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

| Situation | Action |
|---|---|
| `migrate context` fails | `status: "error"`, record error, skip to next item |
| LLM generation produces empty SQL | `status: "error"`, record error, skip |
| Equivalence check finds gaps | `status: "partial"` after revision attempts, record warnings |
| `migrate write` fails | `status: "error"`, record error, skip |
| `dbt compile` fails after retries | `status: "partial"`, record compile errors |

Never stop the batch on a single item failure. Process all items and report aggregate results.

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
| `CONTEXT_ASSEMBLY_FAILED` | error | `migrate context` CLI failed — skip item |
| `MODEL_GENERATION_FAILED` | error | LLM produced empty or invalid model SQL — skip item |
| `EQUIVALENCE_GAP` | warning | semantic gap found between proc and generated model — item proceeds as partial |
| `ARTIFACT_WRITE_FAILED` | error | `migrate write` CLI failed — skip item |
| `DBT_COMPILE_FAILED` | warning | `dbt compile` failed after retries — item proceeds as partial |
