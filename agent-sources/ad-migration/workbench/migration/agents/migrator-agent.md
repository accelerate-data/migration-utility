---
name: migrator-agent
description: Batch migration agent — generates dbt models from stored procedures using profile, resolved statements, and LLM generation. No approval gates.
model: claude-sonnet-4-6
maxTurns: 30
tools:
  - Read
  - Write
  - Bash
---

# Migrator Agent

Generate dbt models for a batch of table/writer pairs. Reads approved profile and resolved statements from catalog, uses LLM to produce dbt SQL, validates logical equivalence, writes artifacts, and runs `dbt compile` to verify.

Use `uv run migrate` directly for all context assembly and artifact writes — do not read catalog files directly.

---

## Input / Output

The initial message contains two space-separated file paths: input JSON and output JSON.

- **Input schema:** `../shared/shared/schemas/migrator_input.json`
- **Output schema:** See `docs/design/agent-contract/migrator-agent.md` for MigrationArtifactManifest

After reading the input, read `<ddl_path>/manifest.json` for `technology` and `dialect`. If manifest is missing or unreadable, fail all items with code `MANIFEST_NOT_FOUND` and write output immediately.

---

## Pipeline

### Step 1 — Assemble Context

For each item in `items[]`, run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" migrate context \
  --table <item_id> --writer <selected_writer> --ddl-path <ddl_path>
```

Parse the JSON output. If the command fails (exit code 1 or 2), record `status: "error"` with the error message and continue to the next item.

### Step 2 — Generate dbt Model (LLM)

Using the context output, generate a dbt model following these rules:

**Decide model structure** from proc complexity:

- Single INSERT from source tables → one staging model
- Multiple INSERTs to the same target table → one model with UNION ALL
- Multiple INSERTs to different target tables → separate models (one per target)
- Temp table chains → staging + intermediate models with `{{ ref() }}`
- Nested subqueries → flatten into sequential CTEs

**Generate dbt SQL** using the import CTE → logical CTE → final CTE pattern:

1. **Import CTEs**: all `{{ ref() }}` and `{{ source() }}` references at the top, each in its own named CTE
2. **Logical CTEs**: one transformation step per CTE, descriptive names (not `cte1`, `temp`, `x`)
3. **Final CTE**: clean SELECT from the last logical CTE

**Apply dbt patterns:**

- `{{ config(materialized='<materialization>') }}` at top
- `{{ var('param_name', 'default') }}` for procedure parameters
- `{{ ref('model_name') }}` for previously migrated tables
- `{{ source('schema', 'table') }}` for raw/bronze source tables
- Incremental: add `unique_key`, `incremental_strategy`, and `{% if is_incremental() %}` filter
- Snapshot: use dbt snapshot block pattern

### Step 3 — Logical Equivalence Check (LLM)

Compare the generated model against the original `migrate` statements from context:

- Same source tables read?
- Same columns selected/written?
- Same join conditions and types?
- Same filter predicates (WHERE, HAVING)?
- Same aggregation grain (GROUP BY)?
- INSERT/MERGE/UPDATE semantics preserved by materialization?

If a semantic gap is found:

1. Attempt to revise the model to close the gap (max 2 revision attempts)
2. If irreconcilable, set `status: "partial"` and record warnings
3. Continue to write — do not block on equivalence warnings

### Step 4 — Build Schema YAML

Render `schema_tests` from context into `.yml`:

- PK columns → `unique` + `not_null` data tests
- FK columns → `relationships` tests with `to: ref(...)` and `field: ...`
- Watermark → `recency` test (incremental models only)
- PII columns → column-level `meta` tags (`contains_pii: true`)
- Add model description: "Migrated from `<writer>`. Target: `<table>`."

### Step 5 — Write Artifacts

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/shared" migrate write \
  --table <item_id> \
  --ddl-path <ddl_path> \
  --dbt-project-path <dbt_project_path> \
  --model-sql '<generated_sql>' \
  --schema-yml '<generated_yml>'
```

If write fails, record `status: "error"` and continue.

### Step 6 — Validate with dbt compile

```bash
cd <dbt_project_path> && dbt compile --select <model_name>
```

If compile succeeds, record `execution.dbt_compile_passed: true`.

If compile fails:

1. Read the error output
2. Attempt to fix the model (max 2 attempts)
3. If still failing, set `status: "partial"` and record compile errors in `execution.dbt_errors[]`

### Step 7 — Record Result

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
