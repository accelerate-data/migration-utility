---
name: reviewing-model
description: >
  Reviews generated dbt model for standards compliance, correctness relative
  to the original proc, and test integration. Invoked by the /generate-model
  command after dbt tests pass, not directly by FDE.
user-invocable: false
argument-hint: "<schema.table>"
---

# Review Model

Quality gate for model generation output. Reviews the generated dbt model SQL and schema YAML for standards compliance, correctness against the original proc context, and test integration with the approved test spec. Issues a verdict: approve or kick back with specific fixes.

## Arguments

`$ARGUMENTS` is the fully-qualified table name (the `item_id`). Ask the caller if missing.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, stop and tell the caller that the project is not initialized.
2. Confirm the generated model SQL exists at the expected path (e.g., `dbt/models/staging/stg_<table>.sql`). If missing, stop and report: "No generated model found for `<item_id>`."
3. Confirm `test-specs/<item_id>.json` exists. If missing, stop and report: "No test spec found for `<item_id>`."

## Step 1: Assemble context

Run the deterministic context assembly CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id>
```

Read the output JSON. It contains:

- `proc_body` — full original procedure SQL
- `statements` — resolved statement list with action and SQL
- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived materialization
- `columns` — target table column list
- `source_tables` — tables read by the writer

Also read:

- The generated model SQL and schema YAML from the dbt project.
- The approved test spec from `test-specs/<item_id>.json`.

## Step 2: Check standards

| Check | What to verify |
|---|---|
| CTE pattern | Import CTE -> logical CTE -> final CTE structure followed |
| Import CTEs | All sources use `{{ source() }}` or `{{ ref() }}` — no hardcoded table names |
| Naming | Model name matches convention (`stg_<table>`), CTE names are descriptive |
| Config block | `{{ config(materialized=...) }}` present and matches derived materialization |
| Column naming | snake_case, consistent with target table columns |
| Schema YAML | Model description, column descriptions, schema tests (PK, FK, recency, PII) all present |
| Unit tests | `unit_tests:` block present in schema YAML with all scenarios from test spec |

## Step 3: Check correctness

Compare the generated model against the original proc context:

| Check | What to verify |
|---|---|
| Source tables | All `source_tables` from context are referenced in the model |
| Column completeness | All target `columns` from context are selected in the final CTE |
| Join semantics | JOIN types and conditions match the original proc |
| Filter predicates | WHERE/HAVING clauses preserved |
| Aggregation | GROUP BY grain matches |
| MERGE semantics | Incremental config correctly implements MERGE logic |
| Materialization | Matches derived materialization from profile |

## Step 4: Check test integration

- Verify every `unit_tests[]` entry from the test spec is rendered in the schema YAML.
- Verify `given[].input` references match actual `source()` / `ref()` calls in the model.
- Verify no test scenarios were dropped or modified during rendering.
- Verify gap tests (`test_gap_*`) have reasonable expectations consistent with the model logic.

## Step 5: Verdict

| Condition | Action |
|---|---|
| All checks pass | **Approve** — set `status` to `approved` |
| Standards issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with specific fixes |
| Correctness issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with specific discrepancies |
| Test integration issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with missing/broken test references |
| Max review iterations reached (2) | **Approve with warnings** — set `status` to `approved_with_warnings`, flag remaining issues for human review |

After kicking back, the model-generator revises the model, re-runs `dbt test` to confirm unit tests still pass, and resubmits. Maximum review / model-generator iterations: 2 (configurable).

## Output schema (ModelReviewResult)

Emit the following JSON structure as the skill's output:

```json
{
  "item_id": "silver.dimproduct",
  "status": "approved|revision_requested|approved_with_warnings|error",
  "checks": {
    "standards": {
      "passed": true,
      "issues": []
    },
    "correctness": {
      "passed": false,
      "issues": [
        {
          "code": "MISSING_SOURCE_TABLE",
          "message": "bronze.product_category referenced in proc but not in model import CTEs",
          "severity": "error"
        }
      ]
    },
    "test_integration": {
      "passed": true,
      "issues": []
    }
  },
  "feedback_for_model_generator": [
    "Add import CTE for source('bronze', 'product_category') — proc reads from it via JOIN"
  ],
  "warnings": [],
  "errors": []
}
```

`checks.*.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema:

```json
{
  "code": "STABLE_MACHINE_READABLE_CODE",
  "message": "Human-readable description of the diagnostic.",
  "item_id": "silver.dimproduct",
  "severity": "error|warning",
  "details": {}
}
```

## Boundary rules

Reviewing-model must not:

- Modify model SQL or schema YAML files
- Run dbt tests or compile commands
- Generate or modify test fixtures
- Override profile decisions (classification, materialization, keys)
- Override ground truth (captured proc output is fact)

## Error handling

- `migrate context` exits 1 — a prerequisite is missing (no profile, no writer, no statements). Report which prerequisite is missing and set `status` to `error` with code `CONTEXT_PREREQUISITE_MISSING`.
- `migrate context` exits 2 — IO or parse error. Surface the CLI error message and set `status` to `error` with code `CONTEXT_IO_ERROR`.
- Generated model files missing — stop before review. Set `status` to `error` with code `MODEL_NOT_FOUND`.
