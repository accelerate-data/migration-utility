# Skill: Reviewing Model

## Purpose

Quality gate for model generation output. Reviews the generated dbt model SQL and schema YAML for standards compliance, correctness against the original stored procedure, and test integration with the approved test spec. Issues a verdict: approve, kick back with specific fixes, or approve with warnings after max iterations. This skill does not modify any files -- it is a pure quality gate.

## Invocation

```text
/reviewing-model <schema.table>
```

Argument is the fully-qualified table name (the `item_id`). This skill is invoked by the `/generate-model` command after dbt tests pass, not directly by the user.

## Prerequisites

- `manifest.json` must exist in the project root.
- Generated model SQL must exist at the expected path (e.g., `dbt/models/staging/stg_<table>.sql`).
- `test-specs/<item_id>.json` must exist (the approved test spec).

## Pipeline

### 1. Assemble context

```bash
uv run --project <shared-path> migrate context --table <item_id>
```

Also reads:

- The generated model SQL and schema YAML from the dbt project
- The approved test spec from `test-specs/<item_id>.json`

### 2. Check standards

| Check | What is verified |
|---|---|
| CTE pattern | Import CTE -> logical CTE -> final CTE structure followed |
| Import CTEs | All sources use `{{ source() }}` or `{{ ref() }}` -- no hardcoded table names |
| Naming | Model name matches convention (`stg_<table>`), CTE names are descriptive |
| Config block | `{{ config(materialized=...) }}` present and matches derived materialization |
| Column naming | snake_case, consistent with target table columns |
| Schema YAML | Model description, column descriptions, schema tests (PK, FK, recency, PII) all present |
| Unit tests | `unit_tests:` block present in schema YAML with all scenarios from test spec |

### 3. Check correctness

| Check | What is verified |
|---|---|
| Source tables | All `source_tables` from context are referenced in the model |
| Column completeness | All target `columns` from context are selected in the final CTE |
| Join semantics | JOIN types and conditions match the original proc |
| Filter predicates | WHERE/HAVING clauses preserved |
| Aggregation | GROUP BY grain matches |
| MERGE semantics | Incremental config correctly implements MERGE logic |
| Materialization | Matches derived materialization from profile |

### 4. Check test integration

- Every `unit_tests[]` entry from the test spec is rendered in the schema YAML
- `given[].input` references match actual `source()` / `ref()` calls in the model
- No test scenarios were dropped or modified during rendering
- Gap tests (`test_gap_*`) have reasonable expectations consistent with model logic

### 5. Verdict

| Condition | Verdict | Status |
|---|---|---|
| All checks pass | Approve | `approved` |
| Standards issues found | Kick back | `revision_requested` |
| Correctness issues found | Kick back | `revision_requested` |
| Test integration issues found | Kick back | `revision_requested` |
| Max review iterations reached (2) | Approve with warnings | `approved_with_warnings` |

After kicking back, the model generator revises the model, re-runs `dbt test`, and resubmits. Maximum review/model-generator iterations: 2.

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<item_id>.json` | Profile, scoping, columns for context assembly |
| `catalog/procedures/<writer>.json` | Writer procedure for context assembly |
| `dbt/models/staging/stg_<table>.sql` | Generated model SQL to review |
| `dbt/models/staging/stg_<table>.yml` | Generated schema YAML to review |
| `test-specs/<item_id>.json` | Approved test spec for integration check |

## Writes

None. The reviewer emits a `ModelReviewResult` JSON structure as output but does not modify any files.

## JSON Format

### ModelReviewResult output

```json
{
  "item_id": "silver.dimproduct",
  "status": "revision_requested",
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
          "item_id": "silver.dimproduct",
          "severity": "error",
          "details": {}
        }
      ]
    },
    "test_integration": {
      "passed": true,
      "issues": []
    }
  },
  "feedback_for_model_generator": [
    "Add import CTE for source('bronze', 'product_category') -- proc reads from it via JOIN"
  ],
  "warnings": [],
  "errors": []
}
```

### ModelReviewResult fields

| Field | Type | Description |
|---|---|---|
| `item_id` | string | Fully qualified table name |
| `status` | string | Enum: `approved`, `revision_requested`, `approved_with_warnings`, `error` |
| `checks` | object | Results for each check area |
| `feedback_for_model_generator` | string[] | Specific fixes for the generator (only when `status` is `revision_requested`) |
| `warnings` | array | Diagnostics entries |
| `errors` | array | Diagnostics entries |

### `checks` object

| Field | Type | Description |
|---|---|---|
| `standards` | object | Standards compliance check result |
| `correctness` | object | Correctness vs source proc check result |
| `test_integration` | object | Test integration check result |

Each check area has the same structure:

| Field | Type | Description |
|---|---|---|
| `passed` | boolean | Whether this check area passed |
| `issues` | array | Diagnostics entries for issues found |

### Check area issue codes

**Standards:**

| Code | Description |
|---|---|
| `MISSING_CONFIG_BLOCK` | `{{ config() }}` not present |
| `WRONG_MATERIALIZATION` | Config materialization does not match derived materialization |
| `HARDCODED_TABLE_NAME` | Import CTE uses hardcoded table name instead of `source()` or `ref()` |
| `MISSING_SCHEMA_TESTS` | Required schema tests (PK, FK, recency, PII) not present |
| `MISSING_UNIT_TESTS_BLOCK` | `unit_tests:` block not present in schema YAML |
| `CTE_PATTERN_VIOLATION` | Model does not follow import/logical/final CTE pattern |

**Correctness:**

| Code | Description |
|---|---|
| `MISSING_SOURCE_TABLE` | Source table from proc not referenced in model |
| `MISSING_COLUMN` | Target column not in final SELECT |
| `JOIN_MISMATCH` | JOIN type or condition differs from original proc |
| `FILTER_MISMATCH` | WHERE/HAVING clause not preserved |
| `GRAIN_MISMATCH` | GROUP BY columns differ |
| `MERGE_SEMANTICS_LOST` | Incremental config does not implement original MERGE logic |

**Test integration:**

| Code | Description |
|---|---|
| `SCENARIO_DROPPED` | Test-spec scenario not rendered in schema YAML |
| `INPUT_REF_MISMATCH` | `given[].input` does not match a `source()` or `ref()` call in the model |
| `GAP_TEST_INCONSISTENT` | Gap test expectations inconsistent with model logic |

### Diagnostics schema

`checks.*.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema:

| Field | Type | Required | Description |
|---|---|---|---|
| `code` | string | yes | Stable machine-readable identifier |
| `message` | string | yes | Human-readable description |
| `item_id` | string | no | Fully qualified table name |
| `severity` | string | yes | Enum: `error`, `warning` |
| `details` | object | no | Optional structured context |

## Boundary Rules

The model reviewer must not:

- Modify model SQL or schema YAML files
- Run dbt tests or compile commands
- Generate or modify test fixtures
- Override profile decisions (classification, materialization, keys)
- Override ground truth (captured proc output is fact)

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `migrate context` exit code 1 | Prerequisite missing (no profile, no writer, no statements) | Run required upstream skills. Status set to `error` with code `CONTEXT_PREREQUISITE_MISSING` |
| `migrate context` exit code 2 | IO/parse error | Check catalog files. Status set to `error` with code `CONTEXT_IO_ERROR` |
| Generated model files missing | Model generator has not run or write failed | Run [[Skill Generating Model]] first. Status set to `error` with code `MODEL_NOT_FOUND` |
| Iteration 2 still has issues | Generator could not fully address reviewer feedback | Approved with warnings -- remaining issues flagged for human review |
