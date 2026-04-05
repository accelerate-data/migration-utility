# Skill: Reviewing Model

## Purpose

Quality gate for model generation output. Reviews the generated dbt model SQL and schema YAML for standards compliance, correctness against the original stored procedure, and test integration with the approved test spec. Issues a verdict: approve, kick back with specific fixes (including stable reference codes and severity tiers), or approve with warnings after max iterations. This skill does not modify any files -- it is a pure quality gate.

## Invocation

```text
/reviewing-model <schema.table>
```

Argument is the fully-qualified table name (the `item_id`). This skill is invoked by the `/generate-model` command after dbt tests pass, not directly by the user.

## Prerequisites

- `manifest.json` must exist in the project root.
- Generated model SQL must exist at the expected path (e.g., `dbt/models/staging/stg_<table>.sql` and/or `dbt/models/<layer>/<table>.sql`).
- `test-specs/<item_id>.json` must exist (the approved test spec).

## Pipeline

### 1. Assemble context

```bash
uv run --project <shared-path> migrate context --table <item_id>
```

Also reads:

- The generated model SQL and schema YAML from the dbt project
- The approved test spec from `test-specs/<item_id>.json`

Context includes `refactored_sql` but the reviewer does **not** use it. Ground truth is always the original proc (`proc_body`).

### 2. Check correctness

Compare the generated model against `proc_body` (the original proc DDL). This is the primary check.

| Check | What is verified |
|---|---|
| Source tables | All `source_tables` from context are referenced in the model |
| Column completeness | All target `columns` from context are selected in the final CTE |
| Join semantics | JOIN types and conditions match the original proc |
| Filter predicates | WHERE/HAVING clauses preserved |
| Aggregation | GROUP BY grain matches |
| MERGE semantics | Incremental config correctly implements MERGE logic |
| Materialization | Matches derived materialization from profile |

Both correctness and standards checks always run regardless of the other's result.

### 3. Check standards

Evaluate against reference files. Each issue must include a stable reference code (e.g., `SQL_001`, `CTE_002`).

| Area | Reference file | Key checks |
|---|---|---|
| SQL style | `sql-style.md` | Lowercase keywords (`SQL_001`), indentation (`SQL_002`), trailing commas (`SQL_003`), one column per line (`SQL_004`), table alias prefixes (`SQL_005`), no `SELECT *` in marts (`SQL_006`) |
| CTE structure | `cte-structure.md` | Import CTEs first (`CTE_001`), final CTE named `final` (`CTE_002`), `select * from final` last (`CTE_003`), single-purpose CTEs (`CTE_004`), no nested CTEs (`CTE_006`) |
| Model naming | `model-naming.md` | Correct layer prefix (`MDL_001`--`MDL_003`), `snake_case` names (`MDL_004`), `_dbt_run_id` present (`MDL_005`), `_loaded_at` rules (`MDL_006`, `MDL_007`), locked columns unchanged (`MDL_008`) |
| YAML style | `yaml-style.md` | `version: 2` at top (`YML_004`), model description present (`YML_002`), PK column descriptions present (`YML_003`), 2-space indentation (`YML_001`) |
| Modularity | `modularity.md` | No joins in staging (`MOD_001`), mart refs use `ref()` not `source()` (`MOD_002`), one staging model per source table (`MOD_003`), staging materialized as ephemeral (`MOD_004`), business logic in mart not staging (`MOD_005`) |

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

After kicking back, the model generator revises the model, re-runs `dbt test`, and resubmits. The resubmission must include an `acknowledgements` block mapping each feedback code to `fixed` or `ignored: <reason>`. Maximum review/model-generator iterations: 2.

## Feedback Tiers

| Tier | Action required |
|---|---|
| `error` | Must fix before approval |
| `warning` | Model-generator must respond with `fixed` or `ignored: <reason>` |
| `info` | Acknowledgement optional; ignoring is always acceptable |

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<item_id>.json` | Profile, scoping, columns for context assembly |
| `catalog/procedures/<writer>.json` | Writer procedure for context assembly |
| `dbt/models/staging/stg_<table>.sql` | Generated staging model SQL to review |
| `dbt/models/<layer>/<table>.sql` | Generated mart model SQL to review |
| `dbt/models/<layer>/<table>.yml` | Generated schema YAML to review |
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
    {
      "code": "SQL_001",
      "message": "Keywords should be lowercase — found uppercase SELECT on line 4",
      "severity": "error",
      "ack_required": true
    },
    {
      "code": "CTE_002",
      "message": "Final CTE must be named 'final' — currently named 'output'",
      "severity": "error",
      "ack_required": true
    },
    {
      "code": "SQL_013",
      "message": "Line 12 exceeds 80 characters",
      "severity": "info",
      "ack_required": false
    }
  ],
  "acknowledgements": {
    "SQL_001": "fixed",
    "CTE_002": "ignored: legacy naming convention required by downstream tool"
  },
  "warnings": [],
  "errors": []
}
```

### feedback_for_model_generator item schema

| Field | Type | Description |
|---|---|---|
| `code` | string | Stable reference code (e.g., `SQL_001`, `CTE_002`, `MISSING_SOURCE_TABLE`) |
| `message` | string | Human-readable description with specific location where possible |
| `severity` | string | Enum: `error`, `warning`, `info` |
| `ack_required` | boolean | `true` for `error` and `warning`; `false` for `info` |

### Acknowledgements protocol

`acknowledgements` is present on resubmission only -- a flat map of `{ "<code>": "fixed" | "ignored: <reason>" }`. The model-generator must acknowledge all `ack_required: true` items before resubmission.

### ModelReviewResult fields

| Field | Type | Description |
|---|---|---|
| `item_id` | string | Fully qualified table name |
| `status` | string | Enum: `approved`, `revision_requested`, `approved_with_warnings`, `error` |
| `checks` | object | Results for each check area |
| `feedback_for_model_generator` | object[] | Structured feedback with stable codes and severity tiers (only when `status` is `revision_requested`) |
| `acknowledgements` | object | Map of code to disposition (`fixed` or `ignored: <reason>`), present on resubmission only |
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
