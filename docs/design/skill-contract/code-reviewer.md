# Code Reviewer Skill Contract

The code reviewer skill is an LLM-based quality gate for migration output. It reviews the generated dbt model for standards compliance, idiomatic dbt patterns, naming conventions, and correctness relative to the original proc. It can kick back to the model-generator with specific issues.

The code reviewer runs after the model-generator's build-and-test loop completes (all unit tests passing).

## Philosophy and Boundary

- Code reviewer owns standards enforcement and model quality assessment.
- Code reviewer reads the generated dbt artifacts (`.sql` + `.yml`), the original proc context (same `migrate context` output), and the approved test spec from `test-specs/<item_id>.json`.
- Code reviewer does not run tests — the model-generator already passed all unit tests before the code reviewer is invoked.
- Code reviewer can request revisions by kicking back to the model-generator with specific issues.
- Code reviewer does not modify files directly.

## Review Strategy

### 1. ReadContext

- Read the generated model SQL and schema YAML.
- Read the original proc context via `uv run migrate context --table <item_id> --writer <selected_writer>`.
- Read the approved test spec from `test-specs/`.

### 2. CheckStandards (LLM)

| Check | What to verify |
|---|---|
| CTE pattern | Import CTE -> logical CTE -> final CTE structure followed |
| Import CTEs | All sources use `{{ source() }}` or `{{ ref() }}` — no hardcoded table names |
| Naming | Model name matches convention (`stg_<table>`), CTE names are descriptive |
| Config block | `{{ config(materialized=...) }}` present and matches derived materialization |
| Column naming | snake_case, consistent with target table columns |
| Schema YAML | Model description, column descriptions, schema tests (PK, FK, recency, PII) all present |
| Unit tests | `unit_tests:` block present in schema YAML with all scenarios from test spec |

### 3. CheckCorrectness (LLM)

Compare the generated model against the original proc context:

| Check | What to verify |
|---|---|
| Source tables | All `source_tables` from context are referenced in the model |
| Column completeness | All target `columns` from context are selected in the final CTE |
| Join semantics | JOIN types and conditions match the original proc |
| Filter predicates | WHERE/HAVING clauses preserved |
| Aggregation | GROUP BY grain matches |
| MERGE semantics | Incremental config correctly implements MERGE logic |
| Materialization | Matches `derive_materialization()` output from profile |

### 4. CheckTestIntegration

- Verify every `unit_tests[]` entry from the test spec is rendered in the schema YAML.
- Verify `given[].input` references match actual `source()` / `ref()` calls in the model.
- Verify no test scenarios were dropped or modified during rendering.

### 5. Verdict

| Condition | Action |
|---|---|
| All checks pass | Approve — migration complete |
| Standards issues found | Kick back to model-generator with specific fixes |
| Correctness issues found | Kick back to model-generator with specific discrepancies |
| Test integration issues found | Kick back to model-generator with missing/broken test references |
| Max review iterations reached | Approve with warnings — flag for human review |

After kicking back, the model-generator revises the model, re-runs `dbt test` to confirm unit tests still pass, and resubmits. Maximum review / model-generator iterations: 2 (configurable).

## Output Schema (CodeReviewResult)

Per-item output:

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

## Code Reviewer Boundary

Code reviewer must not:

- Modify model SQL or schema YAML files
- Run dbt tests or compile commands
- Generate or modify test fixtures
- Override profile decisions (classification, materialization, keys)
- Override ground truth (captured proc output is fact)

`checks.*.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in `README.md`.
