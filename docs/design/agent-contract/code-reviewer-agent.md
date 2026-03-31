# Code Reviewer Agent Contract

The code reviewer agent is an LLM-based quality gate for migration output. It reviews the generated dbt model for standards compliance, idiomatic dbt patterns, naming conventions, and correctness relative to the original proc. It can kick back to the migrator with specific issues.

The code reviewer is always an agent — no interactive skill path. It runs after the migrator's build-and-test loop completes (all unit tests passing). It lives in the migration plugin alongside the migrator.

## Philosophy and Boundary

- Code reviewer owns standards enforcement and model quality assessment.
- Code reviewer reads the generated dbt artifacts (`.sql` + `.yml`), the original proc context (same `migrate context` output), and the approved test spec from `test-specs/<item_id>.json`.
- Code reviewer does not run tests — the migrator already passed all unit tests before the code reviewer is invoked.
- Code reviewer can request revisions by kicking back to the migrator with specific issues.
- Code reviewer does not modify files directly.

## Required Input

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "items": [
    {
      "item_id": "silver.dimproduct",
      "selected_writer": "dbo.usp_load_dimproduct",
      "model_sql_path": "dbt/models/staging/stg_dimproduct.sql",
      "schema_yml_path": "dbt/models/staging/stg_dimproduct.yml",
      "test_spec_path": "test-specs/silver.dimproduct.json"
    }
  ]
}
```

Project root is inferred from CWD.

## Review Strategy

### 1. ReadContext

- Read the generated model SQL and schema YAML.
- Read the original proc context via `uv run migrate context --table <item_id> --writer <selected_writer>`.
- Read the approved test spec from `test-specs/`.

### 2. CheckStandards (LLM)

| Check | What to verify |
|---|---|
| CTE pattern | Import CTE → logical CTE → final CTE structure followed |
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
| Standards issues found | Kick back to migrator with specific fixes |
| Correctness issues found | Kick back to migrator with specific discrepancies |
| Test integration issues found | Kick back to migrator with missing/broken test references |
| Max review iterations reached | Approve with warnings — flag for human review |

After kicking back, the migrator revises the model, re-runs `dbt test` to confirm unit tests still pass, and resubmits. Maximum review ↔ migrator iterations: 2 (configurable).

## Output Schema (CodeReviewResult)

```json
{
  "schema_version": "1.0",
  "run_id": "uuid",
  "results": [
    {
      "item_id": "silver.dimproduct",
      "status": "approved|revision_requested|error",
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
      "feedback_for_migrator": [
        "Add import CTE for source('bronze', 'product_category') — proc reads from it via JOIN"
      ],
      "warnings": [],
      "errors": []
    }
  ],
  "summary": {
    "total": 1,
    "approved": 0,
    "revision_requested": 1,
    "error": 0
  }
}
```

## Code Reviewer Boundary

Code reviewer must not:

- Modify model SQL or schema YAML files
- Run dbt tests or compile commands
- Generate or modify test fixtures
- Override profile decisions (classification, materialization, keys)
- Override ground truth (captured proc output is fact)

`checks.*.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in `docs/design/agent-contract/README.md`.
