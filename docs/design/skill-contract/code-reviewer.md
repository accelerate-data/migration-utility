# Code Reviewer Skill Contract

The code reviewer skill is an LLM-based quality gate for migration output. It reviews the generated dbt model for standards compliance, idiomatic dbt patterns, naming conventions, and correctness relative to the original proc. It validates against the original `proc_body` (not `refactored_sql`, which is an intermediate artifact). It can kick back to the model-generator with structured feedback including stable reference codes and severity tiers.

The code reviewer runs after the model-generator's build-and-test loop completes (all unit tests passing).

## Philosophy and Boundary

- Code reviewer owns standards enforcement and model quality assessment.
- Code reviewer reads the generated dbt artifacts (`.sql` + `.yml`), the original proc context (same `migrate context` output), and the approved test spec from `test-specs/<item_id>.json`.
- Code reviewer validates against `proc_body` (original proc DDL) as ground truth. `refactored_sql` is present in context but not used by the reviewer.
- Code reviewer does not run tests — the model-generator already passed all unit tests before the code reviewer is invoked.
- Code reviewer can request revisions by kicking back to the model-generator with structured feedback objects that include stable reference codes and severity tiers.
- Code reviewer does not modify files directly.

## Review Strategy

### 1. ReadContext

- Read the generated model SQL and schema YAML.
- Read the original proc context via `uv run migrate context --table <item_id>`.
- Read the approved test spec from `test-specs/`.

### 2. CheckCorrectness (LLM)

Compare the generated model against `proc_body` (the original proc DDL). This is the primary check.

| Check | What to verify |
|---|---|
| Source tables | All `source_tables` from context are referenced in the model |
| Column completeness | All target `columns` from context are selected in the final CTE |
| Join semantics | JOIN types and conditions match the original proc |
| Filter predicates | WHERE/HAVING clauses preserved |
| Aggregation | GROUP BY grain matches |
| MERGE semantics | Incremental config correctly implements MERGE logic |
| Materialization | Matches derived materialization from profile |

Both CheckCorrectness and CheckStandards always run regardless of the other's result.

### 3. CheckStandards (LLM)

Evaluate against reference files. Each issue must include a stable reference code (e.g., `SQL_001`, `CTE_002`).

| Area | Reference file | Key checks |
|---|---|---|
| SQL style | `sql-style.md` | Lowercase keywords (`SQL_001`), indentation (`SQL_002`), trailing commas (`SQL_003`), one column per line (`SQL_004`), table alias prefixes (`SQL_005`), no `SELECT *` in marts (`SQL_006`) |
| CTE structure | `cte-structure.md` | Import CTEs first (`CTE_001`), final CTE named `final` (`CTE_002`), `select * from final` last (`CTE_003`), single-purpose CTEs (`CTE_004`), no nested CTEs (`CTE_006`) |
| Model naming | `model-naming.md` | Correct layer prefix (`MDL_001`–`MDL_003`), `snake_case` names (`MDL_004`), `_dbt_run_id` present (`MDL_005`), `_loaded_at` rules (`MDL_006`, `MDL_007`), locked columns unchanged (`MDL_008`) |
| YAML style | `yaml-style.md` | `version: 2` at top (`YML_004`), model description present (`YML_002`), PK column descriptions present (`YML_003`), 2-space indentation (`YML_001`) |
| Modularity | `modularity.md` | No joins in staging (`MOD_001`), mart refs use `ref()` not `source()` (`MOD_002`), one staging model per source table (`MOD_003`), staging materialized as ephemeral (`MOD_004`), business logic in mart not staging (`MOD_005`) |

### 4. CheckTestIntegration

- Verify every `unit_tests[]` entry from the test spec is rendered in the schema YAML.
- Verify `given[].input` references match actual `source()` / `ref()` calls in the model.
- Verify no test scenarios were dropped or modified during rendering.
- Verify gap tests (`test_gap_*`) have reasonable expectations consistent with model logic.

### 5. Verdict

| Condition | Action |
|---|---|
| All checks pass | **Approve** — set `status` to `approved` |
| Standards issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Correctness issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Test integration issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Max review iterations reached (2) | **Approve with warnings** — set `status` to `approved_with_warnings`, flag remaining issues for human review |

After kicking back, the model-generator revises the model, re-runs `dbt test` to confirm unit tests still pass, and resubmits. The resubmission must include an `acknowledgements` block mapping each feedback code to `fixed` or `ignored: <reason>`. Maximum review / model-generator iterations: 2 (configurable).

## Feedback Tiers

| Tier | Action required |
|---|---|
| `error` | Must fix before approval |
| `warning` | Model-generator must respond with `fixed` or `ignored: <reason>` |
| `info` | Acknowledgement optional; ignoring is always acceptable |

## Output Schema (ModelReviewResult)

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

`feedback_for_model_generator` items use this schema:

| Field | Type | Description |
|---|---|---|
| `code` | string | Stable reference code (e.g., `SQL_001`, `CTE_002`, `MISSING_SOURCE_TABLE`) |
| `message` | string | Human-readable description with specific location where possible |
| `severity` | string | Enum: `error`, `warning`, `info` |
| `ack_required` | boolean | `true` for `error` and `warning`; `false` for `info` |

`acknowledgements` is present on resubmission only — a flat map of `{ "<code>": "fixed" | "ignored: <reason>" }`.

`checks.*.issues[]`, `warnings[]`, and `errors[]` use the shared diagnostics schema in `README.md`.

## Code Reviewer Boundary

Code reviewer must not:

- Modify model SQL or schema YAML files
- Run dbt tests or compile commands
- Generate or modify test fixtures
- Override profile decisions (classification, materialization, keys)
- Override ground truth (captured proc output is fact)
