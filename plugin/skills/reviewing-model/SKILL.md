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

Run the stage guard:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util guard <table_fqn> reviewing-model
```

If `passed` is `false`, report the failing guard's `code` and `message` to the user and stop.

## Step 1: Assemble context

Run the deterministic context assembly CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id>
```

Read the output JSON. It contains:

- `proc_body` — full original procedure SQL (use this as the ground truth for correctness checks)
- `statements` — resolved statement list with action and SQL
- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived materialization
- `columns` — target table column list
- `source_tables` — tables read by the writer
- `refactored_sql` — intermediate refactored SQL (present but **not used** — see below)

Also read:

- The generated model SQL and schema YAML from the dbt project.
- The approved test spec from `test-specs/<item_id>.json`.

Do NOT use `refactored_sql`. It is an intermediate artifact produced by the refactor stage. The reviewer validates the generated dbt model directly against the original proc DDL (`proc_body`). Ground truth is always the original proc.

## Step 2: Check correctness

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

Both Step 2 and Step 3 always run regardless of the other's result.

## Step 3: Check standards

Evaluate the generated model SQL and schema YAML against the reference files. Each issue found must include the stable reference code (e.g., `SQL_001`, `CTE_002`).

| Area | Reference file | Key checks |
|---|---|---|
| SQL style | [references/sql-style.md](references/sql-style.md) | Lowercase keywords (`SQL_001`), indentation (`SQL_002`), trailing commas (`SQL_003`), one column per line (`SQL_004`), table alias prefixes (`SQL_005`), no `SELECT *` in marts (`SQL_006`) |
| CTE structure | [references/cte-structure.md](references/cte-structure.md) | Import CTEs first (`CTE_001`), final CTE named `final` (`CTE_002`), `select * from final` last (`CTE_003`), single-purpose CTEs (`CTE_004`), no nested CTEs (`CTE_006`) |
| Model naming | [references/model-naming.md](references/model-naming.md) | Correct layer prefix (`MDL_001`–`MDL_003`), `snake_case` names (`MDL_004`), `_dbt_run_id` present (`MDL_005`), `_loaded_at` rules (`MDL_006`, `MDL_007`), locked columns unchanged (`MDL_008`) |
| YAML style | [references/yaml-style.md](references/yaml-style.md) | `version: 2` at top (`YML_004`), model description present (`YML_002`), PK column descriptions present (`YML_003`), 2-space indentation (`YML_001`) |
| Modularity | [references/modularity.md](references/modularity.md) | No joins in staging (`MOD_001`), mart refs use `ref()` not `source()` (`MOD_002`), one staging model per source table (`MOD_003`), staging materialized as ephemeral (`MOD_004`), business logic in mart not staging (`MOD_005`) |

## Step 4: Check test integration

- Verify every `unit_tests[]` entry from the test spec is rendered in the schema YAML.
- Verify `given[].input` references match actual `source()` / `ref()` calls in the model.
- Verify no test scenarios were dropped or modified during rendering.
- Verify gap tests (`test_gap_*`) have reasonable expectations consistent with the model logic.

## Feedback tiers

| Tier | Action required |
|------|-----------------|
| `error` | Must fix before approval |
| `warning` | Model-generator must respond with `fixed` or `ignored: <reason>` |
| `info` | Acknowledgement optional; ignoring is always acceptable |

## Step 5: Verdict

| Condition | Action |
|---|---|
| All checks pass | **Approve** — set `status` to `approved` |
| Standards issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Correctness issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Test integration issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Max review iterations reached (2) | **Approve with warnings** — set `status` to `approved_with_warnings`, flag remaining issues for human review |

After kicking back, the model-generator revises the model, re-runs `dbt test` to confirm unit tests still pass, and resubmits. The resubmission must include an `acknowledgements` block mapping each feedback code to `fixed` or `ignored: <reason>`. Maximum review / model-generator iterations: 2 (configurable).

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

`feedback_for_model_generator` items use this schema:

```json
{
  "code": "SQL_001",
  "message": "Human-readable description with specific location where possible",
  "severity": "error|warning|info",
  "ack_required": true
}
```

`ack_required` is `true` for `error` and `warning` severity; `false` for `info` severity.

`acknowledgements` is present on resubmission only — a flat map of `{ "<code>": "fixed" | "ignored: <reason>" }`.

## References

- [references/sql-style.md](references/sql-style.md) — SQL formatting rules with stable codes (SQL_001–SQL_013): keywords, indentation, commas, aliases, JOIN style
- [references/cte-structure.md](references/cte-structure.md) — CTE pattern rules (CTE_001–CTE_008): import-first order, `final` naming, single-purpose CTEs, no nesting
- [references/model-naming.md](references/model-naming.md) — layer prefix, snake_case, `_dbt_run_id` and `_loaded_at` ETL control column rules (MDL_001–MDL_013)
- [references/yaml-style.md](references/yaml-style.md) — YAML formatting rules (YML_001–YML_008): `version: 2`, required descriptions, indentation
- [references/modularity.md](references/modularity.md) — staging/mart separation rules (MOD_001–MOD_008): no joins in staging, ephemeral materialization, mart uses `ref()` not `source()`

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
