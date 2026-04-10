---
name: reviewing-model
description: >
  Reviews generated dbt model for standards compliance and correctness relative
  to the original source routine. Invoked by the /generate-model command after dbt tests
  pass, not directly by FDE.
context: fork
user-invocable: false
argument-hint: "<schema.table>"
---

# Reviewing Model

Quality gate for model generation output. Reviews the generated dbt model SQL and schema YAML for standards compliance and correctness against the original source routine context. Issues a verdict: approve or kick back with specific fixes.

## Arguments

`$ARGUMENTS` is the fully-qualified table name (the `item_id`). Ask the caller if missing.

## Step 1: Assemble context

Review the generated model artifacts already present on disk. Run the deterministic context assembly CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id>
```

Read the output JSON. It contains:

- `proc_body` — full original source SQL (use this as the ground truth for correctness checks)
- `statements` — resolved statement list with action and SQL
- `profile` — classification, keys, watermark, PII answers
- `materialization` — derived materialization
- `columns` — target table column list
- `source_tables` — tables read by the writer
- `refactored_sql` — intermediate refactored SQL (present but **not used** — see below)

Also read the generated model SQL and schema YAML from the dbt project using these discovery rules:

- Review the generated dbt files that already exist under `dbt/models/`.
- Check both `dbt/models/staging/` and `dbt/models/marts/`.
- For staging reviews, prefer `stg_<table>.sql` and `_stg_<table>.yml` when present.
- For mart reviews, use the mart model files already on disk.
- If SQL exists but YAML is missing, treat that as a review issue rather than a
  fatal discovery error.
- Return `MODEL_NOT_FOUND` only when no generated model SQL file for the target
  object exists anywhere under `dbt/models/`.

When reading the generated artifacts, also derive the generated `model_name`
from the SQL filename and check it against the naming contract used by
`/generating-model`:

- model files live at `dbt/models/<layer>/<model_name>.sql`
- schema files live at `dbt/models/<layer>/_<model_name>.yml`
- the reviewer must verify that `<model_name>` follows
  [model-naming.md](../_shared/references/model-naming.md) and matches the
  expected target-object convention for the selected layer

Do NOT use `refactored_sql`. It is an intermediate artifact produced by the refactor stage. The reviewer validates the generated dbt model directly against the original source DDL (`proc_body`). Ground truth is always the original source routine.

## Step 2: Check correctness

Compare the generated model against `proc_body` (the original source DDL). This is the primary check.

| Check | What to verify |
|---|---|
| Source tables | All `source_tables` from context are referenced in the model |
| Column completeness | All target `columns` from context are selected in the final CTE |
| Join semantics | JOIN types and conditions match the original source routine |
| Filter predicates | WHERE/HAVING clauses preserved |
| Aggregation | GROUP BY grain matches |
| MERGE semantics | Incremental config correctly implements MERGE logic |
| Materialization | Matches derived materialization from profile |

Both Step 2 and Step 3 always run regardless of the other's result.

## Step 3: Check standards

Evaluate the generated model SQL and schema YAML against the reference files. Each issue found must include the stable reference code (e.g., `SQL_001`, `CTE_002`).

| Area | Reference file | Key checks |
|---|---|---|
| SQL style | [../_shared/references/sql-style.md](../_shared/references/sql-style.md) | Lowercase keywords (`SQL_001`), indentation (`SQL_002`), trailing commas (`SQL_003`), one column per line (`SQL_004`), table alias prefixes (`SQL_005`), no `SELECT *` in marts (`SQL_006`) |
| CTE structure | [../_shared/references/cte-structure.md](../_shared/references/cte-structure.md) | Import CTEs first (`CTE_001`), final CTE named `final` (`CTE_002`), `select * from final` last (`CTE_003`), single-purpose CTEs (`CTE_004`), no nested CTEs (`CTE_006`) |
| Model naming | [../_shared/references/model-naming.md](../_shared/references/model-naming.md) | Correct generated `model_name` and artifact filenames for the layer, correct layer prefix (`MDL_001`--`MDL_003`), `snake_case` names (`MDL_004`), `_dbt_run_id` present (`MDL_005`), `_loaded_at` rules (`MDL_006`, `MDL_007`), locked columns unchanged (`MDL_008`) |
| YAML style | [../_shared/references/yaml-style.md](../_shared/references/yaml-style.md) | `version: 2` at top (`YML_004`), model description present (`YML_002`), PK column descriptions present (`YML_003`), 2-space indentation (`YML_001`) |

For model naming review specifically:

- compare the generated `model_name` and file paths to the target object name
- flag any mismatch between the SQL filename, YAML filename, and declared model
  name using the appropriate `MDL_*` code
- treat a wrong layer prefix or non-`snake_case` filename as a standards issue,
  even if the SQL body itself is otherwise correct

## Feedback tiers

| Tier | Action required |
|------|-----------------|
| `error` | Must fix before approval |
| `warning` | Model-generator must respond with `fixed` or `ignored: <reason>` |
| `info` | Acknowledgement optional; ignoring is always acceptable |

## Step 4: Verdict

| Condition | Action |
|---|---|
| All checks pass | **Approve** — set `status` to `approved` |
| Standards issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Correctness issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_model_generator` with objects including stable code and tier |
| Max review iterations reached (2) | **Approve with warnings** — set `status` to `approved_with_warnings`, flag remaining issues for human review |

After kicking back, the model-generator revises the model, re-runs `dbt test` to confirm unit tests still pass, and resubmits. The resubmission must include an `acknowledgements` block mapping each feedback code to `fixed` or `ignored: <reason>`. Maximum review / model-generator iterations: 2 (configurable).

## Output schema (ModelReviewResult)

Return exactly one JSON object matching this shape. Do not wrap the JSON in markdown, headings, summaries, or follow-up questions.

```json
{
  "item_id": "silver.dimproduct",
  "status": "approved|revision_requested|approved_with_warnings|error",
  "checks": {
    "standards": { "passed": true, "issues": [] },
    "correctness": {
      "passed": false,
      "issues": [
        {
          "code": "MISSING_SOURCE_TABLE",                              // diagnostics: error|warning only
          "message": "bronze.product_category referenced in source routine but not in model import CTEs",
          "severity": "error"
        }
      ]
    }
  },
  "feedback_for_model_generator": [
    { "code": "SQL_001",  "message": "Keywords should be lowercase — found uppercase SELECT on line 4",
      "severity": "error", "ack_required": true },                     // error/warning → ack_required: true
    { "code": "CTE_002",  "message": "Final CTE must be named 'final' — currently named 'output'",
      "severity": "error", "ack_required": true },
    { "code": "SQL_013",  "message": "Line 12 exceeds 80 characters",
      "severity": "info",  "ack_required": false }                     // info → ack_required: false
  ],
  "acknowledgements": {                                                // present on resubmission only
    "SQL_001": "fixed",
    "CTE_002": "ignored: legacy naming convention required by downstream tool"
  },
  "warnings": [],                                                      // diagnostics: error|warning only
  "errors": []                                                         // diagnostics: error|warning only
}
```

Severity and acknowledgement rules:

- `checks.*.issues[]`, `warnings[]`, and `errors[]` use only `error` or `warning` severity. Never place `info` entries in these arrays.
- `feedback_for_model_generator` may use `error`, `warning`, or `info`. Set `ack_required: true` for `error`/`warning`, `false` for `info`.
- `acknowledgements` is a flat map of `{ "<code>": "fixed" | "ignored: <reason>" }`, present on resubmission only.
- All `code` values in `warnings[]` and `errors[]` must come from `../../lib/shared/generate_model_error_codes.md`. Stable standards codes in `feedback_for_model_generator` must come from the referenced standards files.

## References

- [../_shared/references/sql-style.md](../_shared/references/sql-style.md) — SQL formatting rules with stable codes (SQL_001--SQL_013): keywords, indentation, commas, aliases, JOIN style
- [../_shared/references/cte-structure.md](../_shared/references/cte-structure.md) — CTE pattern rules (CTE_001--CTE_008): import-first order, `final` naming, single-purpose CTEs, no nesting
- [../_shared/references/model-naming.md](../_shared/references/model-naming.md) — layer prefix, snake_case, `_dbt_run_id` and `_loaded_at` ETL control column rules (MDL_001--MDL_013)
- [../_shared/references/yaml-style.md](../_shared/references/yaml-style.md) — YAML formatting rules (YML_001--YML_008): `version: 2`, required descriptions, indentation

## Boundary rules

Reviewing-model must not:

- Modify model SQL or schema YAML files
- Repair fixture or catalog files
- Run dbt tests or compile commands
- Generate or modify test fixtures
- Write review result files
- Ask permission to write review result files
- Ask whether the provided `--project-root` fixture path exists or should be created
- Override profile decisions (classification, materialization, keys)
- Override ground truth (captured execution output is fact)

## Error handling

- `migrate context` exits 1 — prerequisite missing. Return valid `ModelReviewResult` JSON with `status: "error"` and code `CONTEXT_PREREQUISITE_MISSING`.
- `migrate context` exits 2 — IO or parse error. Return valid `ModelReviewResult` JSON with `status: "error"` and code `CONTEXT_IO_ERROR`.
- Generated model files missing — return valid `ModelReviewResult` JSON with `status: "error"` and code `MODEL_NOT_FOUND`.

If you hit a handled error, still return a valid `ModelReviewResult` JSON
object. Do not ask follow-up questions.
