---
name: reviewing-model
description: >
  Use when the generate-model workflow needs a read-only review of generated
  dbt model artifacts before accepting the result or sending it back for fixes.
context: fork
user-invocable: false
argument-hint: "<schema.table>"
---

# Reviewing Model

Read-only quality gate for generated dbt model artifacts. Review the generated SQL and schema YAML against the source routine, approved test spec, and shared standards, then return a `ModelReviewResult`.

## Quick Reference

- Input: `$ARGUMENTS` = target table FQN (`schema.table`)
- Read: `migrate context`, generated SQL/YAML, `test-specs/<item_id>.json`
- Check: correctness, test integration, standards
- Never: modify files, run dbt, override profile decisions
- Feedback: `feedback_for_model_generator[]` items are always objects, never strings; `ack_required` is `true` for `error` and `warning`

Return a single `ModelReviewResult` JSON object with this exact check shape:

```json
{
  "item_id": "<schema.table>",
  "status": "approved|revision_requested|error",
  "checks": {
    "standards": { "passed": true, "issues": [] },
    "correctness": { "passed": true, "issues": [] },
    "test_integration": { "passed": true, "issues": [] }
  },
  "feedback_for_model_generator": [],
  "warnings": [],
  "errors": []
}
```

Do not add other `checks` keys.
Do not emit `approved_with_warnings`; caller workflows own max-review-loop and soft-approval policy.

Code allocation:

| Finding type | `checks.*.issues[]` code | `feedback_for_model_generator[].code` |
|---|---|---|
| Standards/style/naming/YAML | `REVIEW_STANDARDS_VIOLATION` | Stable standard code: `SQL_*`, `CTE_*`, `MDL_*`, or `YML_*` |
| Transformation correctness | `REVIEW_CORRECTNESS_GAP` | `REVIEW_CORRECTNESS_GAP` |
| Unit-test/spec integration | `REVIEW_TEST_INTEGRATION_GAP` | `REVIEW_TEST_INTEGRATION_GAP` |
| Prerequisite/read/parse failure | n/a | Put the shared error code in `errors[]` |

## Step 1: Gather context

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" migrate context \
  --table <item_id> \
  [--writer <writer_fqn>]
```

Use `proc_body` as the correctness ground truth. `refactored_sql` and
`writer_ddl_slice` explain the generator input, but they do not override the
stored procedure semantics during review.

Then read the generated artifacts from `dbt/models/`:

- Check `dbt/models/marts/<model_name>.sql` with `dbt/models/marts/_marts__models.yml` for first-pass generated table/view targets.
- Check `dbt/snapshots/` for snapshot targets.
- Treat `dbt/models/staging/` as source-wrapper territory. If the selected migrated target is only present there, review it and flag naming/layer violations instead of returning `MODEL_NOT_FOUND`.
- The valid staging artifacts are pass-through `stg_bronze__<entity>.sql` wrappers plus `dbt/models/staging/_staging__sources.yml` and `_staging__models.yml`.
- Read `dbt/dbt_project.yml` and verify layer defaults declare staging as `view`, intermediate as `ephemeral`, and marts as `table`.
- For every relation in `source_tables`, inspect the matching catalog table JSON when available and note whether it is marked `is_source: true` or `is_seed: true`.
- If the reviewed SQL references a seed with `ref('<seed_name>')`, verify `dbt/seeds/<seed_name>.csv` and `dbt/seeds/_seeds.yml` exist. If `_seeds.yml` is present, verify it includes the seed entry and known columns from catalog metadata.
- Return `MODEL_NOT_FOUND` when the selected artifact is missing either the SQL file or the paired schema YAML.

Derive `model_name` from the SQL filename and verify it matches the naming contract in [model-naming.md](../_shared/references/model-naming.md).

## Step 2: Verify standards fundamentals

Before deeper review, verify the written artifacts directly from disk against [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md). Do not trust generator self-reported booleans such as `generated.*`.

Also verify the SQL/model name/file path match the naming contract.

These are reviewable artifact failures, not generator metadata disputes. Add them to `checks.standards.issues[]` and `feedback_for_model_generator[]`, then continue with the remaining review steps.
Never add `checks.artifact_fundamentals`; artifact invariant failures belong under `checks.standards`.

Missing blocker controls such as `_dbt_run_id` or `_loaded_at` must end in `revision_requested`, not `approved`.

## Step 3: Review correctness

Compare the generated model to `proc_body`.

Verify:

- all `source_tables` are represented
- all target `columns` reach the final CTE
- joins and filters preserve source semantics
- aggregation grain matches
- `UPDATE ... FROM` rewrites preserve target-row retention semantics; if the original routine updates existing target rows and leaves unmatched target rows unchanged, a source-driven full refresh that drops those rows is a correctness gap
- if the original routine updates an existing target table but the reviewed model never reads that target relation, treat that as a likely `REVIEW_CORRECTNESS_GAP` unless the procedure is clearly full-refresh replacement logic
- incremental logic matches MERGE intent where applicable

Use `REVIEW_CORRECTNESS_GAP` in both `checks.correctness.issues[]` and the matching `feedback_for_model_generator[]` item for any correctness failure.

## Step 4: Review test integration

Read `test-specs/<item_id>.json` and compare it to the generated schema YAML.

Verify:

- the approved test spec exists and is readable
- `unit_tests:` are rendered when the spec contains `unit_tests[]`
- scenario names match the approved spec
- unit tests target the reviewed `model_name`
- required schema tests remain present in YAML

Use `REVIEW_TEST_INTEGRATION_GAP` in `checks.test_integration.issues[]` for any failure.

## Step 5: Review standards

Evaluate the generated dbt project artifacts against the shared standards references:

- [dbt-project-standards.md](../_shared/references/dbt-project-standards.md)
- [sql-style.md](../_shared/references/sql-style.md)
- [cte-structure.md](../_shared/references/cte-structure.md)
- [model-naming.md](../_shared/references/model-naming.md)
- [yaml-style.md](../_shared/references/yaml-style.md)

Apply all rules in the shared standards references above. Pay special attention to these project-layout and dependency-reference standards:

- SQL style and casing
- CTE order and `final` shape
- model/file naming and layer rules
- model materialization matches the derived profile; ordinary first-pass mart tables rely on the `marts` layer default, while view and materialized-view profiles (`classification: stg|mart`) must use explicit `materialized='view'`, not `ephemeral`
- `MDL_016`: mart SQL uses `ref('stg_bronze__<entity>')` instead of direct `source('bronze', '<table>')` for confirmed source dependencies; if the expected wrapper is missing, report the missing setup artifact instead of approving direct source use
- `MDL_017`: mart SQL uses `ref('<seed_name>')` for catalog tables marked `is_seed: true` when they appear in joins or filters
- seed dependencies referenced through direct `source()` or raw warehouse names are standards violations, even when the SQL would compile
- `dbt_project.yml` layer materialization defaults
- no redundant `materialized='table'` config on ordinary mart table models
- any remaining shared artifact invariant failures not already recorded in Step 2
- YAML indentation and structure beyond the shared invariants

Do not stop after this callout list; it highlights common misses but does not limit the referenced standards.

Use `REVIEW_STANDARDS_VIOLATION` in `checks.standards.issues[]`.
Report each directly observable stable standards code in `feedback_for_model_generator`. Each feedback item must use this object shape:

```json
{
  "code": "MDL_005",
  "message": "Add _dbt_run_id = '{{ invocation_id }}' to the model SQL and schema YAML.",
  "severity": "error",
  "ack_required": true
}
```

Steps 2-5 always run.

## Step 6: Verdict

Return:

- `approved` if all three review categories pass
- `revision_requested` if standards, correctness, or test-integration issues exist
- `error` only for prerequisite, IO/parse, or missing-artifact/test-spec failures

Missing blocker standards such as `MDL_005`, `MDL_006`, missing required exception config, or missing required YAML fundamentals must always produce `revision_requested`.

When returning `revision_requested`, populate `feedback_for_model_generator` and add `REVIEW_KICKED_BACK` to `warnings[]`.
Do not return `approved_with_warnings`; the generate-model caller decides whether to stop after repeated reviews.

## Output

Return exactly one `ModelReviewResult` JSON object. Output shape, severity rules, and `ack_required` behavior live in [references/model-review-output.md](references/model-review-output.md). Code-family allocation rules live in [references/review-codes.md](references/review-codes.md).

Every `feedback_for_model_generator[]` item must be an object with `code`, `message`, `severity`, and `ack_required`. Never return feedback as strings such as `"MDL_005: ..."`.
Set `ack_required: true` for every `feedback_for_model_generator[]` item whose `severity` is `error` or `warning`. Only `severity: "info"` may use `ack_required: false`.

## Error handling

- `migrate context` exits 1 -> `status: "error"` with `CONTEXT_PREREQUISITE_MISSING`
- `migrate context` exits 2 -> `status: "error"` with `CONTEXT_IO_ERROR`
- generated model SQL or schema YAML missing -> `status: "error"` with `MODEL_NOT_FOUND`
- approved test spec missing -> `status: "error"` with `TEST_SPEC_NOT_FOUND`

Always return valid `ModelReviewResult` JSON, even on error paths.

## Boundary Rules

This skill is read-only. It must not modify files, run dbt commands, or override profile decisions.
