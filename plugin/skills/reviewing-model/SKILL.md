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
- Return: exactly one `ModelReviewResult` JSON object
- Never: modify files, run dbt, override profile decisions

This reviewer is stateless. Caller workflows such as `/generate-model` own the max-review-loop policy.

## Step 1: Gather context

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id> \
  [--writer <writer_fqn>]
```

Use `proc_body` as the ground truth. Ignore `refactored_sql`.

Then read the generated artifacts from `dbt/models/`:

- Check both `dbt/models/staging/` and `dbt/models/marts/`.
- For table migrations, review the generated layer model already on disk.
- If the only discovered SQL/YAML pair for a table migration uses `stg_*`, review that pair and flag naming/layer violations instead of returning `MODEL_NOT_FOUND`.
- For view-based staging reviews, `stg_<table>.sql` and `_<model_name>.yml` are valid artifact shapes.
- Return `MODEL_NOT_FOUND` when the selected artifact is missing either the SQL file or the paired schema YAML.

Derive `model_name` from the SQL filename and verify it matches the naming contract in [model-naming.md](../_shared/references/model-naming.md).

## Step 2: Verify artifact fundamentals

Before deeper review, verify the written artifacts directly from disk against [../_shared/references/model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md). Do not trust generator self-reported booleans such as `generated.*`.

Also verify the SQL/model name/file path match the naming contract.

These are reviewable artifact failures, not generator metadata disputes. Add them to `checks.standards.issues[]` and `feedback_for_model_generator[]`, then continue with the remaining review steps.

Missing required control columns are blocker defects. If `_dbt_run_id` is missing where `MDL_005` applies, or `_loaded_at` is missing where `MDL_006` applies, the review must end in `revision_requested`, not `approved`.

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
- model materialization matches the derived profile

Use `REVIEW_CORRECTNESS_GAP` in `checks.correctness.issues[]` for any correctness failure.
Partial or uncovered test-spec coverage is not permission to ignore a semantic gap visible in `proc_body`.

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

Evaluate the SQL and YAML against the shared standards references:

- [sql-style.md](../_shared/references/sql-style.md)
- [cte-structure.md](../_shared/references/cte-structure.md)
- [model-naming.md](../_shared/references/model-naming.md)
- [yaml-style.md](../_shared/references/yaml-style.md)

Verify at minimum:

- SQL style and casing
- CTE order and `final` shape
- model/file naming and layer rules
- any remaining shared artifact invariant failures not already recorded in Step 2
- YAML indentation and structure beyond the shared invariants

Use `REVIEW_STANDARDS_VIOLATION` in `checks.standards.issues[]`.
Report every directly observable stable standards code that applies in `feedback_for_model_generator`; do not collapse multiple standards failures into one representative item.
Even when blocker defects already require `revision_requested`, continue checking the written SQL and YAML for independent standards issues such as `SQL_*`, `CTE_*`, and `YML_*`.

Steps 2, 3, 4, and 5 always run regardless of one another's result.

## Step 6: Verdict

Return:

- `approved` if all three review categories pass
- `revision_requested` if standards, correctness, or test-integration issues exist
- `error` only for prerequisite, IO/parse, or missing-artifact/test-spec failures

Missing blocker standards such as `MDL_005`, `MDL_006`, missing `config(`, or missing required YAML fundamentals must always produce `revision_requested`.

When returning `revision_requested`, populate `feedback_for_model_generator` and add `REVIEW_KICKED_BACK` to `warnings[]`.

Caller workflows may convert a second unresolved `revision_requested` result into `approved_with_warnings` after their configured review-loop limit is reached.

## Output

Return exactly one `ModelReviewResult` JSON object with:

- `item_id`
- `status`
- `checks.standards`
- `checks.correctness`
- `checks.test_integration`
- `feedback_for_model_generator`
- optional `acknowledgements` on resubmission
- `warnings`
- `errors`

Every `checks.*` object must include both `passed` and `issues`, even when `issues` is empty. Do not omit `passed: true`.
Every `feedback_for_model_generator[]` item must be an object with `code`, `message`, `severity`, and `ack_required`. Never use plain strings or shorthand entries.
Set `ack_required: true` for every `error` or `warning`, and `ack_required: false` only for `info`.

Full output-shape and severity rules: [references/model-review-output.md](references/model-review-output.md)

Code-family allocation rules: [references/review-codes.md](references/review-codes.md)

## Error handling

- `migrate context` exits 1 -> `status: "error"` with `CONTEXT_PREREQUISITE_MISSING`
- `migrate context` exits 2 -> `status: "error"` with `CONTEXT_IO_ERROR`
- generated model SQL or schema YAML missing -> `status: "error"` with `MODEL_NOT_FOUND`
- approved test spec missing -> `status: "error"` with `TEST_SPEC_NOT_FOUND`

Always return valid `ModelReviewResult` JSON, even on error paths.

## Boundary Rules

This skill is read-only. It must not modify files, run dbt commands, or override profile decisions.
