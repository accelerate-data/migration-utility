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

Read-only quality gate for one generated dbt artifact set. Review written
SQL/YAML against the source routine, approved test spec, and shared standards,
then return exactly one `ModelReviewResult`.

## Quick Reference

- Input: `$ARGUMENTS` = target table FQN (`schema.table`)
- Read: `migrate context`, generated SQL/YAML, `test-specs/<item_id>.json`
- Check: artifact fundamentals, correctness, test integration, standards
- Never: modify files, run dbt, override profile decisions
- Output: follow [model-review-output.md](references/model-review-output.md)
- Codes: follow [review-codes.md](references/review-codes.md)

## Workflow

1. Gather context and locate artifacts.

   Read and apply [review-inputs.md](references/review-inputs.md). Use
   `selected_writer_ddl_slice` as the correctness ground truth when present.
   Otherwise use `proc_body`. If both are empty, return a context error.

2. Verify artifact fundamentals.

   Read the written artifacts directly from disk and apply
   [model-artifact-invariants.md](../_shared/references/model-artifact-invariants.md)
   plus the naming contract in
   [model-naming.md](../_shared/references/model-naming.md). Do not trust
   generator self-reported booleans such as `generated.*`.

   Artifact invariant failures belong under `checks.standards`; never add a
   `checks.artifact_fundamentals` key. Missing blocker controls such as
   `_dbt_run_id` or `_loaded_at` must return `revision_requested`, not
   `approved`.

3. Review correctness.

   Apply the correctness checklist in
   [review-checks.md](references/review-checks.md). Use
   `REVIEW_CORRECTNESS_GAP` in both `checks.correctness.issues[]` and the
   matching `feedback_for_model_generator[]` item for any correctness failure.

4. Review test integration.

   Apply the test-integration checklist in
   [review-checks.md](references/review-checks.md). Use
   `REVIEW_TEST_INTEGRATION_GAP` in `checks.test_integration.issues[]` for any
   failure.

5. Review standards.

   Apply the standards checklist in
   [review-checks.md](references/review-checks.md) and all shared standards it
   references. This is exhaustive: direct `source()` usage in mart models for
   confirmed source tables, and direct source/raw warehouse usage for seed
   dependencies, must kick back.

   Classify each dependency before assigning standards codes. If catalog
   metadata marks the relation `is_seed: true`, direct `source()` or raw
   warehouse references are `MDL_017`, not `MDL_016`.

   Use `REVIEW_STANDARDS_VIOLATION` in `checks.standards.issues[]`. Report
   every directly observable stable standards code in
   `feedback_for_model_generator[]`.

Steps 2-5 always run unless a prerequisite/read/parse failure prevents review.

## Verdict

Return:

- `approved` if all three review categories pass
- `revision_requested` if standards, correctness, or test-integration issues exist
- `error` only for prerequisite, IO/parse, or missing-artifact/test-spec failures

When returning `revision_requested`, populate `feedback_for_model_generator[]`
and add `REVIEW_KICKED_BACK` to `warnings[]`.

Do not emit `approved_with_warnings`; `/generate-model` owns max-review-loop
and soft-approval policy.

## Boundary Rules

This skill is read-only. It must not modify files, run dbt commands, or
override profile decisions.
