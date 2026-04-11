---
name: reviewing-tests
description: Use when reviewing generated dbt unit test specs for branch coverage gaps, fixture-quality problems, or iteration-2 approval decisions across table, view, or materialized-view cases
user-invocable: false
context: fork
argument-hint: "<schema.object> — Table, View, or Materialized View FQN"
---

# Reviewing Tests

Quality gate for generated test specs. The reviewer owns branch coverage, fixture quality, and the final verdict.

## When to Use

- Generated tests need an independent branch-by-branch review.
- Fixtures look suspicious, incomplete, or overfit to scenario names.
- Coverage may be partial because of dynamic SQL, runtime state, or missing edge cases.
- Iteration 2 needs a final `approved_with_warnings` vs `revision_requested` decision.

## Arguments

`$ARGUMENTS` is the fully-qualified object name (`item_id`), optionally followed by `--iteration <N>` (1-based). Defaults to 1.

## Quick Workflow

1. Run `migrate-util ready <item_id> test-gen`. Stop on readiness failure.
2. Assemble context using the table or view path in [references/table-vs-view-context.md](references/table-vs-view-context.md).
3. Read `test-specs/<item_id>.json`.
4. Build your own branch manifest from the source logic. Do not trust the generator's `branch_manifest`.
5. Map each scenario to the reviewer-owned branches.
6. Review fixture quality using [references/fixture-quality-rules.md](references/fixture-quality-rules.md).
7. Write and validate the final review JSON.

## Output Requirements

Return one valid `TestReviewOutput` JSON object and validate it with `test-harness validate-review`.

See [references/review-output-contract.md](references/review-output-contract.md) for the field inventory and a full example.

## Step 0: Readiness

Run:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <item_id> test-gen
```

If `ready` is `false`, return a valid `TestReviewOutput` with `status: "error"` and the surfaced `code` and `reason` from `../../lib/shared/generate_tests_error_codes.md`. Do not infer readiness from filenames or directory listings.

## Step 1: Assemble Context

Use the table or view workflow from [references/table-vs-view-context.md](references/table-vs-view-context.md).

Rules:

- For tables, pass `--writer <proc_fqn>` when the caller already provided the intended writer or when more than one writer could target the table.
- For views, use `discover show` plus the view catalog file.
- In both paths, read `test-specs/<item_id>.json` and use `unit_tests[]`.

## Step 2: Enumerate Branches Independently

Enumerate branches from the source logic, not from the generated test spec.

- Use [../_shared/references/branch-patterns.md](../_shared/references/branch-patterns.md).
- Use the table patterns for tables and the view patterns for views.
- Assign each branch a stable snake_case `id` and a human-readable `description`.
- Record the result in `reviewer_branch_manifest`.

## Step 3: Map Scenarios to Branches

For each entry in `unit_tests[]`, determine which reviewer-owned branches it actually exercises.

Rules:

- A scenario may cover multiple branches.
- A branch may require multiple scenarios.
- Map by fixture logic in `given[]` and expected behavior, not by scenario names or generator descriptions.

## Step 4: Score Coverage

Compute:

- `total_branches`
- `covered_branches`
- `untestable_branches`
- `score`
- `uncovered`
- `untestable`

Mark a branch as `untestable` only when static fixtures cannot represent it. Every untestable branch needs a concrete `rationale`.

See [references/coverage-rules.md](references/coverage-rules.md) for examples and boundary cases.

## Step 5: Review Fixture Quality

Check:

- fixture realism
- scenario isolation
- FK consistency across given tables
- edge cases where the logic calls for them
- required source columns

Required-source-column rule:

- Flag a missing source column only when the exercised logic needs that column to join, filter, compute, insert, update, or otherwise represent the reviewed branch correctly.
- Do not require every catalog-level `NOT NULL` source column.
- Do not flag unrelated `NOT NULL` columns that the source logic never reads.

Use [references/fixture-quality-rules.md](references/fixture-quality-rules.md) for the detailed heuristics and examples.

Record each issue with:

- `scenario`
- `issue`
- `severity`

Use `error` only when the fixture is invalid for the reviewed branch. Use `warning` for weaker realism or coverage concerns.

## Step 6: Verdict

| Condition | Status | Required feedback |
|---|---|---|
| All testable branches covered and fixture quality acceptable | `approved` | none required |
| Coverage gaps only | `revision_requested` | `feedback_for_generator.uncovered_branches` |
| Quality issues only | `revision_requested` | `feedback_for_generator.quality_fixes` |
| Coverage gaps and quality issues | `revision_requested` | both feedback fields |
| Iteration 2 and issues remain | `approved_with_warnings` | warning entry for human review |

Maximum review iterations: 2.

## Step 7: Validate and Return

Before schema validation, verify that the review is self-consistent from reviewer-owned evidence:

- `coverage.total_branches` must equal `len(reviewer_branch_manifest)`
- `coverage.covered_branches` must equal the number of `reviewer_branch_manifest` entries where `covered: true`
- every `coverage.uncovered[].id`, `coverage.untestable[].id`, and `feedback_for_generator.uncovered_branches[]` entry must come from `reviewer_branch_manifest`

Do not rely on the generator's `branch_manifest` to fill those fields. The review must remain valid even if the generator's branch manifest is stale or wrong.

Write the `TestReviewResult` JSON to `.staging/review.json`, then validate:

```bash
mkdir -p .staging
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness validate-review \
  --review-file .staging/review.json
```

If validation fails, fix the reported fields and retry. After validation passes, return the JSON and remove `.staging`.

## Common Mistakes

- Trusting the generator's `branch_manifest` instead of building your own.
- Inferring readiness from files instead of running `migrate-util ready`.
- Mapping coverage from scenario names instead of the fixture logic.
- Rejecting fixtures because unrelated source columns are `NOT NULL` in the catalog.
- Forgetting `--writer` when the intended table writer is already known.

## Boundary Rules

This skill is read-only except for `.staging/review.json`. Do not write to `test-specs/`, modify fixtures, execute routines, or override source-of-truth artifacts.

## Error Handling

On command failure, still return valid `TestReviewOutput` JSON using surfaced codes from `../../lib/shared/generate_tests_error_codes.md`.

See [references/error-handling.md](references/error-handling.md) for command-specific mappings.

## References

- [references/review-output-contract.md](references/review-output-contract.md) — full output contract and example JSON
- [references/table-vs-view-context.md](references/table-vs-view-context.md) — context assembly for table vs view reviews
- [references/coverage-rules.md](references/coverage-rules.md) — untestable-branch examples and coverage boundary cases
- [references/fixture-quality-rules.md](references/fixture-quality-rules.md) — detailed fixture-quality heuristics, including NOT NULL guidance
- [references/error-handling.md](references/error-handling.md) — command-specific error mappings
- [../_shared/references/branch-patterns.md](../_shared/references/branch-patterns.md) — branch enumeration patterns
- [`../../lib/shared/generate_tests_error_codes.md`](../../lib/shared/generate_tests_error_codes.md) — canonical surfaced error and warning codes
