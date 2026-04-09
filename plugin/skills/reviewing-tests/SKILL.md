---
name: reviewing-tests
description: >
  Reviews test generation output for coverage and quality. Independently
  enumerates branches, scores coverage, and reviews fixture quality.
  Invoked by the /generate-tests command, not directly by the user.
user-invocable: false
context: fork
argument-hint: "<schema.table>"
---

# Reviewing Tests

## Arguments

`$ARGUMENTS` is the fully-qualified table name (the `item_id`), optionally followed by `--iteration <N>` (1-based). Defaults to 1 if not provided.

## Contracts

Use the canonical review output schema:

- review result: `../../lib/shared/schemas/test_review_output.json`

Return exactly one JSON object matching that schema. Do not wrap in markdown, headings, summaries, or follow-up questions.

## Before invoking

Run the stage readiness check:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util ready <table_fqn> test-gen
```

If `ready` is `false`, report the failing readiness `code` and `reason` to the
caller and stop. Use only codes from
`../../lib/shared/generate_tests_error_codes.md`.

## Step 1: Assemble context

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate context \
  --table <item_id>
```

Read the output JSON. It contains:

- `proc_body` — full original procedure SQL
- `statements` — resolved statement list with action and SQL
- `profile` — classification, keys, watermark, PII answers
- `columns` — target table column list
- `source_tables` — tables read by the writer

Also read the test generator's output:

```text
test-specs/<item_id>.json
```

Read `unit_tests[]` from this file.

## Step 2: Independent branch enumeration

Enumerate all conditional branches from `proc_body` and `statements` independently. Do NOT read or trust the generator's `branch_manifest` — build your own.

| Pattern | Branches |
|---|---|
| MERGE WHEN clauses | One per clause (MATCHED, NOT MATCHED, NOT MATCHED BY SOURCE) |
| CASE/WHEN | One per arm + ELSE |
| JOIN | Match, no-match (LEFT JOIN NULL right side), partial multi-condition |
| WHERE | Pass, fail |
| Subquery | EXISTS true/false, IN match/miss |
| NULL handling | NULL vs non-NULL in filters, joins, COALESCE/ISNULL |
| Aggregation | Single group, multiple groups, empty source |
| Type boundaries | Watermark edges, MAX int, empty string |
| Empty source | Zero-row edge case per source table |

Assign each branch a stable `id` (snake_case, descriptive) and a human-readable `description`. Record the full list as the reviewer's branch manifest.

## Step 3: Map scenarios to branches

Read `unit_tests[]` from `test-specs/<item_id>.json`. For each scenario entry, determine which branches (from the reviewer's own enumeration in Step 2) the scenario exercises.

Rules:

- A scenario may cover multiple branches.
- A branch may require multiple scenarios to cover fully.
- Map by analyzing the `given` fixture rows against the proc logic — not by trusting scenario names or descriptions alone.

## Step 4: Score coverage

Compute coverage:

- **total_branches**: count of branches from the reviewer's enumeration.
- **covered_branches**: count of branches with at least one mapped scenario.
- **untestable_branches**: count of branches marked untestable (see below).
- **score**: `complete` if all testable branches are covered (covered + untestable = total), `partial` otherwise.
- **uncovered**: list of branch objects (`id`, `description`) that have zero mapped scenarios and are not untestable.
- **untestable**: list of branch objects (`id`, `description`, `rationale`) that cannot be tested with static fixtures.

A branch is **untestable** when it depends on runtime state that static fixtures cannot reproduce: `GETDATE()`/`SYSDATETIME()` comparisons, dynamic SQL with variable table/column targets, external service calls, or non-deterministic functions. Each untestable classification requires a `rationale`.

## Step 5: Review fixture quality

For each test scenario in `unit_tests[]`, assess these dimensions:

- **Fixture realism:** Are synthetic values type-appropriate and reasonable? Flag unrealistic values like negative prices, future dates for historical data, or strings in numeric fields.
- **Scenario isolation:** Does each scenario test one branch clearly, or are multiple branches tangled in a way that makes failure diagnosis ambiguous?
- **FK consistency:** Do foreign key values in fixture rows align across source tables within each scenario? A row referencing `customer_key = 42` in the fact table should have a matching `customer_key = 42` in the dimension fixture.
- **Edge cases:** Are boundary values present where appropriate (NULLs, empty strings, MAX values, zero-row inputs)?
- **NOT NULL completeness:** For every source table in `given[]`, load the catalog column list from `source_tables` in the context output. Check that every column where `is_nullable` is false and `is_identity` is false appears in `rows[0]`. Missing NOT NULL columns will cause SQL Server INSERT failures at execution time. Severity: `error` (not warning — these always fail).

Record each issue with the scenario name, a description of the concern, and a severity (`warning` or `error`).

## Step 6: Verdict

Apply the following verdict rules:

| Condition | Action |
|---|---|
| All testable branches covered + quality acceptable | **Approve** — set `status` to `approved`. If untestable branches exist, include them in the output for documentation |
| Testable coverage gaps identified | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_generator.uncovered_branches` with the branch IDs that lack scenarios (exclude untestable branches) |
| Quality issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_generator.quality_fixes` with specific remediation instructions per scenario |
| Both coverage gaps and quality issues | **Kick back** — set `status` to `revision_requested`, populate both feedback fields |
| Iteration 2 and issues remain | **Approve with warnings** — set `status` to `approved_with_warnings`, add a warning entry flagging the item for human review |

Maximum review iterations: 2.

## Boundary rules

Test reviewer must not:

- Generate or modify fixture data
- Execute stored procedures
- Write to `test-specs/` — only the test generator writes there
- Write review result files
- Ask permission to write review result files
- Ask whether the provided `--project-root` fixture path exists or should be created
- Make migration or profiling decisions
- Override the test generator's ground truth output (captured proc results are facts)

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `migrate context` | 1 | Prerequisite missing. Return valid `TestReviewResult` JSON with `status: "error"` and code `CONTEXT_PREREQUISITE_MISSING` |
| `migrate context` | 2 | IO/parse error. Return valid `TestReviewResult` JSON with `status: "error"` and code `CONTEXT_IO_ERROR` |
| `test-specs/<item_id>.json` | missing | Return valid `TestReviewResult` JSON with `status: "error"` and code `TEST_SPEC_MISSING` |

Return a valid `TestReviewResult` JSON for all error paths.
