---
name: reviewing-tests
description: >
  Reviews test generation output for coverage and quality. Independently
  enumerates branches, scores coverage, and reviews fixture quality.
  Invoked by the /generate-tests command, not directly by FDE.
user-invocable: false
argument-hint: "<schema.table>"
---

# Reviewing Tests

Quality gate for test generation output. Independently enumerates conditional branches from the stored procedure, maps the test generator's scenarios against the reviewer's own branch list, scores coverage, reviews fixture quality, and issues a verdict.

## Arguments

`$ARGUMENTS` is the fully-qualified table name (the `item_id`), optionally followed by `--iteration <N>` (1-based). Defaults to 1 if not provided. The `/generate-tests` command passes the current iteration number.

## Before invoking

1. Read `manifest.json` from the current working directory to confirm a valid project root. If missing, tell the caller that the project is not initialized and stop.
2. Confirm `catalog/tables/<item_id>.json` exists. If missing, stop — this skill only operates on tables.
3. Confirm `test-specs/<item_id>.json` exists. If missing, tell the caller to run `/generating-tests` first and stop.

## Step 1: Assemble context

Run the deterministic context assembly CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" migrate context \
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

This file contains `unit_tests[]` — the scenarios and fixtures produced by the test generator.

## Step 2: Independent branch enumeration

Enumerate all conditional branches from `proc_body` and `statements` independently. Do NOT read or trust the generator's `branch_manifest` — build your own from scratch.

Use the same coverage model and branch pattern table as the test generator:

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
- Map by analyzing the `given` fixture rows and `expect.rows` against the proc logic — not by trusting scenario names or descriptions alone.

## Step 4: Score coverage

Compute coverage:

- **total_branches**: count of branches from the reviewer's enumeration.
- **covered_branches**: count of branches with at least one mapped scenario.
- **score**: `complete` if all branches are covered, `partial` otherwise.
- **uncovered**: list of branch objects (`id`, `description`) that have zero mapped scenarios.

## Step 5: Review fixture quality

For each test scenario in `unit_tests[]`, assess these dimensions:

- **Fixture realism:** Are synthetic values type-appropriate and reasonable? Flag unrealistic values like negative prices, future dates for historical data, or strings in numeric fields.
- **Scenario isolation:** Does each scenario test one branch clearly, or are multiple branches tangled in a way that makes failure diagnosis ambiguous?
- **FK consistency:** Do foreign key values in fixture rows align across source tables within each scenario? A row referencing `customer_key = 42` in the fact table should have a matching `customer_key = 42` in the dimension fixture.
- **Edge cases:** Are boundary values present where appropriate (NULLs, empty strings, MAX values, zero-row inputs)?

Record each issue with the scenario name, a description of the concern, and a severity (`warning` or `error`).

## Step 6: Verdict

Apply the following verdict rules:

| Condition | Action |
|---|---|
| Coverage complete + quality acceptable | **Approve** — set `status` to `approved` |
| Coverage gaps identified | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_generator.uncovered_branches` with the branch IDs that lack scenarios |
| Quality issues found | **Kick back** — set `status` to `revision_requested`, populate `feedback_for_generator.quality_fixes` with specific remediation instructions per scenario |
| Both coverage gaps and quality issues | **Kick back** — set `status` to `revision_requested`, populate both feedback fields |
| Iteration 2 and issues remain | **Approve with warnings** — set `status` to `approved_with_warnings`, add a warning entry flagging the item for human review |

Maximum review iterations: 2. If `--iteration 2` and issues remain, approve with warnings rather than looping further.

## Output schema (TestReviewResult)

Emit the following JSON structure as the skill's output:

```json
{
  "item_id": "silver.dimproduct",
  "status": "approved|approved_with_warnings|revision_requested|error",
  "reviewer_branch_manifest": [
    {
      "id": "merge_not_matched_insert",
      "description": "MERGE WHEN NOT MATCHED → INSERT new product",
      "covered": true,
      "covering_scenarios": ["test_merge_not_matched_new_product_inserted"]
    }
  ],
  "coverage": {
    "total_branches": 8,
    "covered_branches": 7,
    "score": "partial",
    "uncovered": [
      {
        "id": "left_join_null_category",
        "description": "LEFT JOIN to category table — no matching category (NULL right side)"
      }
    ]
  },
  "quality_issues": [
    {
      "scenario": "test_merge_matched_existing_product_updated",
      "issue": "Fixture uses negative list_price (-10.00) which is unrealistic for this domain",
      "severity": "warning"
    }
  ],
  "feedback_for_generator": {
    "uncovered_branches": ["left_join_null_category"],
    "quality_fixes": ["Use realistic positive prices in test_merge_matched_existing_product_updated"]
  },
  "warnings": [],
  "errors": []
}
```

`warnings[]` and `errors[]` use the shared diagnostics schema:

```json
{
  "code": "STABLE_MACHINE_READABLE_CODE",
  "message": "Human-readable description of the diagnostic.",
  "item_id": "silver.dimproduct",
  "severity": "error|warning",
  "details": {}
}
```

Field requirements:

- `code`: stable machine-readable identifier (e.g., `COVERAGE_INCOMPLETE`, `FIXTURE_UNREALISTIC`, `CONTEXT_MISSING`).
- `message`: human-readable description.
- `item_id`: fully qualified table name this entry relates to.
- `severity`: `error` or `warning`.
- `details`: optional structured context object.

## Boundary rules

Test reviewer must not:

- Generate or modify fixture data
- Execute stored procedures
- Write to `test-specs/` — only the test generator writes there
- Make migration or profiling decisions
- Override the test generator's ground truth output (captured proc results are facts)

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `migrate context` | 1 | Prerequisite missing. Report which and set `status: "error"` with code `CONTEXT_PREREQUISITE_MISSING` |
| `migrate context` | 2 | IO/parse error. Surface error message and set `status: "error"` with code `CONTEXT_IO_ERROR` |
| `test-specs/<item_id>.json` | missing | Tell caller to run `/generating-tests` first. Set `status: "error"` with code `TEST_SPEC_MISSING` |
