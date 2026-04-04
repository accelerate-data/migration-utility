# Skill: Reviewing Tests

## Purpose

Quality gate for test generation output. Independently enumerates conditional branches from the stored procedure (without trusting the generator's branch manifest), maps test scenarios against the reviewer's own branch list, scores coverage, reviews fixture quality, and issues a verdict. Can kick back to [[Skill Generating Tests]] with specific feedback or approve the test spec.

## Invocation

```text
/reviewing-tests <schema.table> [--iteration <N>]
```

Argument is the fully-qualified table name (the `item_id`). The optional `--iteration` flag (1-based, default 1) is passed by the `/generate-tests` command to track the review loop.

This skill is typically invoked by the `/generate-tests` command after the generator produces output, not directly by the user.

## Prerequisites

- `manifest.json` must exist in the project root.
- `catalog/tables/<item_id>.json` must exist.
- `test-specs/<item_id>.json` must exist (produced by [[Skill Generating Tests]]).

## Pipeline

### 1. Assemble context

```bash
uv run --project <shared-path> migrate context --table <item_id>
```

Also reads the generator's output from `test-specs/<item_id>.json`.

### 2. Independent branch enumeration

The reviewer enumerates all conditional branches from `proc_body` and `statements` independently. The generator's `branch_manifest` is not read or trusted -- the reviewer builds its own from scratch.

Uses the same coverage model:

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

Each branch gets a stable `id` (snake_case, descriptive) and human-readable `description`.

### 3. Map scenarios to branches

For each scenario in `unit_tests[]`, the reviewer determines which branches (from its own enumeration) the scenario exercises by analyzing the `given` fixture rows against the proc logic -- not by trusting scenario names or descriptions.

Rules:

- A scenario may cover multiple branches
- A branch may require multiple scenarios
- Mapping is based on fixture analysis, not naming conventions

### 4. Score coverage

| Metric | Definition |
|---|---|
| `total_branches` | Count of branches from reviewer's enumeration |
| `covered_branches` | Branches with at least one mapped scenario |
| `untestable_branches` | Branches that cannot be tested with static fixtures |
| `score` | `complete` if covered + untestable = total; `partial` otherwise |
| `uncovered` | Branch objects with zero mapped scenarios and not untestable |
| `untestable` | Branch objects with rationale explaining why static fixtures cannot reproduce them |

A branch is **untestable** when it depends on runtime state: `GETDATE()`/`SYSDATETIME()` comparisons, dynamic SQL with variable targets, external service calls, or non-deterministic functions. Each untestable classification requires a `rationale`.

### 5. Review fixture quality

Each scenario is assessed on four dimensions:

| Dimension | What is checked |
|---|---|
| Fixture realism | Type-appropriate and reasonable values (no negative prices, future dates for historical data) |
| Scenario isolation | Each scenario tests one branch clearly, not multiple tangled branches |
| FK consistency | Foreign key values align across source tables within each scenario |
| Edge cases | Boundary values present where appropriate (NULLs, empty strings, MAX values, zero-row inputs) |

Issues are recorded with scenario name, description, and severity (`warning` or `error`).

### 6. Verdict

| Condition | Verdict | Status |
|---|---|---|
| All testable branches covered + quality acceptable | Approve | `approved` |
| Testable coverage gaps identified | Kick back | `revision_requested` |
| Quality issues found | Kick back | `revision_requested` |
| Both coverage gaps and quality issues | Kick back | `revision_requested` |
| Iteration 2 and issues remain | Approve with warnings | `approved_with_warnings` |

**Maximum review iterations: 2.** If `--iteration 2` and issues remain, the reviewer approves with warnings rather than looping further, flagging the item for human review.

## Reads

| File | Description |
|---|---|
| `manifest.json` | Project root validation |
| `catalog/tables/<item_id>.json` | Table catalog for context assembly |
| `catalog/procedures/<writer>.json` | Writer procedure for context assembly |
| `test-specs/<item_id>.json` | Generator's test output to review |

## Writes

None. The reviewer does not modify `test-specs/` or any catalog files. It emits a `TestReviewResult` JSON structure as output.

## JSON Format

### TestReviewResult output

```json
{
  "item_id": "silver.dimproduct",
  "status": "revision_requested",
  "reviewer_branch_manifest": [
    {
      "id": "merge_matched_update",
      "description": "MERGE WHEN MATCHED -> UPDATE existing product",
      "covered": true,
      "covering_scenarios": ["test_merge_matched_existing_product_updated"]
    },
    {
      "id": "merge_not_matched_insert",
      "description": "MERGE WHEN NOT MATCHED -> INSERT new product",
      "covered": true,
      "covering_scenarios": ["test_merge_not_matched_new_product_inserted"]
    },
    {
      "id": "left_join_null_category",
      "description": "LEFT JOIN to category table -- no matching category (NULL right side)",
      "covered": false,
      "covering_scenarios": []
    },
    {
      "id": "getdate_expiry_check",
      "description": "WHERE ExpiryDate < GETDATE() -- runtime date comparison",
      "covered": false,
      "covering_scenarios": []
    }
  ],
  "coverage": {
    "total_branches": 8,
    "covered_branches": 6,
    "untestable_branches": 1,
    "score": "partial",
    "uncovered": [
      {
        "id": "left_join_null_category",
        "description": "LEFT JOIN to category table -- no matching category (NULL right side)"
      }
    ],
    "untestable": [
      {
        "id": "getdate_expiry_check",
        "description": "WHERE ExpiryDate < GETDATE() -- runtime date comparison",
        "rationale": "Branch depends on current system time; static fixtures cannot control GETDATE() output"
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

### TestReviewResult fields

| Field | Type | Description |
|---|---|---|
| `item_id` | string | Fully qualified table name |
| `status` | string | Enum: `approved`, `approved_with_warnings`, `revision_requested`, `error` |
| `reviewer_branch_manifest` | array | Reviewer's independently enumerated branches with coverage mapping |
| `coverage` | object | Coverage scoring results |
| `quality_issues` | array | Per-scenario quality concerns |
| `feedback_for_generator` | object | Structured feedback for the generator (only when `status` is `revision_requested`) |
| `warnings` | array | Diagnostics entries |
| `errors` | array | Diagnostics entries |

### `reviewer_branch_manifest[]` entry

| Field | Type | Description |
|---|---|---|
| `id` | string | Stable branch identifier (snake_case) |
| `description` | string | Human-readable branch description |
| `covered` | boolean | Whether at least one scenario exercises this branch |
| `covering_scenarios` | string[] | Test names that exercise this branch |

### `coverage` object

| Field | Type | Description |
|---|---|---|
| `total_branches` | integer | Total branches from reviewer's enumeration |
| `covered_branches` | integer | Branches with at least one mapped scenario |
| `untestable_branches` | integer | Branches that cannot be tested with static fixtures |
| `score` | string | `complete` or `partial` |
| `uncovered` | array | Uncovered testable branch objects (`id`, `description`) |
| `untestable` | array | Untestable branch objects (`id`, `description`, `rationale`) |

### `feedback_for_generator` object

| Field | Type | Description |
|---|---|---|
| `uncovered_branches` | string[] | Branch IDs missing coverage (excludes untestable) |
| `quality_fixes` | string[] | Per-scenario remediation instructions |

### Diagnostics schema

`warnings[]` and `errors[]` use the shared diagnostics schema:

| Field | Type | Required | Description |
|---|---|---|---|
| `code` | string | yes | Stable machine-readable identifier (e.g., `COVERAGE_INCOMPLETE`, `FIXTURE_UNREALISTIC`, `CONTEXT_MISSING`) |
| `message` | string | yes | Human-readable description |
| `item_id` | string | no | Fully qualified table name |
| `severity` | string | yes | Enum: `error`, `warning` |
| `details` | object | no | Optional structured context |

## Boundary Rules

The test reviewer must not:

- Generate or modify fixture data
- Execute stored procedures
- Write to `test-specs/` -- only the test generator writes there
- Make migration or profiling decisions
- Override the test generator's ground truth output (captured proc results are facts)

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `migrate context` exit code 1 | Prerequisite missing (no profile, no writer, no statements) | Run scoping and profiling first. Status set to `error` with code `CONTEXT_PREREQUISITE_MISSING` |
| `migrate context` exit code 2 | IO/parse error | Check catalog files. Status set to `error` with code `CONTEXT_IO_ERROR` |
| `test-specs/<item_id>.json` missing | Test generator has not run | Run [[Skill Generating Tests]] first. Status set to `error` with code `TEST_SPEC_MISSING` |
| Iteration 2 still has gaps | Generator could not fully address reviewer feedback | Approved with warnings -- flagged for human review |
