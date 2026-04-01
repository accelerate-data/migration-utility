# Test Reviewer Skill Contract

The test reviewer skill is an LLM-based quality gate for test generation. It independently enumerates branches from the proc, maps the test generator's scenarios against its own branch list, scores coverage, and reviews fixture quality. It can kick back to the test generator with specific missing branches.

## Philosophy and Boundary

- Test reviewer owns authoritative coverage scoring. The test generator's self-assessment is informational; the reviewer's is canonical.
- Test reviewer reads the same source context as the test generator (proc body, statements, profile) to perform independent branch enumeration. It does NOT trust the generator's branch manifest — it builds its own.
- Test reviewer reads the generator's output from `test-specs/<item_id>.json`.
- Test reviewer can request additional test cases by kicking back to the test generator with a list of uncovered branches.
- Test reviewer does not generate fixtures, execute procs, or modify test specs directly.

## Review Strategy

### 1. IndependentBranchEnumeration (LLM)

Read the proc body and resolved statements from catalog. Independently enumerate all conditional branches using the same coverage model as the test generator:

| Pattern | Branches |
|---|---|
| MERGE WHEN clauses | One per clause |
| CASE/WHEN | One per arm + ELSE |
| JOIN | Match, no-match, NULL (LEFT JOIN), partial multi-condition |
| WHERE | Pass, fail |
| Subquery | EXISTS true/false, IN match/miss |
| NULL handling | NULL vs non-NULL in filters/joins/COALESCE |
| Aggregation | Single group, multiple groups, empty |
| Type boundaries | Watermark edges, MAX int, empty string |
| Empty source | Zero-row edge case |

### 2. MapScenariosToOwnBranches

Read the test generator's output from `test-specs/<item_id>.json`. For each `unit_tests[]` entry, determine which branches (from the reviewer's own enumeration) the scenario exercises.

- A scenario may cover multiple branches.
- A branch may require multiple scenarios to cover fully.

### 3. ScoreCoverage

Coverage = branches with at least one mapped scenario / total branches.

- `complete`: all branches covered.
- `partial`: some branches uncovered after generator's max iterations.

### 4. ReviewFixtureQuality (LLM)

For each test scenario, assess:

- **Fixture realism:** Are synthetic values type-appropriate and reasonable? (e.g., negative prices, future dates for historical data)
- **Scenario isolation:** Does each scenario test one branch clearly, or are multiple branches tangled?
- **FK consistency:** Do foreign key values in fixtures align across source tables within each scenario?
- **Edge cases:** Are boundary values present where appropriate (NULLs, empty strings, MAX values)?
- **Ground truth plausibility:** Does the `expect.rows` output look consistent with the proc logic and `given` inputs?

### 5. Verdict

| Condition | Action |
|---|---|
| Coverage complete, quality acceptable | Approve — proceed to migration |
| Coverage gaps identified | Kick back to test generator with specific uncovered branches |
| Quality issues found | Kick back with specific fixture concerns |
| Both gaps and quality issues | Kick back with both |
| Max review iterations reached | Approve with warnings — flag for human review |

Maximum review / generator iterations: 2 (configurable).

## Output Schema (TestReviewResult)

Per-item output:

```json
{
  "item_id": "silver.dimproduct",
  "status": "approved|revision_requested|error",
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

## Test Reviewer Boundary

Test reviewer must not:

- Generate or modify fixture data
- Execute stored procedures
- Write to `test-specs/` — only the test generator writes there
- Make migration or profiling decisions
- Override the test generator's ground truth output (captured proc results are facts)

`warnings[]` and `errors[]` use the shared diagnostics schema in `README.md`.
