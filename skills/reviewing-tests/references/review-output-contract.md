# Review Output Contract

Return one valid `TestReviewOutput` JSON object.

Required fields:

- `item_id`
- `status`
- `reviewer_branch_manifest`
- `coverage`
- `quality_issues`
- `feedback_for_generator`
- `warnings`
- `errors`

Coverage fields:

- `total_branches`
- `covered_branches`
- `untestable_branches`
- `score`
- `uncovered`
- `untestable`

Use `score: "complete"` only when every testable branch is covered.

Example:

```json
{
  "item_id": "silver.dimproduct",
  "status": "revision_requested",
  "reviewer_branch_manifest": [
    {
      "id": "case_status_current",
      "description": "CASE branch maps active rows to Current",
      "covered": true,
      "covering_scenarios": ["test_status_current_when_active_product"]
    }
  ],
  "coverage": {
    "total_branches": 3,
    "covered_branches": 2,
    "untestable_branches": 0,
    "score": "partial",
    "uncovered": [
      {
        "id": "empty_source_bronze_product",
        "description": "Empty source yields no staged rows"
      }
    ],
    "untestable": []
  },
  "quality_issues": [
    {
      "scenario": "test_status_current_when_active_product",
      "issue": "Fixture omits join key required by the reviewed branch",
      "severity": "error"
    }
  ],
  "feedback_for_generator": {
    "uncovered_branches": ["empty_source_bronze_product"],
    "quality_fixes": ["Add the required join key column to the current-row scenario fixture"]
  },
  "warnings": [],
  "errors": []
}
```

Always validate with:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" test-harness validate-review \
  --review-file .staging/review.json
```
