# Model Review Output

`/reviewing-model` returns exactly one `ModelReviewResult` JSON object.

Required top-level fields:

- `item_id`
- `status`
- `checks`
- `feedback_for_model_generator`
- `warnings`
- `errors`

`checks` contains exactly:

- `standards`
- `correctness`
- `test_integration`

Each check is:

```json
{ "passed": true, "issues": [] }
```

Always include `passed` explicitly. Do not return `{ "issues": [] }` without the boolean.

`feedback_for_model_generator` is always an array of objects. Do not use strings.
Each item must include:

- `code`
- `message`
- `severity`
- `ack_required`

Example revision shape:

```json
{
  "item_id": "silver.dimproduct",
  "status": "revision_requested",
  "checks": {
    "standards": {
      "passed": false,
      "issues": [
        {
          "code": "REVIEW_STANDARDS_VIOLATION",
          "message": "SQL style violation",
          "severity": "error"
        }
      ]
    },
    "correctness": { "passed": true, "issues": [] },
    "test_integration": { "passed": false, "issues": [] }
  },
  "feedback_for_model_generator": [
    {
      "code": "SQL_001",
      "message": "Keywords should be lowercase — found uppercase SELECT on line 4",
      "severity": "error",
      "ack_required": true
    }
  ],
  "warnings": [
    {
      "code": "REVIEW_KICKED_BACK",
      "message": "Reviewer requested another generation pass",
      "severity": "warning"
    }
  ],
  "errors": []
}
```

Severity rules:

- `checks.*.issues[]`, `warnings[]`, and `errors[]` use only `error` or `warning`
- `feedback_for_model_generator[]` may use `error`, `warning`, or `info`
- `ack_required` is `true` for `error` and `warning`, `false` for `info`

Acknowledgement rules:

- `acknowledgements` is a flat map of `{ "<code>": "fixed" | "ignored: <reason>" }`
- include it only on resubmission

Statuses:

- `approved`
- `revision_requested`
- `error`

`ModelReviewResult` schema permits `approved_with_warnings` for caller-owned aggregation, but `/reviewing-model` must not emit it.
