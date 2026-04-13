# Error Handling

Return valid `TestReviewOutput` JSON for all error paths.

| Command | Exit code | Action |
|---|---|---|
| `migrate context` | 1 | Return `status: "error"` with code `CONTEXT_PREREQUISITE_MISSING` |
| `migrate context` | 2 | Return `status: "error"` with code `CONTEXT_IO_ERROR` |
| `discover show` | 1 | Return `status: "error"` with code `CONTEXT_PREREQUISITE_MISSING` |
| `discover show` | 2 | Return `status: "error"` with code `CONTEXT_IO_ERROR` |
| `test-specs/<item_id>.json` | missing | Return `status: "error"` with code `TEST_SPEC_MISSING` |
| `test-harness validate-review` | 1 | Fix the reported JSON fields and retry |

Use surfaced codes from `../../lib/shared/generate_tests_error_codes.md`.
