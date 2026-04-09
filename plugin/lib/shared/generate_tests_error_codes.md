# Generate Tests Error Codes

Use only these statuses and surfaced codes for `/generate-tests` and
`/reviewing-tests`.

This file is the shared source of truth for the `/generate-tests` command, the
`generating-tests` skill, and the `reviewing-tests` skill.

## Statuses

`/generate-tests` item results may use only these statuses:

- `ok`
- `partial`
- `error`

`/reviewing-tests` results may use only these statuses:

- `approved`
- `approved_with_warnings`
- `revision_requested`
- `error`

## Codes

| Code | Severity | Use when | Status |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | error | `manifest.json` is missing before `/generate-tests` starts | `error` |
| `SANDBOX_NOT_CONFIGURED` | error | `manifest.json` has no `sandbox.database` | `error` |
| `SANDBOX_NOT_RUNNING` | error | sandbox-status check failed | `error` |
| `CATALOG_FILE_MISSING` | error | required table or view catalog file is missing | `error` |
| `SCOPING_NOT_COMPLETED` | error | scoping is missing, unresolved, or not analyzed for the target object | `error` |
| `PROFILE_NOT_COMPLETED` | error | profile is missing or not complete enough for test generation | `error` |
| `TEST_GENERATION_FAILED` | error | `/generating-tests` could not produce a usable test spec | `error` |
| `TEST_SPEC_MISSING` | error | `test-specs/<item_id>.json` is missing when review starts | `error` |
| `CONTEXT_PREREQUISITE_MISSING` | error | `migrate context` could not assemble because a prerequisite is missing | `error` |
| `CONTEXT_IO_ERROR` | error | `migrate context` failed with an IO or parse error | `error` |
| `REVIEW_KICKED_BACK` | warning | reviewer requested another generation pass | `revision_requested` |
| `COVERAGE_PARTIAL` | warning | coverage is still incomplete after the allowed review loop | `partial` or `approved_with_warnings` |
| `SCENARIO_EXECUTION_FAILED` | warning | one or more scenarios failed during ground-truth capture | `partial` |
| `FIXTURE_QUALITY_WARNING` | warning | fixture realism or isolation issues were found but review can proceed | `revision_requested` or `approved_with_warnings` |
| `STALE_BRANCH` | warning | a branch in the stored manifest was not found in re-extracted SQL — procedure may have changed since spec was written | `partial` |

## Rules

- Do not invent new surfaced codes in the command or skill.
- If a lower-level failure has no canonical code here, use `TEST_GENERATION_FAILED`
  for `/generate-tests` and preserve the raw detail in `message`.
- Use `severity: "error"` only for conditions that must stop the workflow.
- Use `severity: "warning"` for conditions that still allow review, partial
  completion, or approval with warnings.
- Keep the command and skill docs aligned to this file.
