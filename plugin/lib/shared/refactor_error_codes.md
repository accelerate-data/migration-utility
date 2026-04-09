# Refactor Error Codes

Use only these surfaced codes for `/refactor` and `/refactoring-sql`.

## Statuses

- `ok`
- `partial`
- `error`

## Codes

| Code | Severity | Use when | Status |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | error | `manifest.json` is missing | `error` |
| `SANDBOX_NOT_CONFIGURED` | error | sandbox metadata is missing from the manifest | `error` |
| `SANDBOX_NOT_RUNNING` | error | live sandbox execution is required but unavailable | `error` |
| `CONTEXT_PREREQUISITE_MISSING` | error | `refactor context` cannot assemble required catalog/profile/test-spec inputs | `error` |
| `CONTEXT_IO_ERROR` | error | `refactor context` failed due to IO or parse problems | `error` |
| `EQUIVALENCE_PARTIAL` | warning | semantic review found unresolved differences, or executable compare was skipped in harness mode | `partial` |
| `COMPARE_SQL_FAILED` | warning | executable compare ran and at least one scenario failed equivalence | `partial` |
| `REFACTOR_WRITE_FAILED` | error | `refactor write` rejected the payload or could not persist it | `error` |

## Rules

- Do not invent new surfaced `/refactor` codes in commands or skills.
- `ok` requires semantic review to pass and executable compare to pass when compare is required.
- Harness mode should normally persist `partial`, not `ok`.
