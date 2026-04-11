# Listing Objects Error Codes

Canonical surfaced error/warning codes for the `listing-objects` skill.

This skill is read-only. It does not write catalog state and does not own item-result statuses like `/scope`. The codes here describe user-visible failures from `discover`.

## Surfaced codes

| Code | Severity | Source layer | When |
|---|---|---|---|
| `OBJECT_NOT_FOUND` | error | `discover show` / `discover refs` | requested object has no catalog file |
| `UNSUPPORTED_OBJECT_TYPE` | error | `discover refs` | `refs` was requested for an object type the CLI cannot resolve in that mode |
| `CATALOG_IO_ERROR` | error | `discover list/show/refs` | catalog directory or file could not be read |
| `PARSE_ERROR` | warning | shared diagnostics / discover payload | Object DDL failed to parse, but raw DDL is still available for inspection |
| `DDL_PARSE_ERROR` | warning | discover payload | View DDL failed to parse, but raw DDL is still available for inspection |

## CLI failure handling

- If `discover` fails before returning payload data, surface the failure under one of the canonical codes above.
- `discover list/show` exit code `1` should be surfaced as `OBJECT_NOT_FOUND`.
- `discover list/show/refs` exit code `2` should be surfaced as `CATALOG_IO_ERROR`.
- `discover refs` may return a successful payload with `error` populated instead of exiting nonzero. Surface procedure/unsupported-target payload errors as `UNSUPPORTED_OBJECT_TYPE`.
- `discover refs` payload errors starting with `no catalog file for` should be surfaced as `OBJECT_NOT_FOUND`.
- Parse-related warnings should be surfaced from the payload when present; they should not be promoted to fatal errors unless the CLI itself fails.

## Notes

- This file is the shared source of truth for the `listing-objects` skill's public error surface.
- If a command wrapper is introduced for `listing-objects` later, that command should reference this file or replace it with a command-specific shared contract.
