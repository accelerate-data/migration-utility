# Profile Error Codes

Use only these statuses and surfaced codes for `/profile-tables`.

This file is the shared source of truth for the `/profile-tables` command and the `profiling-table` skill.

## Statuses

`/profile-tables` item results may use only these statuses:

- `ok`
- `partial`
- `error`

## Codes

| Code | Severity | Use when | Status |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | error | `manifest.json` is missing before `/profile-tables` starts | `error` |
| `CATALOG_FILE_MISSING` | error | the required table catalog file does not exist | `error` |
| `VIEW_CATALOG_FILE_MISSING` | error | the required view catalog file does not exist | `error` |
| `SOURCE_TABLE` | error | the item is already marked `is_source: true` and is not applicable to `/profile-tables` | `error` |
| `EXCLUDED` | error | the item is excluded from the migration pipeline | `error` |
| `SCOPING_NOT_COMPLETED` | error | a table is not ready for profiling because scoping is missing or unresolved | `error` |
| `VIEW_SCOPING_NOT_COMPLETED` | error | a view is not ready for profiling because scoping is missing or not analyzed | `error` |
| `PROFILING_FAILED` | error | profiling could not complete and no more specific canonical code applies | `error` |
| `PARTIAL_PROFILE` | warning | one or more profiling questions could not be answered confidently | `partial` |
| `PARSE_ERROR` | warning | procedure or table-side DDL parse limitations reduced confidence but profiling continued | `partial` or `ok` |
| `DDL_PARSE_ERROR` | warning | view DDL parse limitations reduced confidence but profiling continued | `partial` or `ok` |

## Rules

- Do not invent new `/profile-tables` surfaced codes in commands or skills.
- If a lower-level failure has no canonical code here, use `PROFILING_FAILED` and preserve the raw detail in `message`.
- Use `severity: "error"` only for conditions that must persist or surface as `status: error`.
- Use `severity: "warning"` for conditions that may still allow `ok` or `partial`.
- Keep command docs and skill docs aligned to this file.
