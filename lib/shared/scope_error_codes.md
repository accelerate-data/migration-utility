# Scope Error Codes

Use only these statuses and surfaced codes for `/scope-tables`.

This file is the shared source of truth for the `/scope-tables` command and the `analyzing-table` skill.

## Statuses

`/scope-tables` item results may use only these statuses:

- `resolved`
- `ambiguous_multi_writer`
- `no_writer_found`
- `analyzed`
- `error`

## Codes

| Code | Severity | Use when | Status |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | error | `manifest.json` is missing before `/scope-tables` starts | `error` |
| `CATALOG_FILE_MISSING` | error | the required table or view catalog file does not exist | `error` |
| `SCOPING_FAILED` | error | scoping could not complete and no more specific canonical code applies | `error` |
| `SOURCE_TABLE` | error | the item is already marked `is_source: true` and is not applicable to `/scope-tables` | `error` |
| `EXCLUDED` | error | the item is excluded from the migration pipeline | `error` |
| `WRITERLESS_TABLE` | error | a readiness check hit a table whose scoping status is `no_writer_found` | `error` |
| `MULTI_TABLE_WRITE` | warning | the candidate writer updates multiple tables and needs disambiguation or slicing | `ambiguous_multi_writer` or `resolved` |
| `REMOTE_EXEC_UNSUPPORTED` | error | the apparent writer delegates through cross-database or linked-server `EXEC` | `error` |
| `PARSE_ERROR` | error | procedure or other DDL failed to parse | `error` |
| `DDL_PARSE_ERROR` | error | view DDL failed to parse | `analyzed` or `error` |
| `MISSING_REFERENCE` | warning | an in-scope reference has no catalog entry | usually non-terminal |
| `OUT_OF_SCOPE_REFERENCE` | warning | the object references an external object outside migration scope | non-terminal unless it blocks resolution |

## Rules

- Do not invent new `/scope-tables` surfaced codes in commands or skills.
- If a lower-level failure has no canonical code here, use `SCOPING_FAILED` and preserve the raw detail in `message`.
- Use `severity: "error"` only for conditions that must persist or surface as `status: error`.
- Use `severity: "warning"` for conditions that may still allow `resolved`, `ambiguous_multi_writer`, or `analyzed`.
- `REMOTE_EXEC_UNSUPPORTED` is the canonical code for unsupported external procedure delegation during `/scope-tables`.
- Keep command docs and skill docs aligned to this file.
