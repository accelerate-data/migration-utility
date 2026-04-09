# Status Error Codes

Use only these surfaced codes and workflow actions for `/status`.

This file is the shared source of truth for the `/status` command.

## Codes

| Code | Severity | Use when | Action |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | error | `manifest.json` is missing before `/status` starts | stop and tell the user to run `/setup-ddl` |
| `CATALOG_NOT_FOUND` | error | `catalog/tables/` has no catalog files before `/status` starts | stop and tell the user to run `/setup-ddl` |
| `STATUS_FAILED` | error | `migrate-util status` or related status collection failed and no more specific canonical code applies | stop and surface the failure detail |
| `BATCH_PLAN_FAILED` | error | `migrate-util batch-plan` failed and the dashboard cannot compute next steps | stop and surface the failure detail |
| `SYNC_EXCLUDED_WARNINGS_FAILED` | error | `migrate-util sync-excluded-warnings` failed before batch planning | stop and surface the failure detail |
| `STALE_OBJECT` | warning | batch-plan or catalog diagnostics report a stale catalog object | offer stale-file cleanup if the user confirms |
| `CIRCULAR_REFERENCE` | warning | batch-plan excluded one or more objects because of circular references | show the exclusion inline and keep building the dashboard |

## Rules

- Do not invent new `/status` surfaced codes in the command.
- If a lower-level failure has no canonical code here, use `STATUS_FAILED` and preserve the raw detail in `message`.
- Use `severity: "error"` only for conditions that stop the status workflow.
- Use `severity: "warning"` for conditions that still allow the dashboard to be presented.
- `/status` is a workflow command. It may ask for confirmation before running a follow-on command or deleting stale files.
- In eval or harness mode, present recommendations and stale-file candidates but do not execute follow-on commands or delete files.
