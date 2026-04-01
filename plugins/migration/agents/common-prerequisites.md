# Common Agent Prerequisites

Shared prerequisite checks for all batch agents in this plugin. Each agent references this file instead of duplicating the checks.

## Batch-wide checks

Before processing any items:

1. Read `manifest.json` from the current working directory for `technology` and `dialect`. If missing or unreadable, fail **all** items with code `MANIFEST_NOT_FOUND` and write output immediately.

## Per-item checks

Before running the skill for each item:

1. Check `catalog/tables/<item_id>.json` exists. If missing, skip this item with `CATALOG_FILE_MISSING` in `errors[]`.

## Additional per-item checks (agent-specific)

Some agents require additional per-item checks beyond the common ones above. These are documented in each agent's own Prerequisites section.

## Output integrity

Never set `status: "ok"` if `errors[]` is non-empty or a required step was skipped. The `status` field must reflect the actual outcome.
