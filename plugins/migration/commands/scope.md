---
name: scope
description: >
  Batch scoping command — identifies writer procedures for each table.
  Delegates per-item scoping to the /scoping-table skill.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Scope

Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable. Delegates per-item scoping to the `/scoping-table` skill.

## Pipeline

### Step 1 — Scope Table (Skill Delegation)

For each item, invoke `/scoping-table <item_id>`. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Collect Result

After the skill completes for an item, read `catalog/tables/<item_id>.json` to confirm scoping was persisted. Write the item result to `.migration-runs/<item_id>.json`:

```json
{
  "item_id": "<table_fqn>",
  "status": "resolved|ambiguous_multi_writer|no_writer_found|error",
  "selected_writer": "<writer_fqn or null>",
  "catalog_path": "catalog/tables/<item_id>.json",
  "warnings": [],
  "errors": []
}
```

The full scoping data lives in the catalog files, not duplicated in the run log.

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_FAILED` | error | `/scoping-table` skill pipeline failed — skip item |
