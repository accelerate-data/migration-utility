---
name: scoping
description: >
  Batch scoping command — identifies writer procedures for each table.
  Delegates per-item scoping to the /scoping-table skill. No approval gates.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Scoping

Given a batch of target tables, identify which procedures write to each and select the single writer when resolvable. Delegates per-item scoping to the `/scoping-table` skill.

This command follows the shared lifecycle in `.claude/rules/command-lifecycle.md`.

## Pipeline

### Step 1 — Scope Table (Skill Delegation)

For each item, invoke `/scoping-table --table <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Collect Result

After the skill completes for an item, read `catalog/tables/<item_id>.json` to confirm scoping was persisted. Write the item result to `.migration-runs/results/<item_id>.json`:

- `item_id` — the table FQN
- `status` — `resolved`, `ambiguous_multi_writer`, `no_writer_found`, or `error`
- `catalog_path` — path to the catalog file

The full scoping data lives in the catalog files, not duplicated in the run log. The result file is for aggregation and status tracking.

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_FAILED` | error | `/scoping-table` skill pipeline failed — skip item |
