---
name: profiling
description: >
  Batch profiling command — produces migration profiles for each table.
  Delegates per-item profiling to the /profiling-table skill. No approval gates.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Profiling

Given a batch of target tables, produce migration profile candidates for each table and write them into the table catalog files. Delegates per-item profiling to the `/profiling-table` skill.

## Additional Per-item Guard

Before running the skill for each item (after common guards):

- Check `scoping.selected_writer` is set in the catalog file. If scoping section is missing or `selected_writer` is null, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.

## Pipeline

### Step 1 — Profile Table (Skill Delegation)

For each item, invoke `/profiling-table <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Record Result

Write the item result to `.migration-runs/results/<item_id>.json`:

- `item_id` — the table FQN
- `status` — `ok`, `partial`, or `error`
- `catalog_path` — path to the catalog file

The actual profile data lives in the catalog file, not duplicated in the run log.

## `source` Field Semantics

- `"catalog"` — fact from setup-ddl catalog data. Not inferred.
- `"llm"` — inferred by LLM from proc body / column patterns / reference tables.
- `"catalog+llm"` — catalog provided the base fact, LLM added classification.

## `status` Field

- `ok` — required questions answered (classification, primary_key, watermark).
- `partial` — one or more required questions unanswered.
- `error` — runtime failure prevented profiling.

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILING_FAILED` | error | `/profiling-table` skill pipeline failed — skip item |
| `PARTIAL_PROFILE` | warning | LLM could not answer a required question — item proceeds as partial |
