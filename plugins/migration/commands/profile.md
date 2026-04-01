---
name: profile
description: >
  Batch profiling command — produces migration profiles for each table.
  Delegates per-item profiling to the /profiling-table skill.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Profile

Produce migration profiles for each table. Launches one sub-agent per table in parallel, each running `migration:profiling-table`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- Per item: `catalog/tables/<item_id>.json` must exist. If missing, skip with `CATALOG_FILE_MISSING`.
- Per item: `scoping.selected_writer` must be set. If missing, skip with `SCOPING_NOT_COMPLETED`.

## Pipeline

### Step 1 — Setup

1. Read worktree base path from `.claude/rules/git-workflow.md`.
2. Generate run slug: `profile-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters if too long).
3. Create worktree: `git worktree add <base>/<slug> -b <slug>`.
4. In the worktree, clear `.migration-runs/` and write `meta.json`:

```json
{
  "command": "profile",
  "tables": ["silver.DimCustomer", "silver.DimProduct"],
  "worktree": "../worktrees/profile-silver-dimcustomer-silver-dimproduct",
  "started_at": "2026-04-01T12:00:00Z"
}
```

### Step 2 — Run migration:profiling-table per table

Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the migration:profiling-table skill for <schema.table>.
The worktree is at <worktree-path>.
Write the item result JSON to .migration-runs/<schema.table>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 3 — Summarize

1. Read each `.migration-runs/<schema.table>.json`.
2. Write `.migration-runs/summary.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   profile complete — N tables processed

     ok      silver.DimCustomer
     partial silver.DimProduct     (PARTIAL_PROFILE)
     error   silver.DimDate        (CATALOG_FILE_MISSING)

     ok: 1 | partial: 1 | error: 1
   ```

4. Ask FDE: commit and open PR? PR body format:

   ```markdown
   ## Profiling — N tables

   | Table | Status | Classification | Primary Key |
   |---|---|---|---|
   | silver.DimCustomer | ok | dimension | CustomerKey |
   | silver.DimProduct | partial | dimension | — |
   | silver.DimDate | error | — | SCOPING_NOT_COMPLETED |
   ```

### Cleanup

After PR is merged:

1. `git push origin --delete <branch>`
2. `git worktree remove <worktree-path>`
3. `git branch -d <branch>`

## `source` Field Semantics

- `"catalog"` — fact from setup-ddl catalog data. Not inferred.
- `"llm"` — inferred by LLM from proc body / column patterns / reference tables.
- `"catalog+llm"` — catalog provided the base fact, LLM added classification.

## `status` Field

- `ok` — required questions answered (classification, primary_key, watermark).
- `partial` — one or more required questions unanswered.
- `error` — runtime failure prevented profiling.

## Item Result Schema

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "catalog_path": "catalog/tables/<item_id>.json",
  "warnings": [],
  "errors": []
}
```

The actual profile data lives in the catalog file, not duplicated in the run log.

## Error and Warning Codes

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILING_FAILED` | error | `/profiling-table` skill pipeline failed — skip item |
| `PARTIAL_PROFILE` | warning | LLM could not answer a required question — item proceeds as partial |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "PARTIAL_PROFILE", "message": "Could not determine watermark column for silver.dimcustomer.", "item_id": "silver.dimcustomer", "severity": "warning"}
```
