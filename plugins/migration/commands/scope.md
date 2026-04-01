---
name: scope
description: >
  Batch scoping command — identifies writer procedures for each table.
  Delegates per-item scoping to the /scoping-table skill.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Scope

Identify which procedures write to each table. Launches one sub-agent per table in parallel, each running `migration:scoping-table`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- Per item: `catalog/tables/<item_id>.json` must exist. If missing, skip with `CATALOG_FILE_MISSING`.

## Pipeline

### Step 1 — Setup

1. Read worktree base path from `.claude/rules/git-workflow.md`.
2. Generate run slug: `feature/scope-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters after `feature/`).
3. Create worktree: `mkdir -p <base>/feature && git worktree add <base>/<slug> -b <slug>`.
4. In the worktree, clear `.migration-runs/` and write `meta.json`:

```json
{
  "command": "scope",
  "tables": ["silver.DimCustomer", "silver.DimProduct"],
  "worktree": "../worktrees/feature/scope-silver-dimcustomer-silver-dimproduct",
  "started_at": "2026-04-01T12:00:00Z"
}
```

### Step 2 — Run migration:scoping-table per table

Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the migration:scoping-table skill for <schema.table>.
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
   scope complete — N tables processed

     ✓ silver.DimCustomer    resolved
     ✓ silver.DimProduct     resolved
     ✗ silver.DimDate        error (CATALOG_FILE_MISSING)

     resolved: 2 | error: 1
   ```

4. Ask FDE: commit and open PR? PR body format:

   ```markdown
   ## Scoping — N tables

   | Table | Status | Writer |
   |---|---|---|
   | silver.DimCustomer | resolved | dbo.usp_load_dimcustomer |
   | silver.DimProduct | resolved | dbo.usp_load_dimproduct |
   | silver.DimDate | error | CATALOG_FILE_MISSING |
   ```

### Cleanup

After PR is merged:

1. `git push origin --delete <branch>`
2. `git worktree remove <worktree-path>`
3. `git branch -d <branch>`

## Item Result Schema

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

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "CATALOG_FILE_MISSING", "message": "catalog/tables/silver.dimdate.json not found.", "item_id": "silver.dimdate", "severity": "error"}
```
