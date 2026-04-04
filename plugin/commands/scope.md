---
name: scope
description: >
  Batch scoping command — identifies writer procedures for each table.
  Delegates per-item scoping to the /analyzing-table skill.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Scope

Identify which procedures write to each table. Launches one sub-agent per table in parallel, each running `migration:analyzing-table`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- Per item: `catalog/tables/<item_id>.json` must exist. If missing, skip with `CATALOG_FILE_MISSING`.

## Pipeline

### Step 1 — Setup

1. Read worktree base path from `.claude/rules/git-workflow.md`.
2. Generate run slug: `feature/scope-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters after `feature/`).
3. Scan for existing worktrees with open PRs: run `git worktree list --porcelain` to find all worktrees under `<base>/`. For each worktree branch, check for an open PR via `gh pr list --head <branch> --state open --json number,title,url`.
   - **One or more worktrees with open PRs found:** list them and ask the user:
     > Existing worktrees with open PRs:
     >
     > 1. `feature/scope-silver-dimcustomer` — PR #42: "Scoping — 1 table"
     > 2. `feature/profile-silver-dimcustomer` — PR #43: "Profiling — 1 table"
     >
     > - **Continue on #N** — add to that branch and update its PR
     > - **New worktree** — create `<slug>` and start fresh
   - **No worktrees with open PRs:** create the new worktree — `mkdir -p <base>/feature && git worktree add <base>/<slug> -b <slug>`, then run `./scripts/setup-worktree.sh <worktree-path>`.
4. **New worktree**: clear `.migration-runs/` and write `meta.json` (see below). **Continue on existing**: skip clearing — preserve existing `.migration-runs/` and `meta.json`.

```json
{
  "command": "scope",
  "tables": ["silver.DimCustomer", "silver.DimProduct"],
  "worktree": "../worktrees/feature/scope-silver-dimcustomer-silver-dimproduct",
  "started_at": "2026-04-01T12:00:00Z"
}
```

### Step 2 — Run migration:analyzing-table per table

**Single-table path (1 table):** Run `migration:analyzing-table` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the migration:analyzing-table skill for <schema.table>.
The worktree is at <worktree-path>.
Write the item result JSON to .migration-runs/<schema.table>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 3 — Revert errored items

For each item with `status: "error"`, revert any files the skill may have partially modified:

```bash
git checkout -- catalog/tables/<item_id>.json
```

Ignore errors from `git checkout` (the file may not have been modified).

### Step 4 — Summarize

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

4. If all items errored, skip commit/PR — report errors only and stop.
5. Ask the user: commit and push? Stage only files changed by successful items (catalog JSON files). Do not stage `.migration-runs/`. Check for an existing open PR on the branch via `gh pr list --head <slug> --state open --json number,url`. If one exists, update it with `gh pr edit` instead of creating a new PR. PR body format:

   ```markdown
   ## Scoping — N tables

   | Table | Status | Writer |
   |---|---|---|
   | silver.DimCustomer | resolved | dbo.usp_load_dimcustomer |
   | silver.DimProduct | resolved | dbo.usp_load_dimproduct |
   | silver.DimDate | error | CATALOG_FILE_MISSING |
   ```

6. After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <worktree-path>

   Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches.
   ```

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
| `SCOPING_FAILED` | error | `/analyzing-table` skill pipeline failed — skip item |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "CATALOG_FILE_MISSING", "message": "catalog/tables/silver.dimdate.json not found.", "item_id": "silver.dimdate", "severity": "error"}
```
