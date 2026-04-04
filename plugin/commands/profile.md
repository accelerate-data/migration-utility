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

1. Generate run slug: `profile-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters).
2. Check for existing worktrees. If any exist, list them and ask the user:
   > Existing worktrees:
   >
   > 1. `feature/scope-silver-dimcustomer`
   > 2. `feature/profile-silver-dimcustomer`
   >
   > - **Continue on #N** — add to that branch
   > - **New worktree** — create a new worktree from the slug
   If none exist, proceed to the **New worktree** path below.
3. **New worktree**: create the worktree and branch per `.claude/rules/git-workflow.md`, then clear `.migration-runs/`. **Continue on existing**: skip clearing — preserve existing `.migration-runs/`.

### Step 2 — Run migration:profiling-table per table

**Single-table path (1 table):** Run `migration:profiling-table` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the migration:profiling-table skill for <schema.table>.
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
   profile complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (PARTIAL_PROFILE)
     ✗ silver.DimDate        error (CATALOG_FILE_MISSING)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, skip commit/PR — report errors only and stop.
5. Ask the user: commit and push? Stage only files changed by successful items (catalog JSON files). Do not stage `.migration-runs/`. Check for an existing open PR on the branch via `gh pr list --head <slug> --state open --json number,url`. If one exists, update it with `gh pr edit` instead of creating a new PR. PR body format:

   ```markdown
   ## Profiling — N tables

   | Table | Status | Classification | Primary Key |
   |---|---|---|---|
   | silver.DimCustomer | ok | dimension | CustomerKey |
   | silver.DimProduct | partial | dimension | — |
   | silver.DimDate | error | — | SCOPING_NOT_COMPLETED |
   ```

6. After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <worktree-path>

   Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches.
   ```

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
