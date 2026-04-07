---
name: scope
description: >
  Batch scoping command — identifies writer procedures for each table, and analyzes SQL structure for each view or materialized view.
  Delegates per-item scoping to the /analyzing-table skill.
user-invocable: true
argument-hint: "<schema.table_or_view> [schema.table_or_view ...]"
---

# Scope

Identify which procedures write to each table, or analyze SQL structure for each view or materialized view. Launches one sub-agent per item in parallel, routing tables to `migration:analyzing-table` and views to `migration:analyzing-view`.

## Guards

- `manifest.json` must exist. If missing, tell the user to run `/setup-ddl` first.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `/add-source-tables` to manage source tables.

Per-item guards are checked by the skill via `migrate-util guard`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) when it finishes.

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `scope-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `scope-order-pipeline`, `scope-customer-dims`). The full slug (including the `scope-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns `"main"`: proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. For each FQN argument, detect its object type by checking which catalog file exists:
   - If `catalog/views/<fqn>.json` exists → `view`
   - Else → `table`

   Store the type alongside each FQN for use in Step 2.
4. Generate a run epoch: seconds since Unix epoch (e.g. `1743868200`). All run artifacts use this as a filename suffix.

### Step 2 — Run skill per item

**Single-item path (1 item):** Run the appropriate skill directly in the current conversation — do not launch a sub-agent:

- Table → `migration:analyzing-table`
- View/MV → `migration:analyzing-view`

After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.item>.<epoch>.json`.

If the item status is `error`, immediately revert any files the skill may have partially modified:

```bash
git checkout -- catalog/<object_type>s/<item_id>.json
```

Ignore errors from `git checkout` (the file may not have been modified).

If the item status is not `error`, auto-commit and push: run `/commit catalog/<object_type>s/<item_id>.json`.

Then continue to Step 3.

**Multi-item path (2+ items):** Launch one sub-agent per item in parallel. Each sub-agent receives this prompt:

```text
Run the migration:analyzing-<object_type> skill for <schema.item>.
The working directory is <working-directory>.
Write the item result JSON to .migration-runs/<schema.item>.<epoch>.json.

After writing the result:
- If status == "error": run `git checkout -- catalog/<object_type>s/<item_id>.json` (ignore errors).
- If status != "error": invoke the /commit command with catalog/<object_type>s/<item_id>.json

On failure before writing a result, write result with status: "error" and error details, then revert as above.
Return the item result JSON.
```

### Step 3 — Summarize

1. Read each `.migration-runs/<schema.item>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   scope complete — N items processed

     ✓ silver.DimCustomer      resolved   (table)
     ✓ silver.DimProduct       resolved   (table)
     ✓ silver.vw_Sales         analyzed   (view)
     ✗ silver.DimDate          error      (table, CATALOG_FILE_MISSING)

     resolved: 2 | analyzed: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. Ask the user:

   > All successful items have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr scope <comma-separated list of successfully processed items>`.
   After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <working-directory>  (omit this line if on main)
   ```

   If on a feature branch, also tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."

6. Suggest running `/status` to see overall migration readiness across all tables.

## Item Result Schema

```json
{
  "item_id": "<fqn>",
  "object_type": "table|view",
  "status": "resolved|ambiguous_multi_writer|no_writer_found|analyzed|error",
  "selected_writer": "<writer_fqn or null>",
  "catalog_path": "catalog/<object_type>s/<item_id>.json",
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
| `CATALOG_FILE_MISSING` | error | catalog/views/\<item_id>.json not found — skip item |
| `SCOPING_FAILED` | error | `/analyzing-table` skill pipeline failed — skip item |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "CATALOG_FILE_MISSING", "message": "catalog/tables/silver.dimdate.json not found.", "item_id": "silver.dimdate", "severity": "error"}
```
