---
name: profile
description: >
  Batch profiling command — produces migration profiles for each table, view, or materialized view.
  Delegates per-item profiling to the /profiling-table skill (auto-detects table vs view).
user-invocable: true
argument-hint: "<schema.table_or_view> [schema.table_or_view ...]"
---

# Profile

Produce migration profiles for each table, view, or materialized view. Launches one sub-agent per item in parallel using `/profiling-table` (which auto-detects table vs view).

## Harness Mode

When the caller explicitly says to skip git operations, worktree creation, or PR steps, treat that as a non-interactive harness mode. Do not ask follow-up questions about those skips.

In harness mode:

- Skip `git-checkpoints`.
- Use the current project root as `<working-directory>`.
- Write `.migration-runs/` artifacts under the caller-specified project root. Create the directory if it does not exist.
- Skip per-item `/commit` calls.
- Skip the final PR prompt and `/commit-push-pr`.
- Still run the full profiling flow and write item/summary artifacts.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `/add-source-tables` to manage source tables.

Per-item readiness is checked by the skill via `migrate-util ready`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) when it finishes.

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `profile-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `profile-customer-dims`, `profile-order-pipeline`). The full slug (including the `profile-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If harness mode is active: skip this step and set `<working-directory>` to the current project root.
   - If it returns the default branch name (not a worktree path): proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. Generate a run epoch: seconds since Unix epoch (e.g. `1743868200`). All run artifacts use this as a filename suffix.

### Step 2 — Route and run per item

**Single-item path (1 item):** Run `/profiling-table` directly in the current conversation — do not launch a sub-agent. The skill auto-detects table vs view from catalog presence. Set `catalog_path` in the item result accordingly (`catalog/views/` or `catalog/tables/`).

After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.item>.<epoch>.json`.

Create `.migration-runs/` first if it does not already exist.

If the item status is `error`, immediately revert any files the skill may have partially modified:

```bash
git checkout -- catalog/tables/<item_id>.json  # or catalog/views/<item_id>.json for views
```

Ignore errors from `git checkout` (the file may not have been modified).

If the item status is not `error`, auto-commit and push: run `/commit catalog/tables/<item_id>.json` (or `catalog/views/<item_id>.json` for views).

If harness mode is active, skip both the revert and commit steps and leave files in place for the eval harness to inspect.

Then continue to Step 3.

**Multi-item path (2+ items):** Launch one sub-agent per item in parallel. Each sub-agent receives this prompt:

```text
Run /profiling-table for <schema.item>. The skill auto-detects table vs view.
The working directory is <working-directory>.
Write the item result JSON to .migration-runs/<schema.item>.<epoch>.json.

Create `.migration-runs/` first if it does not already exist.

After writing the result:
- If status == "error": run `git checkout -- catalog/tables/<item_id>.json` or `catalog/views/<item_id>.json` (ignore errors).
- If status != "error": invoke the /commit command with the appropriate catalog path.

If harness mode is active, skip both the revert and commit steps.

On failure before writing a result, write result with status: "error" and error details, then revert as above.
Return the item result JSON.
```

### Step 3 — Summarize

1. Read each `.migration-runs/<schema.table>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   profile complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (PARTIAL_PROFILE)
     ✗ silver.DimDate        error (CATALOG_FILE_MISSING)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. Ask the user:

   > All successful items have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr profile <comma-separated list of successfully processed tables>`.
   After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <working-directory>  (omit this line if on the default branch)
   ```

   If on a feature branch, also tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."

   If harness mode is active, skip this entire PR step and end after printing the summary.

6. Suggest running `/status` to see overall migration readiness across all tables.

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
  "item_id": "<table_or_view_fqn>",
  "status": "ok|partial|error",
  "catalog_path": "catalog/tables/<item_id>.json",
  "warnings": [],
  "errors": []
}
```

For views: `catalog_path` is `catalog/views/<item_id>.json`.

The actual profile data lives in the catalog file, not duplicated in the run log.

## Error and Warning Codes

Use the canonical `/profile` code list in [../lib/shared/profile_error_codes.md](../lib/shared/profile_error_codes.md).

Each entry in `errors[]` or `warnings[]` uses this shape:

```json
{"code": "PARTIAL_PROFILE", "message": "Could not determine watermark column for silver.dimcustomer.", "item_id": "silver.dimcustomer", "severity": "warning"}
```
