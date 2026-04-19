---
name: scope-tables
description: >
  Batch scoping command — identifies writer procedures for tables, analyzes SQL structure for views/MVs.
  Delegates per-item scoping to the /analyzing-table skill (auto-detects table vs view).
user-invocable: true
argument-hint: "<schema.table_or_view> [schema.table_or_view ...]"
---

# Scope Tables

Identify which procedures write to each table, or analyze SQL structure for each view or materialized view. Launches one sub-agent per item in parallel using `/analyzing-table` (which auto-detects table vs view).

## Guards

- `manifest.json` must exist. If missing, tell the user to run `ad-migration setup-source` first.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `ad-migration add-source-table` to manage source tables.

Per-item readiness is checked by the skill via `migrate-util ready`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table or view with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) when it finishes.

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `scope-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `scope-order-pipeline`, `scope-customer-dims`). The full slug (including the `scope-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Coordinator mode only happens when `$0` is a Markdown plan path. In coordinator mode, parse the invocation as:

   ```text
   /scope-tables <plan-file> <stage-id> <worktree-name> <base-branch> <object> [object ...]
   ```

   Read the matching `## Stage <stage-id>` checklist from `<plan-file>`. Use `$1` as the stage ID, `$2` as the worktree name, `$3` as the base branch, and `$4...` as the object arguments.
3. Use `${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh` for setup instead of `git-checkpoints`.
   - Coordinator mode: read `Branch:`, `Worktree name:`, and `Base branch:` from the matching stage section, then run:

     ```bash
     "${CLAUDE_PLUGIN_ROOT}/shared/scripts/worktree.sh" "<branch>" "<worktree-name>" "<base-branch>"
     ```

     Use the returned `worktree_path` for all reads, writes, commits, and sub-agent prompts.
   - Manual mode: derive a stable branch name from the run slug, resolve the remote default branch, and call the same helper with those explicit values.
4. In coordinator mode, own only the matching `## Stage <stage-id>` checklist in `<plan-file>`. After each stage substep or item result, update only that checklist, then commit the plan update together with the artifact or catalog change that caused it.
5. For each FQN argument, detect its object type by checking which catalog file exists:
   - If `catalog/views/<fqn>.json` exists → `view`
   - Else → `table`

   Store the type alongside each FQN for use in Step 2.
6. Generate a run ID in the form `<epoch_ms>-<random_8hex>` (for example `1743868200123-a1b2c3d4`). All run artifacts use this as the filename suffix.

### Step 2 — Run skill per item

**Single-item path (1 item):** Run `/analyzing-table` directly in the current conversation — do not launch a sub-agent. The skill auto-detects table vs view from catalog presence.

After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.item>.<run_id>.json`.

If the item status is `error`, immediately revert any files the skill may have partially modified:

```bash
git checkout -- catalog/<object_type>s/<item_id>.json
```

Ignore errors from `git checkout` (the file may not have been modified).

If the item status is not `error`, stage `catalog/<object_type>s/<item_id>.json`, create a checkpoint commit, and push the current branch.

Then continue to Step 3.

**Multi-item path (2+ items):** Launch one sub-agent per item in parallel. Each sub-agent receives this prompt:

```text
Run the /analyzing-table skill for <schema.item>.
The working directory is <working-directory>.
Write the item result JSON to .migration-runs/<schema.item>.<run_id>.json.

After writing the result:
- If status == "error": run `git checkout -- catalog/<object_type>s/<item_id>.json` (ignore errors).
- If status != "error": stage `catalog/<object_type>s/<item_id>.json`, create a checkpoint commit, and push the current branch.

On failure before writing a result, write result with status: "error" and error details, then revert as above.
Return the item result JSON.
```

### Step 3 — Summarize

1. Read each `.migration-runs/<schema.item>.<run_id>.json`.
2. Write `.migration-runs/summary.<run_id>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   scope-tables complete — N items processed

     ✓ silver.DimCustomer      resolved   (table)
     ✓ silver.DimProduct       resolved   (table)
     ✓ silver.vw_Sales         analyzed   (view)
     ✗ silver.DimDate          error      (table, CATALOG_FILE_MISSING)

     resolved: 2 | analyzed: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. After successful item work is committed and pushed, always open or update a PR:

   ```bash
   "${CLAUDE_PLUGIN_ROOT}/shared/scripts/stage-pr.sh" "<branch>" "<base-branch>" "<title>" ".migration-runs/pr-body.<run_id>.md"
   ```

   Report the PR number and URL. In manual mode, tell the human to review and merge the PR. In coordinator mode, return the PR metadata to the coordinator and do not ask any question.

   After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <working-directory>  (omit this line if on the default branch)
   ```

   If on a feature branch, also tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."

6. Suggest running `/status` to see overall migration readiness across all tables.

## Output shapes

### Item result (per-item run artifact)

Written to `.migration-runs/<schema.item>.<run_id>.json`:

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

### Batch summary

Written to `.migration-runs/summary.<run_id>.json`:

```json
{
  "schema_version": "1.0",
  "run_id": "<uuid>",
  "results": [
    {"item_id": "silver.dimcurrency", "status": "resolved"},
    {"item_id": "silver.dimdate", "status": "error"}
  ],
  "summary": {"total": 2, "resolved": 1, "ambiguous_multi_writer": 0, "no_writer_found": 0, "analyzed": 0, "error": 1}
}
```

### Table scoping (catalog write-back)

Written to `catalog/tables/<fqn>.json` → `scoping` section:

```json
{
  "status": "resolved",
  "selected_writer": "silver.usp_load_dimcurrency",
  "selected_writer_rationale": "Only direct writer for this table.",
  "candidates": [{"procedure_name": "silver.usp_load_dimcurrency", "rationale": "Direct writer.", "dependencies": {"tables": ["bronze.currency"], "views": [], "functions": []}}],
  "warnings": [],
  "errors": []
}
```

### View scoping (catalog write-back)

Written to `catalog/views/<fqn>.json` → `scoping` section:

```json
{
  "status": "analyzed",
  "sql_elements": [{"type": "join", "detail": "INNER JOIN bronze.person"}],
  "call_tree": {"reads_from": ["bronze.customer", "bronze.person"], "views_referenced": []},
  "logic_summary": "Joins customer and person data.",
  "rationale": "Simple join view.",
  "warnings": [],
  "errors": []
}
```

## Error and Warning Codes

Use the canonical `/scope-tables` code list in [../lib/shared/scope_error_codes.md](../lib/shared/scope_error_codes.md).

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "CATALOG_FILE_MISSING", "message": "catalog/tables/silver.dimdate.json not found.", "item_id": "silver.dimdate", "severity": "error"}
```
