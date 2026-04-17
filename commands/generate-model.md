---
name: generate-model
description: >
  Batch model generation command — generates dbt models from stored procedures.
  Delegates per-item generation to the /generating-model skill with
  /reviewing-model review loop.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Model

Generate dbt models for a batch of tables. Launches one sub-agent per table in parallel, each running `/generating-model`. Review runs via `/reviewing-model` with a maximum of 2 iterations per item.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_seed": true`, skip that table, write the workflow-exempt skip result described in Step 2, and print:
  > `<fqn>` is marked as a dbt seed -- no migration needed. Use `ad-migration add-seed-table` to manage seed tables.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table, write the workflow-exempt skip result described in Step 2, and print:
  > `<fqn>` is marked as a dbt source -- no migration needed. Use `ad-migration add-source-table` to manage source tables.
- `dbt_project.yml` must exist at `./dbt/`. If missing, fail all items with `DBT_PROJECT_MISSING`.
- `dbt/profiles.yml` must exist. If missing, fail all items with `DBT_PROFILE_MISSING` and tell the user to run `ad-migration setup-target`.
- `dbt debug` must show "Connection test: OK". If it fails, fail all items with `DBT_CONNECTION_FAILED` and tell the user to check the resolved `runtime.target` credentials and endpoint in `manifest.json` and the matching `dbt/profiles.yml` configuration.
- `runtime.target` must be present in `manifest.json`. If missing, fail all items with `TARGET_NOT_CONFIGURED` and tell the user to run `ad-migration setup-target`.
- `runtime.sandbox` must be present in `manifest.json`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell the user to run `ad-migration setup-sandbox`. The sandbox is the active execution endpoint when the workflow needs live source-backed validation; it is separate from `runtime.target`.
- The sandbox must be reachable: run `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" test-harness sandbox-status`. If the sandbox does not exist or is not accessible, fail all items with `SANDBOX_NOT_CONFIGURED`.

Per-item readiness is checked by the skill via `migrate-util ready` (which enforces that refactor, test generation, and sandbox configuration are complete before model generation can proceed).

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table or view with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) after its final step completes (Step 3 commit, or the last step at which the item is abandoned).

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `generate-model-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `generate-model-silver-dims`, `generate-model-order-facts`). The full slug (including the `generate-model-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name (not a worktree path): proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. Generate a run ID in the form `<epoch_ms>-<random_8hex>` (for example `1743868200123-a1b2c3d4`). All run artifacts use this as the filename suffix.

### Step 2 — Execute generation

Create `.migration-runs/` first if it does not already exist.

**Workflow-exempt source and seed check:** For each item, read
`catalog/tables/<fqn>.json` before any idempotency check or model generation.
If the catalog marks the table as a source or seed, do not invoke
`/generating-model` or `/reviewing-model` for that item. Write one of these
skip results to `.migration-runs/<schema.table>.<run_id>.json` and continue to
the next item:

```json
{"item_id": "<fqn>", "status": "skipped", "output": {"skipped": true, "reason": "is_source", "message": "<fqn> is marked as a dbt source -- no migration needed. Use `ad-migration add-source-table` to manage source tables."}}
```

```json
{"item_id": "<fqn>", "status": "skipped", "output": {"skipped": true, "reason": "is_seed", "message": "<fqn> is marked as a dbt seed -- no migration needed. Use `ad-migration add-seed-table` to manage seed tables."}}
```

**Idempotency check:** For each non-source, non-seed item, read
`catalog/tables/<fqn>.json`. If `generate.status == "ok"` and the user did not
explicitly request a rerun, skip fresh generation but still carry the item into
Step 3 review using the existing written artifacts. Write a skip result:

```json
{"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "model_already_generated"}}
```

This skip means "reuse existing artifacts for review," not "bypass the quality gate."

**Single-table path (1 table):** Run `/generating-model` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.<run_id>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel for items that still need fresh generation. Items that passed the idempotency check above write the skip result immediately, then continue to Step 3 review. Each sub-agent receives this prompt:

```text
Run /generating-model for <schema.table>.
The working directory is <working-directory>.
Equivalence warnings: proceed and write the model. Record each gap as EQUIVALENCE_GAP warning.
dbt compile/build failure: attempt up to 3 self-corrections. If still failing, write as-is with DBT_TEST_FAILED warning.
Write the item result JSON to .migration-runs/<schema.table>.<run_id>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 3 — Review model

For each item, read `.migration-runs/<item_id>.<run_id>.json` from Step 2. If
`status` is `error` or `skipped`, skip review for that item. For each remaining
item, invoke `/reviewing-model <item_id>`, including items whose Step 2 result
was an idempotency skip.

- If verdict is `approved`: proceed to commit/revert below.
- If Step 2 was a skip and review returns `error` because the persisted artifacts are missing or stale, invoke `/generating-model <item_id>` once to rebuild the artifacts, then invoke `/reviewing-model <item_id>` again.
- `revision_requested`: invoke `/generating-model <item_id>` with the reviewer's `feedback_for_model_generator` as additional context (pass it via `ModelGenerationHandoff.revision_feedback`). The model-generator must re-run dbt validation with `dbt build` after revisions. Then invoke `/reviewing-model <item_id>` again. Maximum 2 review iterations per item.
- On review failure or max iterations reached: approve with warnings and proceed to commit/revert below.

Once the review outcome is final for an item, derive `<model_name>` from item_id.

If the item final status is `error`, revert any files the skill may have partially written:

```bash
git checkout -- dbt/models/marts/<model_name>.sql
```

Do not run `git checkout` on shared aggregate YAML files such as
`dbt/models/marts/_marts__models.yml` or
`dbt/snapshots/_snapshots__models.yml`. Those files can contain sibling model
entries from other successful items in the same run. If a failed item added an
entry to shared YAML, remove only that model or snapshot entry by `name` and
preserve every other entry. If entry-level cleanup cannot be performed safely,
leave the shared YAML file unchanged and report the stale failed-item entry in
the summary.

For snapshot artifacts, revert only the per-item snapshot SQL path returned by
the item result:

```bash
git checkout -- dbt/snapshots/<snapshot_name>.sql
```

Use `rm -f` instead of `git checkout` for newly created files with no prior version.

If the item final status is not `error`, stage the generated dbt files, create a checkpoint commit, and push the current branch.

In multi-table runs, the parent command owns review and commit/revert after each generation result is written. Generation sub-agents only run `/generating-model` and write their item result JSON.

### Step 4 — Summarize

1. Read each `.migration-runs/<schema.table>.<run_id>.json`.
2. Write `.migration-runs/summary.<run_id>.json` with `{total, ok, partial, error, skipped}` counts and per-item status.
3. Present human-readable summary:

   ```text
   generate-model complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (EQUIVALENCE_GAP)
     ✗ silver.DimDate        error (PROFILE_NOT_COMPLETED)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. Ask the user:

   > All successful items have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr generate-model <comma-separated list of successfully processed tables>`.
   After the PR is created or updated, tell the user:

   ```text
   PR #<number> is open: <pr_url>
   Branch: <branch>
   Worktree: <working-directory>  (omit this line if on the default branch)
   ```

   If on a feature branch, also tell the user: "Once the PR is merged, run /cleanup-worktrees to remove the worktree and branches."

6. Suggest running `/status` to see overall migration readiness across all tables.

## Item Result Schema

For snapshots, `artifact_paths.model_sql` uses
`snapshots/<snapshot_name>.sql` and `artifact_paths.model_yaml` uses
`snapshots/_snapshots__models.yml`.

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "output": {
    "table_ref": "<table_fqn>",
    "model_name": "<model_name>",
    "artifact_paths": {
      "model_sql": "models/marts/<model_name>.sql",
      "model_yaml": "models/marts/_marts__models.yml"
    },
    "generated": {
      "model_sql": {
        "materialized": "<materialization>",
        "uses_watermark": true
      },
      "model_yaml": {
        "has_model_description": true,
        "schema_tests_rendered": ["..."],
        "has_unit_tests": true
      }
    },
    "execution": {
      "dbt_compile_passed": true,
      "dbt_test_passed": true,
      "self_correction_iterations": 0,
      "dbt_errors": []
    },
    "review": {
      "iterations": 1,
      "verdict": "approved|approved_with_warnings"
    },
    "warnings": [],
    "errors": []
  }
}
```

## Error and Warning Codes

Use only the shared canonical codes in `../lib/shared/generate_model_error_codes.md`.

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "EQUIVALENCE_GAP", "message": "Missing column 'legacy_flag' in generated model for silver.dimcustomer.", "item_id": "silver.dimcustomer", "severity": "warning"}
```
