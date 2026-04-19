---
name: generate-model
description: >
  Batch model generation command — generates dbt models from stored procedures.
  Coordinates generation, review, unit-test setup, and unit-test repair stages
  via focused sub-agents.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Model

Generate dbt models for a batch of tables. Coordinates four stages — generation, review, unit-test setup, and unit-test repair — each delegated to focused sub-agents whose prompts live in `references/`.

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
- `runtime.sandbox` must be present in `manifest.json`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell the user to run `ad-migration setup-sandbox`.
- The sandbox must be reachable: run `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" test-harness sandbox-status`. If the sandbox does not exist or is not accessible, fail all items with `SANDBOX_NOT_CONFIGURED`.

Per-item readiness is checked by the skill via `migrate-util ready`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table or view with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) after its final step completes (Step 5 commit/revert, or the last step at which the item is abandoned).

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `generate-model-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `generate-model-silver-dims`, `generate-model-order-facts`). The full slug (including the `generate-model-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Coordinator mode only happens when `$0` is a Markdown plan path. In coordinator mode, parse the invocation as:

   ```text
   /generate-model <plan-file> <stage-id> <worktree-name> <base-branch> <object> [object ...]
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
5. Generate a run ID in the form `<epoch_ms>-<random_8hex>` (for example `1743868200123-a1b2c3d4`). All run artifacts use this as the filename suffix.

### Step 2 — Stage 1: Generate

Create `.migration-runs/` first if it does not already exist.

**Workflow-exempt source and seed check:** For each item, read `catalog/tables/<fqn>.json` before any idempotency check or model generation. If the catalog marks the table as a source or seed, do not invoke `/generating-model` for that item. Write one of these skip results to `.migration-runs/<schema.table>.<run_id>.json` and continue to the next item:

```json
{"item_id": "<fqn>", "status": "skipped", "output": {"skipped": true, "reason": "is_source", "message": "<fqn> is marked as a dbt source -- no migration needed. Use `ad-migration add-source-table` to manage source tables."}}
```

```json
{"item_id": "<fqn>", "status": "skipped", "output": {"skipped": true, "reason": "is_seed", "message": "<fqn> is marked as a dbt seed -- no migration needed. Use `ad-migration add-seed-table` to manage seed tables."}}
```

**Idempotency check:** For each non-source, non-seed item, read `catalog/tables/<fqn>.json`. If `generate.status == "ok"` and the user did not explicitly request a rerun, skip fresh generation but still carry the item into Stage 2 review using the existing written artifacts. Write a skip result:

```json
{"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "model_already_generated"}}
```

This skip means "reuse existing artifacts for review," not "bypass the quality gate."

**Prompt:** Read [references/generation-agent-prompt.md](references/generation-agent-prompt.md). Substitute `<schema.table>`, `<working-directory>`, and `<run_id>` before dispatching.

Launch one sub-agent per item in parallel for items that still need fresh generation. Items that passed the idempotency check above write the skip result immediately and carry forward to Stage 2. Each sub-agent follows the generation-agent-prompt and writes its item result JSON.

### Step 3 — Stage 2: Review

For each item, read `.migration-runs/<item_id>.<run_id>.json` from Stage 1. If `status` is `error` or `skipped`, skip review for that item and carry it forward to Stage 3. For each remaining item, run the review flow for that item, including items whose Stage 1 result was an idempotency skip.

If a Stage 1 idempotency-skip item's review returns `error` because persisted artifacts are missing or stale, invoke `/generating-model <item_id>` once to rebuild them, then retry review.

**Prompt:** Read [references/review-agent-prompt.md](references/review-agent-prompt.md). Substitute `<schema.table>`, `<working-directory>`, and `<run_id>` before dispatching.

Launch one review sub-agent per eligible item in parallel. Each sub-agent follows the review-agent-prompt and updates the item result JSON.

### Step 4 — Stage 3: Unit-test setup

Read each item result from `.migration-runs/<item_id>.<run_id>.json`. Collect the subset of items where `output.generated.model_yaml.has_unit_tests` is `true` and `status` is not `error`. If none, skip this stage and proceed to Step 5.

**Prompt:** Read [references/unit-test-setup-agent-prompt.md](references/unit-test-setup-agent-prompt.md). Substitute `<model_names>` (space-separated `model_name` values for the collected items), `<working-directory>`, and `<run_id>` before dispatching.

Dispatch one setup sub-agent for the entire collected list.

### Step 5 — Stage 4: Unit-test repair and commit

Before dispatching repair agents, read `.migration-runs/unit-test-setup.<run_id>.json`. If `status` is `error`, skip repair for all unit-test items: update each item result with `status: "partial"` and a `DBT_TEST_FAILED` warning (reason: parent materialisation failed), then proceed to commit/revert.

For each item where `output.generated.model_yaml.has_unit_tests` is `true` and `status` is not `error`:

**Prompt:** Read [references/unit-test-repair-agent-prompt.md](references/unit-test-repair-agent-prompt.md). Substitute `<schema.table>`, `<model_name>`, `<working-directory>`, and `<run_id>` before dispatching.

Launch one repair sub-agent per eligible item in parallel. Each sub-agent follows the unit-test-repair-agent-prompt and updates `execution.dbt_test_passed` in the item result JSON.

**Commit/revert (all items):** Once all repair agents complete, commit all items together to avoid shared YAML races. Apply per item:

Derive `<model_name>` from item_id.

If the item final status is `error`, revert any files the skill may have partially written:

```bash
git checkout -- dbt/models/marts/<model_name>.sql
```

Do not run `git checkout` on shared aggregate YAML files such as `dbt/models/marts/_marts__models.yml` or `dbt/snapshots/_snapshots__models.yml`. Those files can contain sibling model entries from other successful items in the same run. If a failed item added an entry to shared YAML, remove only that model or snapshot entry by `name` and preserve every other entry. If entry-level cleanup cannot be performed safely, leave the shared YAML file unchanged and report the stale failed-item entry in the summary.

For snapshot artifacts, revert only the per-item snapshot SQL path returned by the item result:

```bash
git checkout -- dbt/snapshots/<snapshot_name>.sql
```

Use `rm -f` instead of `git checkout` for newly created files with no prior version.

If the item final status is not `error`, stage the generated dbt files, create a checkpoint commit, and push the current branch.

### Step 6 — Summarize

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

## Item Result Schema

For snapshots, `artifact_paths.model_sql` uses `snapshots/<snapshot_name>.sql` and `artifact_paths.model_yaml` uses `snapshots/_snapshots__models.yml`.

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
