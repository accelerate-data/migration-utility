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

Generate dbt models for a batch of tables. For 2+ tables, runs a planning sweep to identify shared staging models, check existing artifacts, and confirm the execution plan before spawning agents. Claude determines execution parallelism based on the plan.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `/add-source-tables` to manage source tables.
- `dbt_project.yml` must exist at `./dbt/`. If missing, fail all items with `DBT_PROJECT_MISSING`.
- `dbt/profiles.yml` must exist. If missing, fail all items with `DBT_PROFILE_MISSING` and tell the user to run `/init-dbt`.
- `dbt debug` must show "Connection test: OK". If it fails, fail all items with `DBT_CONNECTION_FAILED` and tell the user to check credentials — for SQL Server: `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD` env vars; for other adapters: update `profiles.yml` placeholder values.

Per-item guards are checked by the skill via `migrate-util guard`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track execution phases, not individual tables.

For **2+ tables**, create these tasks at command start:

| Task subject | Complete when |
|---|---|
| `sweep` | Plan artifact written and shared staging files created |
| `pre-flight` | User confirms the plan |
| `execute: <description>` | That execution thread finishes (create when Claude decides each thread) |
| `summarize` | Summary written |

Thread task subjects describe the work: e.g. `execute: generate silver.DimCustomer, silver.FactSales` or `execute: test-only silver.DimDate`. Skipped tables are not tracked as tasks — they appear in the summary only.

For **single-table runs**: create one `execute: generate <fqn>` task and one `summarize` task. No sweep or pre-flight tasks.

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `generate-model-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `generate-model-silver-dims`, `generate-model-order-facts`). The full slug (including the `generate-model-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name (not a worktree path): proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. Generate a run epoch: seconds since Unix epoch (e.g. `1743868200`). All run artifacts use this as a filename suffix.

### Step 1b — Model sweep (2+ tables only)

For each FQN in the batch, collect signals:

1. Read `catalog/tables/<fqn>.json` → `scoping.selected_writer`
2. Read `catalog/procedures/<writer>.json` → `references.tables.in_scope` → collect source tables where `is_selected=true` and `is_updated=false`
3. Check `dbt/models/staging/` for existing `stg_<source_table>.sql` files
4. Check `dbt/models/marts/<fqn_model_name>.sql` for an existing mart model
5. Read `dbt/target/run_results.json` — find results whose `unique_id` contains the mart model name:
   - `"passing"` — all matched results have `status == "pass"`
   - `"failing"` — any matched result has `status == "fail"` or `"error"`
   - `"none"` — no matched results found
6. Derive `recommended_action`:
   - `"skip"` — mart model exists AND `test_status == "passing"`
   - `"test-only"` — mart model exists AND `test_status != "passing"`
   - `"generate"` — no mart model

Across all FQNs, find source tables referenced by 2+ SPs → `shared_staging_candidates`.

For each shared staging candidate not already on disk, write `dbt/models/staging/stg_<table>.sql`:

```sql
{{ config(materialized='ephemeral') }}

select * from {{ source('<schema>', '<table>') }}
```

Write plan artifact to `.migration-runs/model-sweep.<epoch>.json` (schema: `plugin/lib/shared/schemas/model_sweep_output.json`).

Show pre-flight table:

```text
Table                  Staging  Mart     Tests          Action
─────────────────────────────────────────────────────────────
silver.DimCustomer     none     none     —              generate
silver.DimProduct      exists   exists   passing        skip
silver.DimDate         exists   exists   missing        test-only
silver.FactSales       exists   none     —              generate (stg reused)

Shared staging written: stg_dimdate.sql (3 SPs)

Proceed? (y/n/edit)
```

On `edit`: user specifies per-table action overrides (e.g. `silver.DimProduct: regenerate`). Update the plan artifact to reflect the override before proceeding. `regenerate` forces `recommended_action: "generate"` even when a mart model exists.

### Step 2 — Execute generation (plan-driven)

**Single-table path (1 table):** Run `migration:generating-model` directly in the current conversation — do not launch a sub-agent. Pass the model sweep artifact path if it exists. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.<epoch>.json`. Then continue to Step 3 (review).

**Multi-table path (2+ tables):** Read the model sweep plan. For each table:

- `recommended_action: "skip"` — write a skip result immediately (status: `ok`, no agent needed). Use `{"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "mart model exists with passing tests"}}`.
- `recommended_action: "test-only"` — spawn an agent with test-only instructions (see prompt below)
- `recommended_action: "generate"` — spawn an agent for full generation (see prompt below)

Claude decides how many agents to spawn and which tables to group, based on shared staging relationships and any explicit user ordering. Tables that share a staging model can run fully in parallel — their shared staging files are already on disk.

**Full generation agent prompt:**

```text
Run the migration:generating-model skill for <schema.table>.
The working directory is <working-directory>.
Model sweep artifact: .migration-runs/model-sweep.<epoch>.json
Skip the Step 4 user confirmation prompt and the Step 6 approval prompt — proceed automatically. Still run the full equivalence analysis in Step 4.
Equivalence warnings: proceed and write the model. Record each gap as EQUIVALENCE_GAP warning.
dbt compile/test failure: attempt up to 3 self-corrections. If still failing, write as-is with DBT_TEST_FAILED warning.
Write the item result JSON to .migration-runs/<schema.table>.<epoch>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

**Test-only agent prompt:**

```text
Run the migration:generating-model skill for <schema.table> in test-only mode.
The working directory is <working-directory>.
Model sweep artifact: .migration-runs/model-sweep.<epoch>.json
The mart model already exists on disk. Skip Steps 2–7. Proceed directly to Step 8 (compile + test).
Write the item result JSON to .migration-runs/<schema.table>.<epoch>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

### Step 3 — Review Model

For each item, read `.migration-runs/<item_id>.<epoch>.json` from Step 2. If `status` is `error`, skip the item. For each remaining item, invoke `/reviewing-model <item_id>`.

- If verdict is `approved`: proceed to commit/revert below.
- `revision_requested`: invoke `/generating-model <item_id>` with the reviewer's `feedback_for_model_generator` as additional context. The model-generator must re-run `dbt test` to confirm unit tests still pass after revisions. Then invoke `/reviewing-model <item_id>` again. Maximum 2 review iterations per item.
- On review failure or max iterations reached: approve with warnings and proceed to commit/revert below.

Once the review outcome is final for an item, derive `<model_name>` from item_id using the `stg_<table>` convention.

If the item final status is `error`, revert any files the skill may have partially written:

```bash
git checkout -- dbt/models/staging/<model_name>.sql dbt/models/staging/_<model_name>.yml
```

Use `rm -f` instead of `git checkout` for newly created files with no prior version.

If the item final status is not `error`, auto-commit and push: run `/commit dbt/models/staging/<model_name>.sql dbt/models/staging/_<model_name>.yml`.

For multi-table sub-agents: include the commit/revert instructions in the sub-agent prompt at the end of the review loop, using "invoke the /commit command with <files>".

### Step 4 — Summarize

1. Read each `.migration-runs/<schema.table>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
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

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "output": {
    "table_ref": "<table_fqn>",
    "model_name": "<model_name>",
    "artifact_paths": {
      "model_sql": "models/staging/<model_name>.sql",
      "model_yaml": "models/staging/_<model_name>.yml"
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

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `DBT_PROJECT_MISSING` | error | dbt_project.yml not found — all items fail |
| `DBT_PROFILE_MISSING` | error | dbt/profiles.yml not found — run `/init-dbt` — all items fail |
| `DBT_CONNECTION_FAILED` | error | `dbt debug` connection test failed — check credentials — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILE_NOT_COMPLETED` | error | profile section missing or status != ok — skip item |
| `TEST_SPEC_NOT_FOUND` | error | test-specs/\<item_id>.json not found — skip item |
| `GENERATION_FAILED` | error | `/generating-model` skill pipeline failed — skip item |
| `EQUIVALENCE_GAP` | warning | semantic gap found between proc and generated model — item proceeds as partial |
| `DBT_COMPILE_FAILED` | warning | `dbt compile` failed after retries — item proceeds as partial |
| `DBT_TEST_FAILED` | warning | `dbt test` failed after 3 self-correction iterations — item proceeds as partial |
| `REVIEW_KICKED_BACK` | warning | reviewer requested revision — item retried |
| `REVIEW_APPROVED_WITH_WARNINGS` | warning | reviewer approved with remaining issues after max iterations — item proceeds |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "EQUIVALENCE_GAP", "message": "Missing column 'legacy_flag' in generated model for silver.dimcustomer.", "item_id": "silver.dimcustomer", "severity": "warning"}
```
