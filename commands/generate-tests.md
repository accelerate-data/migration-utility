---
name: generate-tests
description: >
  Use when one or more migrated tables are ready for end-to-end test generation
  after scoping and profiling, and the command workflow should run generation,
  independent review, and expectation capture together.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Tests

Generate test scenarios, review for coverage, then bulk-execute approved scenarios to capture ground truth. Launches one sub-agent per table in parallel, each running `/generating-tests`. Review runs as a separate sub-agent via `/reviewing-tests`.

## Arguments

Manual mode:

```text
/generate-tests <object> [object ...]
```

Coordinator mode:

```text
/generate-tests <plan-file> <stage-id> <worktree-name> <base-branch> <object> [object ...]
```

In Claude Code slash commands, `$0` is the first user-supplied argument.
Coordinator mode is active only when `$0` is a Markdown plan path.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_seed": true`, skip that table and print:
  > `<fqn>` is marked as a dbt seed -- no migration needed. Use `ad-migration add-seed-table` to manage seed tables.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `ad-migration add-source-table` to manage source tables.
- `manifest.json` must have `runtime.sandbox`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell user to run `ad-migration setup-sandbox`. The command executes against the active sandbox endpoint, not against the source or target runtime.
- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" test-harness sandbox-status`. If not found, fail all items with `SANDBOX_NOT_RUNNING` and tell user to check the sandbox with `ad-migration setup-sandbox` (it may have been torn down or the database dropped).

Per-item readiness is checked by the skill via `migrate-util ready`.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table or view with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) after its final step completes (Step 5 commit, or the last step at which the item is abandoned).

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `generate-tests-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `generate-tests-customer-tables`, `generate-tests-silver-facts`). The full slug (including the `generate-tests-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Use the `## Arguments` contract above to determine whether this is manual mode or coordinator mode.
3. Use `${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh` for deterministic worktree setup.
   - Coordinator mode: read `Branch:`, `Worktree name:`, and `Base branch:` from the matching stage section, then run:

     ```bash
     "${CLAUDE_PLUGIN_ROOT}/scripts/stage-worktree.sh" "<branch>" "<worktree-name>" "<base-branch>"
     ```

     Use the returned `worktree_path` for all reads, writes, commits, and sub-agent prompts.
   - Manual mode: derive a stable branch name from the run slug, resolve the remote default branch, and call the same helper with those explicit values.
4. In coordinator mode, own only the matching `## Stage <stage-id>` checklist in `<plan-file>`. After each stage substep or item result, update only that checklist, then commit the plan update together with the artifact or catalog change that caused it.
5. Generate a run ID in the form `<epoch_ms>-<random_8hex>` (for example `1743868200123-a1b2c3d4`). All run artifacts use this as the filename suffix.

### Step 2 — Generate scenarios per table

Create `.migration-runs/` first if it does not already exist.

**Workflow-exempt source and seed check:** For each item, read `catalog/tables/<fqn>.json` before any scenario generation. If the catalog marks the table as a source or seed, do not invoke `/generating-tests` for that item. Write one of these skip results to `.migration-runs/<schema.table>.<run_id>.json` and continue to the next item. These skip artifacts are summary-only; they do not enter review, capture, or commit stages.

```json
{"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "is_source", "message": "<fqn> is marked as a dbt source -- no migration needed. Use `ad-migration add-source-table` to manage source tables."}}
```

```json
{"item_id": "<fqn>", "status": "ok", "output": {"skipped": true, "reason": "is_seed", "message": "<fqn> is marked as a dbt seed -- no migration needed. Use `ad-migration add-seed-table` to manage seed tables."}}
```

**Single-table path (1 table):** Run `/generating-tests` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.<run_id>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the /generating-tests skill for <schema.table>.
The working directory is <working-directory>.
Skip the Step 4 approval prompt — the review loop handles quality gating.
Write the item result JSON to .migration-runs/<schema.table>.<run_id>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

The skill writes `test-specs/<item_id>.json` with branch manifest and fixtures but no `expect.rows`.

### Step 3 — Review scenarios

For each item, read `.migration-runs/<item_id>.<run_id>.json` from Step 2. If `status` is `error` or `output.skipped == true`, skip the item. For each remaining item, invoke `/reviewing-tests <item_id> --iteration 1`.

Parse the returned TestReviewResult JSON:

- `approved` or `approved_with_warnings`: proceed to step 4.
- `revision_requested`: pass `feedback_for_generator` to `/generating-tests <item_id>` unchanged (include the feedback JSON — see the skill's "Handling reviewer feedback" section) and prepend this wrapper line to the repair request:
  > Apply reviewer feedback exactly; do not broaden repair beyond named branches/scenarios.
  Then invoke `/reviewing-tests <item_id> --iteration 2`. Maximum 2 review iterations per item.
- On review failure: record `status: "partial"` and continue.

### Step 4 — Capture ground truth

For each item with approved scenarios and `output.skipped != true`:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/packages/ad-migration-internal" test-harness execute-spec \
  --spec test-specs/<item_id>.json
```

**View entries:** When `execute-spec` encounters a test entry without a `procedure` key, it calls `execute_select` instead of `execute_scenario`, running the entry's `sql` SELECT directly against the sandbox. Fixture seeding and rollback work the same way.

If `execute-spec` exits non-zero or individual scenarios fail:

- **Non-zero exit (full failure):** record `status: "error"` with code `SCENARIO_EXECUTION_FAILED` for the item and continue to the next item.
- **Partial scenario failures** (exit 0 but some scenarios report errors in the output): record `status: "partial"` with a `SCENARIO_EXECUTION_FAILED` warning listing which scenarios failed. The item proceeds with the successfully captured expectations.

### Step 5 — Commit test spec

For each item with `status: "ok"` or `status: "partial"` and `output.skipped != true` (i.e., ground truth was captured):

If the item final status is `error`, revert any partially written files:

```bash
git checkout -- test-specs/<item_id>.json
```

Use `rm -f` for files that were newly created and have no prior version.

If the item final status is not `error`, stage `test-specs/<item_id>.json`, create a checkpoint commit, and push the current branch.

### Step 6 — Summarize

1. Read each `.migration-runs/<schema.table>.<run_id>.json`.
2. Write `.migration-runs/summary.<run_id>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   generate-tests complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (COVERAGE_PARTIAL)
     ✗ silver.DimDate        error (PROFILE_NOT_COMPLETED)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, report errors only and stop.
5. After successful item work is committed and pushed, always open or update a PR:

   ```bash
   "${CLAUDE_PLUGIN_ROOT}/scripts/stage-pr.sh" "<branch>" "<base-branch>" "<title>" ".migration-runs/pr-body.<run_id>.md"
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

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "output": {
    "test_spec_path": "test-specs/<item_id>.json",
    "coverage": "complete|partial",
    "branch_count": 8,
    "scenario_count": 7,
    "scenarios_executed": 7,
    "scenarios_failed": 0,
    "review_iterations": 1,
    "review_verdict": "approved|approved_with_warnings"
  },
  "warnings": [],
  "errors": []
}
```

## Error and Warning Codes

Use only the shared canonical codes in `../lib/shared/generate_tests_error_codes.md`.

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "COVERAGE_PARTIAL", "message": "2 of 8 branches uncovered for silver.dimproduct after 2 review iterations.", "item_id": "silver.dimproduct", "severity": "warning"}
```
