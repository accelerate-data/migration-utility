---
name: generate-tests
description: >
  Multi-table test generation command. Delegates scenario generation to
  /generating-tests skill with /reviewing-tests review loop, then
  bulk-executes approved scenarios to capture ground truth.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Tests

Generate test scenarios, review for coverage, then bulk-execute approved scenarios to capture ground truth. Launches one sub-agent per table in parallel, each running `ground-truth-harness:generating-tests`. Review runs as a separate sub-agent via `ground-truth-harness:reviewing-tests`.

## Guards

- `manifest.json` must exist. If missing, fail all items with `MANIFEST_NOT_FOUND`.
- For each FQN argument: if `catalog/tables/<fqn>.json` has `"is_source": true`, skip that table and print:
  > `<fqn>` is marked as a dbt source — no migration needed. Use `/add-source-tables` to manage source tables.
- `manifest.json` must have `sandbox.database`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell user to run `/setup-sandbox`.
- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status`. If not found, fail all items with `SANDBOX_NOT_RUNNING` and tell user to check the sandbox with `/setup-sandbox` (it may have been torn down or the database dropped).

Per-item readiness is checked by the skill via `migrate-util ready`.

## Contracts

Test spec and review output shapes are enforced by Pydantic models in `../lib/shared/output_models.py`:

- `TestSpec` — per-item spec written to `test-specs/<item_id>.json` (see `generating-tests/SKILL.md` for shape)
- `TestReviewOutput` — review result returned by the reviewing-tests skill (see `reviewing-tests/SKILL.md` for shape)
- `TestSpecOutput` — batch wrapper: `{"schema_version": "1.0", "results": [TestSpec, ...], "summary": {"total": N, "ok": N, "partial": N, "error": N}}`

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to show live progress. At the start of Step 2, create one task per table with status `pending`. Update each task to `in_progress` before it starts processing, and to `completed` (ok/partial result) or `cancelled` (error — include the error code) after its final step completes (Step 5 commit, or the last step at which the item is abandoned).

## Pipeline

### Step 1 — Setup

1. Generate run slug:
   - **Single object (1 item):** use the object FQN directly — `generate-tests-<schema>-<name>` (lowercase, dots → hyphens). No LLM reasoning needed.
   - **Multiple objects (2+):** reason about the conversation context — what is the user trying to accomplish with this batch? Generate a short, descriptive slug that captures the intent (e.g. `generate-tests-customer-tables`, `generate-tests-silver-facts`). The full slug (including the `generate-tests-` prefix) must be lowercase, hyphen-separated, and at most 40 characters.
2. Run the `git-checkpoints` skill with the run slug as the argument.
   - If it returns the default branch name (not a worktree path): proceed without a branch or worktree. All file writes and git operations target the current directory. Set `<working-directory>` to `$(git rev-parse --show-toplevel)` for use in sub-agent prompts below.
   - Otherwise: use the returned path as the working directory for all file writes and git operations in this run. Set `<working-directory>` to the returned path.
3. Generate a run ID in the form `<epoch_ms>-<random_8hex>` (for example
   `1743868200123-a1b2c3d4`). All run artifacts use this as the filename suffix.

### Step 2 — Generate scenarios per table

**Single-table path (1 table):** Run `ground-truth-harness:generating-tests` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.<run_id>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the ground-truth-harness:generating-tests skill for <schema.table>.
The working directory is <working-directory>.
Skip the Step 4 approval prompt — the review loop handles quality gating.
Write the item result JSON to .migration-runs/<schema.table>.<run_id>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

The skill writes `test-specs/<item_id>.json` with branch manifest and fixtures but no `expect.rows`.

### Step 3 — Review scenarios

For each item, read `.migration-runs/<item_id>.<run_id>.json` from Step 2. If `status` is `error`, skip the item. For each remaining item, invoke `/reviewing-tests <item_id> --iteration 1`.

Parse the returned TestReviewResult JSON:

- `approved` or `approved_with_warnings`: proceed to step 4.
- `revision_requested`: pass `feedback_for_generator` to `/generating-tests <item_id>` (include the feedback JSON — see the skill's "Handling reviewer feedback" section), then invoke `/reviewing-tests <item_id> --iteration 2`. Maximum 2 review iterations per item.
- On review failure: record `status: "partial"` and continue.

### Step 4 — Capture ground truth

For each item with approved scenarios:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness execute-spec \
  --spec test-specs/<item_id>.json
```

The CLI reads `sandbox.database` from `manifest.json`, executes all scenarios, captures ground truth, and writes `expect.rows` back into the file.

**View entries:** When `execute-spec` encounters a test entry without a `procedure` key, it calls `execute_select` instead of `execute_scenario`, running the entry's `sql` SELECT directly against the sandbox. Fixture seeding and rollback work the same way.

If `execute-spec` exits non-zero or individual scenarios fail:

- **Non-zero exit (full failure):** record `status: "error"` with code `SCENARIO_EXECUTION_FAILED` for the item and continue to the next item.
- **Partial scenario failures** (exit 0 but some scenarios report errors in the output): record `status: "partial"` with a `SCENARIO_EXECUTION_FAILED` warning listing which scenarios failed. The item proceeds with the successfully captured expectations.

### Step 5 — Commit test spec

For each item with `status: "ok"` or `status: "partial"` (i.e., ground truth was captured):

If the item final status is `error`, revert any partially written files:

```bash
git checkout -- test-specs/<item_id>.json
```

Use `rm -f` for files that were newly created and have no prior version.

If the item final status is not `error`, auto-commit and push: run `/commit test-specs/<item_id>.json`.

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
5. Ask the user:

   > All successful items have been committed and pushed.
   > Raise a PR for this run? (y/n)

   If yes: run `/commit-push-pr generate-tests <comma-separated list of successfully processed tables>`.
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
