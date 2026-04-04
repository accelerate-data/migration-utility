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
- `manifest.json` must have `sandbox.database`. If missing, fail all items with `SANDBOX_NOT_CONFIGURED` and tell user to run `/setup-sandbox`.
- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-status`. If not found, fail all items with `SANDBOX_NOT_RUNNING` and tell user to check the sandbox with `/setup-sandbox` (it may have been torn down or the database dropped).
- Per item: `catalog/tables/<item_id>.json` must exist. If missing, skip with `CATALOG_FILE_MISSING`.
- Per item: `scoping.selected_writer` must be set. If missing, skip with `SCOPING_NOT_COMPLETED`.
- Per item: `profile` must exist with `status: "ok"`. If missing, skip with `PROFILE_NOT_COMPLETED`.

## Pipeline

### Step 1 — Setup

1. Generate run slug: `generate-tests-<table1>-<table2>-...` (lowercase, dots replaced with hyphens, truncated to 60 characters).
2. Check for existing worktrees. If any exist, list them as options alongside creating a new one and ask the user to pick:
   > 1. `feature/scope-silver-dimcustomer`
   > 2. `feature/profile-silver-dimcustomer`
   > 3. **New worktree**
   If none exist, create a new worktree and branch per `.claude/rules/git-workflow.md`.
3. Generate a run epoch: seconds since Unix epoch (e.g. `1743868200`). All run artifacts use this as a filename suffix.

### Step 2 — Generate scenarios per table

**Single-table path (1 table):** Run `ground-truth-harness:generating-tests` directly in the current conversation — do not launch a sub-agent. After the skill completes, write the item result JSON (see Item Result Schema) to `.migration-runs/<schema.table>.<epoch>.json`. Then continue to Step 3.

**Multi-table path (2+ tables):** Launch one sub-agent per table in parallel. Each sub-agent receives this prompt:

```text
Run the ground-truth-harness:generating-tests skill for <schema.table>.
The worktree is at <worktree-path>.
Skip the Step 4 approval prompt — the review loop handles quality gating.
Write the item result JSON to .migration-runs/<schema.table>.<epoch>.json.
On failure, write result with status: "error" and error details.
Return the item result JSON.
```

The skill writes `test-specs/<item_id>.json` with branch manifest and fixtures but no `expect.rows`.

### Step 3 — Review scenarios (sub-agent)

For each item that completed step 2, launch a review sub-agent in isolated context:

```text
Run the ground-truth-harness:reviewing-tests skill for <item_id> --iteration 1.
The worktree is at <worktree-path>.
The test spec is at <worktree-path>/test-specs/<item_id>.json.
Write the TestReviewResult JSON to .migration-runs/<item_id>.review.<epoch>.json.
On failure, write result with status: "error" and error details.
Return the TestReviewResult JSON.
```

Parse the returned TestReviewResult JSON:

- `approved` or `approved_with_warnings`: proceed to step 4.
- `revision_requested`: pass `feedback_for_generator` to a new `ground-truth-harness:generating-tests` sub-agent (include the feedback JSON in the prompt — see the skill's "Handling reviewer feedback" section), then launch review sub-agent with `--iteration 2`. Maximum 2 review iterations per item.
- On review failure: record `status: "partial"` and continue.

### Step 4 — Capture ground truth

For each item with approved scenarios:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness execute-spec \
  --spec test-specs/<item_id>.json
```

The CLI reads `sandbox.database` from `manifest.json`, executes all scenarios, captures ground truth, and writes `expect.rows` back into the file.

If `execute-spec` exits non-zero or individual scenarios fail:

- **Non-zero exit (full failure):** record `status: "error"` with code `SCENARIO_EXECUTION_FAILED` for the item and continue to the next item.
- **Partial scenario failures** (exit 0 but some scenarios report errors in the output): record `status: "partial"` with a `SCENARIO_EXECUTION_FAILED` warning listing which scenarios failed. The item proceeds with the successfully captured expectations.

### Step 5 — Revert errored items

For each item with `status: "error"`, revert any files the skill may have partially written:

```bash
git checkout -- test-specs/<item_id>.json
```

Ignore errors from `git checkout` (the file may not exist yet — use `rm -f` instead if the test-spec was newly created and has no prior version).

### Step 5.5 — Convert to dbt YAML

For each item with `status: "ok"` or `status: "partial"` (i.e., ground truth was captured):

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness convert-dbt \
  --spec test-specs/<item_id>.json \
  --output test-specs/<item_id>.yml
```

This converts the CLI-ready JSON (with `expect.rows`) to dbt unit test YAML format:

- `given[].table` bracket-quoted identifiers become `source()`/`ref()` expressions
- `target_table` is mapped to a dbt `model` name (e.g. `[silver].[DimProduct]` → `stg_dimproduct`)
- The committed artifact is the `.yml` file; the intermediate `.json` is not staged

### Step 6 — Summarize

1. Read each `.migration-runs/<schema.table>.<epoch>.json`.
2. Write `.migration-runs/summary.<epoch>.json` with `{total, ok, partial, error}` counts and per-item status.
3. Present human-readable summary:

   ```text
   generate-tests complete — N tables processed

     ✓ silver.DimCustomer    ok
     ~ silver.DimProduct     partial (COVERAGE_PARTIAL)
     ✗ silver.DimDate        error (PROFILE_NOT_COMPLETED)

     ok: 1 | partial: 1 | error: 1
   ```

4. If all items errored, skip commit/PR — report errors only and stop.
5. Ask the user: commit and push? Stage only dbt YAML files from successful items (`test-specs/<item_id>.yml`). Do not stage `.migration-runs/` or intermediate JSON files. Check for an existing open PR on the branch via `gh pr list --head <slug> --state open --json number,url`. If one exists, update it with `gh pr edit` instead of creating a new PR. PR body format:

   ```markdown
   ## Test Generation — N tables

   | Table | Status | Branches | Scenarios | Coverage | Review |
   |---|---|---|---|---|---|
   | silver.DimCustomer | ok | 8 | 8 | complete | approved |
   | silver.DimProduct | partial | 6 | 4 | partial | approved_with_warnings |
   | silver.DimDate | error | — | — | — | PROFILE_NOT_COMPLETED |
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

| Code | Severity | When |
|---|---|---|
| `MANIFEST_NOT_FOUND` | error | manifest.json missing — all items fail |
| `SANDBOX_NOT_CONFIGURED` | error | manifest.json has no `sandbox.database` — run `/setup-sandbox` first |
| `SANDBOX_NOT_RUNNING` | error | sandbox-status check failed — sandbox may have been torn down or DB dropped |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILE_NOT_COMPLETED` | error | profile section missing or status != ok — skip item |
| `TEST_GENERATION_FAILED` | error | `/generating-tests` skill pipeline failed — skip item |
| `REVIEW_KICKED_BACK` | warning | reviewer requested revision — item retried |
| `COVERAGE_PARTIAL` | warning | not all branches covered after max iterations — item proceeds as partial |
| `SCENARIO_EXECUTION_FAILED` | warning | one or more scenarios failed during ground truth capture — item proceeds with partial expectations |

Each entry in `errors[]` or `warnings[]`:

```json
{"code": "COVERAGE_PARTIAL", "message": "2 of 8 branches uncovered for silver.dimproduct after 2 review iterations.", "item_id": "silver.dimproduct", "severity": "warning"}
```
