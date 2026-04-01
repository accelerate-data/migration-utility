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

Given a batch of target tables, generate test scenarios, review them for coverage, then bulk-execute approved scenarios in the sandbox to capture ground truth. Delegates scenario generation to the `/generating-tests` skill and includes a `/reviewing-tests` review loop.

## Additional Batch-wide Guard

Before processing any items:

- Ask the user for the sandbox run ID (the UUID from `/setup-sandbox`).
- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness sandbox-status --run-id <run_id>`. If missing, fail **all** items with code `SANDBOX_NOT_FOUND` and write output immediately.

## Additional Per-item Guards

Before running the skill for each item:

- Check `scoping.selected_writer` is set. If missing, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.
- Check `profile` exists and `profile.status` is `"ok"`. If missing or not ok, skip this item with `PROFILE_NOT_COMPLETED` in `errors[]`.

## Pipeline

### Step 1 — Generate Scenarios (Skill Delegation)

For each item, invoke `/generating-tests <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

The skill writes `test-specs/<item_id>.json` with branch manifest and fixtures but no `expect.rows`.

### Step 2 — Review Scenarios (Sub-agent)

For each item that completed step 1, launch a **sub-agent** to run `/reviewing-tests`. The reviewer runs in an isolated context to prevent its branch enumeration and quality analysis from polluting the generator's context.

Construct the sub-agent prompt:

```text
You are a test reviewer. Follow the /reviewing-tests skill instructions.
Table: <item_id>
Iteration: 1
Read test-specs/<item_id>.json and the proc context via migrate context.
Return your verdict as a TestReviewResult JSON block.
```

Parse the sub-agent's returned `TestReviewResult` JSON:

- If `status` is `approved` or `approved_with_warnings`: proceed to step 3.
- If `status` is `revision_requested`: pass `feedback_for_generator` to `/generating-tests <item_id>` as additional context, then launch a new review sub-agent with `Iteration: 2`. Maximum 2 review iterations per item.
- On review failure, record `status: "partial"` and continue.

### Step 3 — Capture Ground Truth (Deterministic)

For each item with approved scenarios, bulk-execute all `unit_tests[]` entries against the sandbox:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness execute \
  --run-id <run_id> \
  --scenario <json_file>
```

For each scenario:

1. Write the scenario's `given` fixtures to a temp JSON file.
2. Call `test-harness execute` to insert fixtures, exec the proc, and capture output.
3. Merge the `ground_truth_rows` from the CLI output into `expect.rows` in `test-specs/<item_id>.json`.

If a scenario execution fails, record the error in `warnings[]` and leave `expect.rows` empty for that scenario. Do not abort the batch.

After all scenarios execute, update `test-specs/<item_id>.json` with the merged ground truth.

### Step 4 — Record Result

Write the item result to `.migration-runs/<item_id>.json`:

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
| `SANDBOX_NOT_FOUND` | error | Sandbox database not running — all items fail |
| `CATALOG_FILE_MISSING` | error | catalog/tables/\<item_id>.json not found — skip item |
| `SCOPING_NOT_COMPLETED` | error | scoping section missing or no selected_writer — skip item |
| `PROFILE_NOT_COMPLETED` | error | profile section missing or status != ok — skip item |
| `TEST_GENERATION_FAILED` | error | `/generating-tests` skill pipeline failed — skip item |
| `REVIEW_KICKED_BACK` | warning | reviewer requested revision — item retried |
| `COVERAGE_PARTIAL` | warning | not all branches covered after max iterations — item proceeds as partial |
| `SCENARIO_EXECUTION_FAILED` | warning | one or more scenarios failed during ground truth capture — item proceeds with partial expectations |
