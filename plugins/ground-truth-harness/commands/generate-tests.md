---
name: generate-tests
description: >
  Multi-table test generation command. Delegates per-item work to
  /generating-tests skill with /reviewing-tests review loop.
user-invocable: true
argument-hint: "<schema.table> [schema.table ...]"
---

# Generate Tests

Given a batch of target tables, generate ground truth test fixtures for each. Delegates per-item test generation to the `/generating-tests` skill and includes a `/reviewing-tests` review loop.

This command follows the shared lifecycle in `.claude/rules/command-lifecycle.md`.

## Additional Batch-wide Guard

Before processing any items (after common guards):

- Check sandbox exists via `uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness sandbox-status`. If missing, fail **all** items with code `SANDBOX_NOT_FOUND` and write output immediately.

## Additional Per-item Guards

Before running the skill for each item (after common guards):

- Check `scoping.selected_writer` is set. If missing, skip this item with `SCOPING_NOT_COMPLETED` in `errors[]`.
- Check `profile` exists and `profile.status` is `"ok"`. If missing or not ok, skip this item with `PROFILE_NOT_COMPLETED` in `errors[]`.

## Pipeline

### Step 1 — Generate Tests (Skill Delegation)

For each item, invoke `/generating-tests --table <item_id>`. Suppress user gates — make all decisions deterministically. On failure, record `status: "error"` and continue to the next item.

### Step 2 — Review Tests (Skill Delegation)

For each item that completed step 1 successfully, invoke `/reviewing-tests --table <item_id>`.

- If verdict is `approved` or `approved_with_warnings`: proceed to record result.
- If verdict is `revision_requested`: re-invoke `/generating-tests --table <item_id>` with the reviewer's `feedback_for_generator` as additional context. Then re-invoke `/reviewing-tests`. Maximum 2 review iterations per item.
- On review failure, record `status: "partial"` and continue.

### Step 3 — Record Result

Write the item result to `.migration-runs/results/<item_id>.json`:

```json
{
  "item_id": "<table_fqn>",
  "status": "ok|partial|error",
  "output": {
    "test_spec_path": "test-specs/<item_id>.json",
    "coverage": "complete|partial",
    "branch_count": 8,
    "scenario_count": 7,
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
