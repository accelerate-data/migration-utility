---
name: teardown-sandbox
description: Drops a throwaway sandbox database created by setup-sandbox.
argument-hint: "<run-id>"
---

# Tear Down Sandbox

Drop the sandbox database (`__test_<run_id>`) that was previously created by `setup-sandbox`.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop and tell the user to load the plugin with `claude --plugin-dir <path-to-plugins>`.

## Step 2: Get run ID

Ask the user for the run ID of the sandbox to tear down. This is the same UUID that was used with `setup-sandbox`.

## Step 3: Confirm

Tell the user which database will be dropped (`__test_<run_id>` with dashes replaced by underscores) and ask for confirmation. This is a destructive operation.

## Step 4: Execute

After the user confirms:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness sandbox-down \
  --run-id <uuid>
```

Parse the JSON output and report whether the database was successfully dropped.

## Step 5: Report

Confirm the sandbox has been removed. If the database did not exist, report that it was already cleaned up (not an error).

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `test-harness sandbox-down` | 1 | Drop failed (connection error, permissions). Report error from JSON output |
| `test-harness sandbox-down` | 0 + database not found | Already cleaned up. Report as success, not an error |

Errors in JSON output use this format:

```json
{"code": "SANDBOX_DOWN_FAILED", "message": "timeout"}
```
