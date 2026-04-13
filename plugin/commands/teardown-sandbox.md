---
name: teardown-sandbox
description: Drops a throwaway sandbox database created by setup-sandbox.
user-invocable: true
---

# Tear Down Sandbox

Drop the sandbox database that was previously created by `setup-sandbox`.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop and tell the user to load the plugin with `claude --plugin-dir <path-to-plugins>`.

## Step 2: Read sandbox from manifest

Read `manifest.json` and check for `runtime.sandbox`. If missing, tell the user no sandbox exists and stop.

Show the user which sandbox runtime will be dropped (`runtime.sandbox` from manifest) and ask for confirmation. This is a destructive operation.

## Step 3: Execute

After the user confirms:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-down
```

Output shape:

```json
{"sandbox_database": "__test_abc123def456", "status": "ok | error", "errors": []}
```

## Step 4: Report

Confirm the sandbox has been removed. If the database did not exist, report that it was already cleaned up (not an error).

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `test-harness sandbox-down` | 1 | Drop failed (connection error, permissions). Report error from JSON output |
| `test-harness sandbox-down` | 0 + database not found | Already cleaned up. Report as success, not an error |
