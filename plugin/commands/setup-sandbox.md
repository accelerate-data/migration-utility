---
name: setup-sandbox
description: Creates a throwaway sandbox execution environment from the configured source runtime. Checks prerequisites and calls the test-harness CLI. Persists sandbox info to manifest.json.
user-invocable: true
---

# Set Up Sandbox

Create a throwaway sandbox execution environment from the configured source runtime. The sandbox is used by the test generator to execute stored procedures and capture ground truth output. The active sandbox endpoint is persisted in `manifest.json` as `runtime.sandbox`. The environment name is auto-generated (`__test_<random_hex>`) unless the backend requires a different identifier shape.

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track the automated phases of this command. After the user confirms (Step 3) and before execution begins, create a task for `Create sandbox environment`. Update it to `in_progress` when the CLI starts and to `completed` or `cancelled` (include the error reason) when it finishes.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop and tell the user to load the plugin with `claude --plugin-dir <path-to-plugins>`.

## Step 2: Gather evidence

Run all checks silently — do not change anything yet.

1. Check `manifest.json` exists in the current working directory. Read it to confirm `runtime.source` is configured. Default the sandbox technology to the source technology, but let the user choose a different supported sandbox technology if they want one. Persist the chosen sandbox endpoint explicitly in `runtime.sandbox`; do not infer it from source after setup is complete.
2. Check that `extraction.schemas` in the manifest is a non-empty array.
3. Check FreeTDS is installed: `brew list --formula freetds 2>/dev/null`. If missing, tell the user to run `brew install freetds` (or run `/init-ad-migration` which auto-installs it) and stop.
4. Inspect `runtime.sandbox.connection` in `manifest.json` and note which env-bound secrets it requires, such as `password_env`. Check that every referenced env var is set (non-empty). Do not print secret values.
5. Verify the test-harness CLI is available: `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness --help`
6. Check `dbt/profiles.yml` exists. This is **required** — without it, `/generate-tests` and `/generate-model` will fail on dbt validation. If missing, stop and tell the user: "No `dbt/profiles.yml` found. Run `/setup-target` to scaffold the dbt project and select a target platform before setting up the sandbox." If present, read the adapter `type:` and compare against `runtime.target.technology` in `manifest.json`. Warn on mismatch but don't block.
7. Verify dbt can connect: `cd dbt && dbt debug`. Check the output shows "Connection test: OK". If it fails, stop and tell the user to check the env vars named by the configured runtime roles and any target-specific values in `profiles.yml`.

## Step 3: Present plan

Show the user what was found:

```text
Sandbox setup:
  manifest.json:     ✓ found  /  ✗ not found
  technology:        sql_server (or whatever value)
  source runtime:    runtime.source present
  extraction.schemas: [dbo, silver, bronze]
  freetds:           ✓ installed  /  ✗ not found
  test-harness CLI:  ✓ available  /  ✗ not found
  dbt profile:       ✓ sqlserver (matches target runtime)  /  ⚠ duckdb (mismatch)  /  ✗ not found (run /setup-target)
  dbt connection:    ✓ OK  /  ✗ failed (check credentials)

  Env-bound runtime secrets:
  SA_PASSWORD:              ✓ set  /  — not set
  ORACLE_SANDBOX_PASSWORD:  ✓ set  /  — not set
  (list only the vars actually referenced by the configured runtime roles)
```

If any required item is missing, explain what needs to be fixed and stop. Do not proceed without all prerequisites met.

Ask the user if they want to proceed.

## Step 4: Execute

After the user confirms, invoke the CLI:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-up
```

Output shape:

```json
{
  "sandbox_database": "__test_abc123def456",
  "status": "ok | partial | error",
  "tables_cloned": ["dbo.Product"],
  "views_cloned": ["dbo.vProduct"],
  "procedures_cloned": ["dbo.usp_load"],
  "errors": [{"code": "TABLE_CLONE_FAILED", "message": "..."}]
}
```

Parse the JSON output and report:

- Sandbox database name (persisted in manifest)
- Number of tables and views cloned
- Number of procedures cloned
- Any errors or warnings

You can also check sandbox existence with `test-harness sandbox-status`:

```json
{"sandbox_database": "__test_abc123def456", "status": "ok | not_found | error", "exists": true}
```

## Step 5: Report

Tell the user the sandbox is ready and provide the database name for use with `/generate-tests` or `test-harness execute`.

If there were errors, list them and recommend checking the source database connectivity.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `test-harness sandbox-up` | 1 | Sandbox creation or schema cloning failed. Report errors from JSON output |
| `test-harness sandbox-up` | 0 + `status: "partial"` | Some tables/procs failed to clone. Report which ones failed, sandbox is still usable |
| `test-harness --help` | non-zero | CLI not installed. Tell user to run `uv sync` in the lib directory |

## Idempotency

Safe to re-run. The CLI recreates the active sandbox endpoint if it already exists for the given run ID.
