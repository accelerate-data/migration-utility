---
name: setup-sandbox
description: Creates a throwaway sandbox database by cloning schema from the source SQL Server. Checks prerequisites, generates a run ID, and calls the test-harness CLI.
---

# Set Up Sandbox

Create a throwaway sandbox database (`__test_<run_id>`) by cloning schema from the source database. The sandbox is used by the test generator to execute stored procedures and capture ground truth output.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop and tell the user to load the plugin with `claude --plugin-dir <path-to-plugins>`.

## Step 2: Gather evidence

Run all checks silently — do not change anything yet.

1. Check `manifest.json` exists in the current working directory. Read it to confirm `technology` and `source_database` are present.
2. Check that `extracted_schemas` in the manifest is a non-empty array.
3. Check whether each MSSQL environment variable is set (non-empty): `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. Do not print their values.
4. Verify the test-harness CLI is available:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness --help
```

## Step 3: Present plan

Show the user what was found:

```text
Sandbox setup:
  manifest.json:     ✓ found  /  ✗ not found
  technology:        sql_server (or whatever value)
  source_database:   AdventureWorksDW (or whatever value)
  extracted_schemas: [dbo, silver, bronze]
  test-harness CLI:  ✓ available  /  ✗ not found

  SQL Server credentials:
  MSSQL_HOST:   ✓ set  /  — not set
  MSSQL_PORT:   ✓ set  /  — not set
  MSSQL_DB:     ✓ set  /  — not set
  SA_PASSWORD:  ✓ set  /  — not set
```

If any required item is missing, explain what needs to be fixed and stop. Do not proceed without all prerequisites met.

Ask the user if they want to proceed. If the user provides a run ID, use it. Otherwise generate a new UUID.

## Step 4: Execute

After the user confirms:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/../../lib" test-harness sandbox-up \
  --run-id <uuid>
```

Parse the JSON output and report:

- Sandbox database name
- Number of tables cloned
- Number of procedures cloned
- Any errors or warnings

## Step 5: Report

Tell the user the sandbox is ready and provide the run ID for use with `/generate-tests` or `test-harness execute`.

If there were errors, list them and recommend checking the source database connectivity.

## Idempotency

Safe to re-run. The CLI drops and recreates the sandbox database if it already exists for the given run ID.
