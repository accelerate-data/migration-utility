---
name: setup-sandbox
description: Creates a throwaway sandbox database by cloning schema from the source SQL Server. Checks prerequisites and calls the test-harness CLI. Persists sandbox info to manifest.json.
user-invocable: true
---

# Set Up Sandbox

Create a throwaway sandbox database by cloning schema from the source database. The sandbox is used by the test generator to execute stored procedures and capture ground truth output. The database name is auto-generated (`__test_<random_hex>`).

## Progress Tracking

Use `TaskCreate` and `TaskUpdate` to track the automated phases of this command. After the user confirms (Step 3) and before execution begins, create a task for `Create sandbox database`. Update it to `in_progress` when the CLI starts and to `completed` or `cancelled` (include the error reason) when it finishes.

## Step 1: Pre-check

If `CLAUDE_PLUGIN_ROOT` is not set, stop and tell the user to load the plugin with `claude --plugin-dir <path-to-plugins>`.

## Step 2: Gather evidence

Run all checks silently — do not change anything yet.

1. Check `manifest.json` exists in the current working directory. Read it to confirm `technology` and `source_database` are present.
2. Check that `extracted_schemas` in the manifest is a non-empty array.
3. Check FreeTDS is installed: `brew list --formula freetds 2>/dev/null`. If missing, tell the user to run `brew install freetds` (or run `/init-ad-migration` which auto-installs it) and stop.
4. Check whether each MSSQL environment variable is set (non-empty): `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD`. Do not print their values.
5. Verify the test-harness CLI is available: `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness --help`
6. Check `dbt/profiles.yml` exists. This is **required** — without it, `/generate-tests` and `/generate-model` will fail on `dbt compile`/`dbt test`. If missing, stop and tell the user: "No `dbt/profiles.yml` found. Run `/init-dbt` to scaffold the dbt project and select a target platform before setting up the sandbox." If present, read the adapter `type:` and compare against `technology` in `manifest.json` — `sql_server` expects `sqlserver`, `fabric_warehouse` expects `fabric`, `fabric_lakehouse` expects `fabric` or `spark`, `snowflake` expects `snowflake`. Warn on mismatch but don't block.
7. Verify dbt can connect: `cd dbt && dbt debug`. Check the output shows "Connection test: OK". If it fails, stop and tell the user to check credentials — for SQL Server this means `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD` env vars; for other adapters, update placeholder values in `profiles.yml`.

## Step 3: Present plan

Show the user what was found:

```text
Sandbox setup:
  manifest.json:     ✓ found  /  ✗ not found
  technology:        sql_server (or whatever value)
  source_database:   AdventureWorksDW (or whatever value)
  extracted_schemas: [dbo, silver, bronze]
  freetds:           ✓ installed  /  ✗ not found
  test-harness CLI:  ✓ available  /  ✗ not found
  dbt profile:       ✓ sqlserver (matches manifest)  /  ⚠ duckdb (mismatch)  /  ✗ not found (run /init-dbt)
  dbt connection:    ✓ OK  /  ✗ failed (check credentials)

  SQL Server credentials:
  MSSQL_HOST:   ✓ set  /  — not set
  MSSQL_PORT:   ✓ set  /  — not set
  MSSQL_DB:     ✓ set  /  — not set
  SA_PASSWORD:  ✓ set  /  — not set
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

Safe to re-run. The CLI drops and recreates the sandbox database if it already exists for the given run ID.
