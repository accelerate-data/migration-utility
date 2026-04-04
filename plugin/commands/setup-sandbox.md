---
name: setup-sandbox
description: Creates a throwaway sandbox database by cloning schema from the source SQL Server. Checks prerequisites and calls the test-harness CLI. Persists sandbox info to manifest.json.
user-invocable: true
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
4. Verify the test-harness CLI is available: `uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness --help`
5. Check `dbt/profiles.yml` exists. This is **required** — without it, `/generate-tests` and `/generate-model` will fail on `dbt compile`/`dbt test`. If missing, stop and tell the user: "No `dbt/profiles.yml` found. Run `/init-dbt` to scaffold the dbt project and select a target platform before setting up the sandbox."
6. If `dbt/profiles.yml` exists, read the adapter `type:` from the active profile and compare against `technology` in `manifest.json`:

| manifest.technology | Expected dbt adapter |
|---|---|
| `sql_server` | `sqlserver` |
| `fabric_warehouse` | `fabric` |
| `fabric_lakehouse` | `fabric` or `spark` |
| `snowflake` | `snowflake` |

If the adapter doesn't match, warn the user: "dbt profile uses `<adapter>` but manifest technology is `<technology>`. Run `/init-dbt` to reconfigure the dbt target." This is a warning, not a blocker — the sandbox itself doesn't depend on dbt, but downstream commands will use the wrong dialect.

7. Verify dbt can connect with the configured credentials: `cd dbt && dbt debug`. Check that the output shows "Connection test: OK". If it fails, stop and tell the user to check their credentials — for SQL Server this means verifying `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, and `SA_PASSWORD` env vars; for other adapters, check `profiles.yml` placeholder values.

## Step 3: Present plan

Show the user what was found:

```text
Sandbox setup:
  manifest.json:     ✓ found  /  ✗ not found
  technology:        sql_server (or whatever value)
  source_database:   AdventureWorksDW (or whatever value)
  extracted_schemas: [dbo, silver, bronze]
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

After the user confirms:

```bash
uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" test-harness sandbox-up
```

The CLI generates a run ID, creates the sandbox, and writes `sandbox.run_id` and `sandbox.database` into `manifest.json`. Parse the JSON output and report:

- Sandbox database name
- Run ID (persisted in manifest)
- Number of tables cloned
- Number of procedures cloned
- Any errors or warnings

## Step 5: Report

Tell the user the sandbox is ready and provide the run ID for use with `/generate-tests` or `test-harness execute`.

If there were errors, list them and recommend checking the source database connectivity.

## Error handling

| Command | Exit code | Action |
|---|---|---|
| `test-harness sandbox-up` | 1 | Sandbox creation or schema cloning failed. Report errors from JSON output |
| `test-harness sandbox-up` | 0 + `status: "partial"` | Some tables/procs failed to clone. Report which ones failed, sandbox is still usable |
| `test-harness --help` | non-zero | CLI not installed. Tell user to run `uv sync` in the lib directory |

Errors in JSON output use this format:

```json
{"code": "TABLE_CLONE_FAILED", "message": "Failed to clone [silver].[DimProduct]: ..."}
```

## Idempotency

Safe to re-run. The CLI drops and recreates the sandbox database if it already exists for the given run ID.
