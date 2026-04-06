# Troubleshooting and Error Codes

Cross-reference index of error codes, common issues, and fixes organized by pipeline stage.

## Setup errors

These errors occur during project initialization and DDL extraction.

| Code | Message | Command | Fix |
|---|---|---|---|
| `MANIFEST_NOT_FOUND` | `manifest.json` not found in project root | `/status`, any dry-run guard | Run `/setup-ddl` to extract DDL and create the manifest |
| `MANIFEST_CORRUPT` | `manifest.json` is not valid JSON | `/status`, any dry-run guard | Re-run `/setup-ddl` or manually fix the JSON syntax in `manifest.json` |
| `CATALOG_FILE_MISSING` | `catalog/tables/<table>.json` not found | `/status`, any dry-run guard | Run `/setup-ddl` to populate the catalog. If the table was added after initial setup, re-run extraction for the relevant schema |
| `CATALOG_FILE_CORRUPT` | Table catalog file is not valid JSON | `/status`, any dry-run guard | Re-run `/setup-ddl` or manually fix the JSON syntax in the catalog file |

### Common setup issues

**Toolbox not found on PATH**

The `/setup-ddl` skill requires `genai-toolbox` to be available. If the command fails with a toolbox error, ensure `genai-toolbox` is installed and on your `PATH`. In GitHub Actions, the toolbox runs in HTTP mode; locally it runs via stdio.

**MSSQL environment variables not set**

DDL extraction and sandbox creation require these environment variables:

- `MSSQL_HOST` -- SQL Server hostname
- `MSSQL_PORT` -- SQL Server port
- `SA_PASSWORD` -- SQL Server SA password

If these are not set, `/setup-ddl` and `/setup-sandbox` will fail. Set them in your `.env` file (symlinked into worktrees by `setup-worktree.sh`).

**`CLAUDE_PLUGIN_ROOT` not set**

All plugin commands require the `CLAUDE_PLUGIN_ROOT` environment variable. This is set automatically when the `ad-migration` plugin is installed via the marketplace or loaded with `claude --plugin-dir plugin/`. If you see this error, ensure the plugin is installed (see [[Installation and Prerequisites]]).

## Scoping errors

These errors indicate that scoping has not been completed for a table.

| Code | Message | Command | Fix |
|---|---|---|---|
| `SCOPING_NOT_COMPLETED` | `scoping.selected_writer` missing in catalog | `/status`, profile/test-gen/refactor/migrate guards | Run `/scope <table>` or `/analyzing-table <table>` to discover and resolve the writer |
| `STATEMENTS_NOT_RESOLVED` | One or more statements not resolved to `migrate` or `skip` | `/status`, profile/test-gen/refactor/migrate guards | Run `/scope <table>` to resolve remaining statements. Use `/listing-objects show <proc>` to inspect unresolved statements |

### Common scoping issues

**No writer candidates found**

If `/analyzing-table` finds no procedures that write to the table, the table may be populated by an ETL process outside of stored procedures (e.g. ADF copy activity, SSIS). Check `/listing-objects refs <table>` to verify.

**Multiple writer candidates**

When multiple procedures write to the same table, scoping presents all candidates and asks the FDE to select one. Use `/listing-objects show <proc>` to inspect each candidate before choosing.

## Profiling errors

These errors indicate that profiling has not been completed.

| Code | Message | Command | Fix |
|---|---|---|---|
| `PROFILE_NOT_COMPLETED` | Profile section missing or status is not `ok`/`partial` | `/status`, test-gen/refactor/migrate guards | Run `/profile <table>` or `/profiling-table <table>` |

### Common profiling issues

**Profile status is `partial`**

A `partial` status means some profiling questions were not answered. The pipeline can proceed with a partial profile, but the generated model may need manual adjustment. Run `/status <table>` to see which questions are unanswered.

## Test generation errors

These errors relate to sandbox setup and test spec creation.

| Code | Message | Command | Fix |
|---|---|---|---|
| `SANDBOX_NOT_CONFIGURED` | Sandbox metadata (`database`) missing from manifest | `/status`, test-gen/refactor/migrate guards | Run `/setup-sandbox` to create the throwaway test database |
| `TEST_SPEC_NOT_FOUND` | `test-specs/<table>.json` not found | `/status`, refactor/migrate guards | Run `/generate-tests <table>` or `/generating-tests <table>` |
| `SANDBOX_DOWN_FAILED` | Sandbox teardown failed | `/teardown-sandbox` | Check SQL Server connectivity and permissions. Verify the database exists |

### Common test generation issues

**Sandbox database not reachable**

The sandbox requires a running SQL Server instance. Locally, this is typically a Docker container. In CI, it runs as a service container. Verify that `MSSQL_HOST` and `MSSQL_PORT` point to a running instance and that `SA_PASSWORD` is correct.

**Stored procedure requires parameters**

When a stored procedure needs parameters to execute, the test generator infers defaults or asks the FDE inline. If execution fails, check the procedure's parameter list via `/listing-objects show <proc>` and provide appropriate values.

## SQL refactoring errors

These errors indicate that the refactor stage has not been completed.

| Code | Message | Command | Fix |
|---|---|---|---|
| `REFACTOR_NOT_COMPLETED` | Refactor section missing or `refactored_sql` absent | `/status`, migrate guard | Run `/refactor <table>` or `/refactoring-sql <table>` |

### Common refactoring issues

**Audit loop does not converge**

The refactoring skill self-corrects the refactored SQL until the sandbox equivalence audit passes. If the loop does not converge after several iterations, the proc may have side effects (temp tables, dynamic SQL) that complicate equivalence checking. Review the audit output and manually verify the refactored SQL before proceeding.

## Migration errors

These errors occur during dbt model generation.

| Code | Message | Command | Fix |
|---|---|---|---|
| All guards from prior stages | Any unmet prerequisite blocks migration | `/status`, migrate guards | Fix the upstream stage first -- run `/status <table>` to identify the blocking guard |

### Common migration issues

**`dbt test` fails after model generation**

The model generator self-corrects up to 3 iterations when `dbt test` fails. If it still fails after 3 attempts, the code reviewer may kick back for revisions (up to 2 review iterations). Check the test output in the dbt `target/` directory for details.

**Model not found in dbt project**

The model generator writes to `dbt/models/staging/` and `dbt/models/marts/`. If the dbt project was not scaffolded, run `/init-dbt` first. Verify with `/status <table>` that the `dbt_model_exists` flag is set.

## Guard check reference

Quick reference for which guards apply to each stage:

| Guard | scope | profile | test-gen | refactor | migrate |
|---|---|---|---|---|---|
| `manifest_exists` | yes | yes | yes | yes | yes |
| `table_catalog_exists` | yes | yes | yes | yes | yes |
| `selected_writer_set` | | yes | yes | yes | yes |
| `statements_resolved` | | yes | yes | yes | yes |
| `profile_completed` | | | yes | yes | yes |
| `sandbox_configured` | | | yes | yes | yes |
| `test_spec_exists` | | | | yes | yes |
| `refactor_completed` | | | | | yes |

## CLI exit codes

The `migrate-util dry-run` CLI uses these exit codes:

| Exit code | Meaning |
|---|---|
| 0 | Success (check `guards_passed` in JSON output for pass/fail) |
| 1 | Domain failure (invalid stage name, malformed table FQN) |
| 2 | IO or parse error |

## Related pages

- [[Status Dashboard]] -- running guard checks and viewing recommendations
- [[Stage 1 Project Init]] -- project initialization and DDL extraction
- [[Stage 1 Scoping]] -- writer discovery and statement resolution
- [[Stage 2 Profiling]] -- table classification and profiling questions
- [[Stage 3 Test Generation]] -- test spec creation
- [[Stage 4 Sandbox Setup]] -- sandbox database setup
- [[Stage 5 SQL Refactoring]] -- SQL restructuring and equivalence audit
- [[Stage 4 Model Generation]] -- dbt model generation
- [[Cleanup and Teardown]] -- sandbox teardown and worktree cleanup
- [[Glossary]] -- definitions of terms used in error messages
