# Stage 4 -- Sandbox Setup

Creates a throwaway database (`__test_<random_hex>`) by cloning schema and procedures from the source SQL Server.

## Prerequisites

- `manifest.json` with `runtime.source`
- `extraction.schemas` must be a non-empty array in the manifest
- All four MSSQL environment variables set
- `test-harness` CLI available
- A dbt project must exist (`dbt_project.yml`) and `dbt debug` must pass

## What it does

1. Verifies all prerequisites and presents a status summary
2. Asks for confirmation before proceeding
3. Runs `test-harness sandbox-up` to create the throwaway database
4. Persists `runtime.sandbox` to `manifest.json`
5. Reports the sandbox environment name and clone statistics

## Invocation

```text
/setup-sandbox
```

## Idempotency

Safe to re-run. The CLI drops and recreates the sandbox database if it already exists for the given run ID.

## Next Step

Proceed to [[Stage 3 Test Generation]] to generate test specs for your stored procedures.
