# Stage 4 -- Sandbox Setup

Creates a throwaway sandbox execution environment and persists it as `runtime.sandbox` in `manifest.json`.

## Prerequisites

- `manifest.json` with `runtime.source`
- `extraction.schemas` must be a non-empty array in the manifest
- The env-bound secrets referenced by the configured sandbox runtime must be set
- `test-harness` CLI available
- A dbt project must exist (`dbt_project.yml`) and `dbt debug` must pass against `runtime.target`

## What it does

1. Verifies all prerequisites and presents a status summary
2. Asks for confirmation before proceeding
3. Runs `test-harness sandbox-up` to create the sandbox endpoint
4. Persists `runtime.sandbox` to `manifest.json`
5. Reports the sandbox environment name and clone statistics

## Invocation

```bash
ad-migration setup-sandbox
```

Use `--yes` to skip the confirmation prompt (useful in scripts).

## Idempotency

Safe to re-run. The CLI recreates the active sandbox endpoint for the current project state.

## Next Step

Proceed to [[Stage 3 Test Generation]] to generate test specs for your stored procedures.
