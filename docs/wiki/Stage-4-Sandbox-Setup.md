# Stage 4 -- Sandbox Setup

Creates a throwaway sandbox execution environment and persists it as `runtime.sandbox` in `manifest.json`.

## Prerequisites

- `manifest.json` with `runtime.sandbox`
- `extraction.schemas` must be a non-empty array in the manifest
- The env-bound secrets referenced by the configured sandbox runtime must be set
- the sandbox backend must be available for the configured technology

## What it does

1. Loads the configured sandbox runtime from `manifest.json`
2. Validates the sandbox environment variables for that technology
3. Writes the current sandbox connection details into `manifest.json`
4. Asks for confirmation unless `--yes` is used
5. Provisions the sandbox by cloning the extracted schemas
6. Persists the active sandbox name in `manifest.json`
7. Reports clone counts and the sandbox status

## Invocation

```bash
ad-migration setup-sandbox
```

Use `--yes` to skip the confirmation prompt (useful in scripts).

## Idempotency

Safe to re-run. The CLI recreates the active sandbox endpoint for the current project state.

## Connection variables

`setup-sandbox` requires the sandbox runtime variables for the configured technology. See:

- [[SQL Server Connection Variables]]
- [[Oracle Connection Variables]]

## Next Step

Proceed to [[Stage 3 Test Generation]] to generate test specs for your stored procedures.
