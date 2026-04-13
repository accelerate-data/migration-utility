---
name: setup-target
description: Collects target runtime information, scaffolds the dbt project, and generates target-mapped sources.yml. This is the canonical target setup phase and replaces init-dbt.
user-invocable: true
argument-hint: "[project-root-path]"
---

# Set Up Target

Configure the migration target, scaffold the dbt project, and generate `sources.yml` against the configured target source schema.

`/setup-target` is the only target setup flow. It replaces `/init-dbt`.

## Guards

- `manifest.json` must exist. If missing, stop and tell the user to run `/setup-ddl` first.
- `catalog/tables/` must exist.
- In-scope tables must be analyzed before target setup proceeds. Run:

  ```bash
  uv run --project "${CLAUDE_PLUGIN_ROOT}/lib" migrate-util batch-plan
  ```

  If `scope_phase` is non-empty, stop and tell the user to finish analysis before continuing.

## Runtime contract

`manifest.json` is the source of truth.

- `runtime.source` describes the extracted source endpoint.
- `runtime.target` describes the target endpoint used by dbt validation and materialization.
- `runtime.sandbox` describes the execution endpoint used for ground-truth and equivalence flows.

Target setup must collect and persist a full independent `runtime.target`. Do not derive target credentials from source credentials.

## What setup-target does

1. Collect target technology and connection/runtime information from the user.
2. Collect the target source schema for dbt `source()` relations. Default: `bronze`.
3. Persist `runtime.target` into `manifest.json`.
4. Scaffold `dbt/` if it does not exist.
5. Reuse the shared `generate-sources` artifact flow, but remap physical source tables to `runtime.target.schemas.source`.
6. Write `dbt/models/staging/sources.yml`.
7. Validate the target-facing dbt setup with `dbt deps` and `dbt compile`.

## Target schema rule

Logical source names remain grouped by extracted schema. Physical resolution on the target uses `runtime.target.schemas.source`.

Example:

- logical source name: `silver`
- physical target schema: `bronze`

This keeps source naming stable while allowing the target landing schema to be independent.

## Idempotency

Safe to re-run.

- Re-running updates `runtime.target`
- re-generates `sources.yml`
- preserves existing edited `profiles.yml` values unless this command is explicitly rewriting them
- applies target-side source changes as a delta after new source tables are added

## Next steps

- Run `/setup-sandbox` to configure an execution endpoint for proof-backed testing.
- Run `/generate-tests`, `/refactor`, and `/generate-model` after the target and sandbox are configured.
