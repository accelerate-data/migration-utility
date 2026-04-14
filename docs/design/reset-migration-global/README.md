# Reset Migration Global Mode

`/reset-migration` supports both object-scoped stage resets and a global reset mode for wiping migration state while preserving local project scaffolding from `/init-ad-migration`.

## Decisions

- Extend `/reset-migration` with a distinct global mode: `/reset-migration all`.
- Require an explicit destructive confirmation before global reset executes.
- If a sandbox runtime is configured, global reset must run `/teardown-sandbox` first and stop on teardown failure.
- Global reset clears migration state completely, including extracted artifacts, generated artifacts, and runtime endpoints in `manifest.json`.
- `/init-ad-migration` scaffolding remains in place because it represents local machine/project setup rather than migration state.

## Required global reset behavior

- Preserve project scaffolding such as `CLAUDE.md`, `README.md`, `repo-map.json`, `.claude/`, `.githooks/`, `.envrc`, and local `.env`.
- Remove extracted migration artifacts such as `ddl/`, `catalog/`, and extraction staging/intermediate files.
- Remove generated migration artifacts such as test artifacts and dbt outputs produced by the migration pipeline.
- Clear `manifest.json` migration runtime state, including `runtime.source`, `runtime.target`, `runtime.sandbox`, extraction metadata, and init/setup handoff state that should be rebuilt from a fresh extraction flow.
- Leave the project in a state where the next required pipeline step is `/setup-ddl`.

## Confirmation contract

- The command must present a preflight summary that makes the blast radius explicit.
- The confirmation step must require an affirmative destructive confirmation, not a passive default.
- The summary must state that:
  - sandbox teardown will run first when configured
  - source, target, and sandbox runtime configuration will be removed
  - extracted and generated migration artifacts will be deleted
  - init scaffolding and local environment setup will remain

## Why this matters

Agents need a deterministic way to reset a migration project to a clean post-init state without accidentally removing laptop-specific setup or leaving stale runtime/catalog state behind.
