# Tiered Eval Groups

## Decision

Add grouped package-level eval entrypoints in `tests/evals/package.json`:

- `eval:smoke`
- `eval:skills`
- `eval:commands`

Keep the existing package-per-skill and package-per-command Promptfoo configs under `tests/evals/packages/` as the source of truth. Do not add new package layers or aggregate Promptfoo config files.

Mark exactly one scenario in each package config with a `[smoke]` prefix in the test description. `eval:smoke` selects those scenarios with Promptfoo `--filter-pattern`, so the smoke run executes one representative scenario per package.

Treat `oracle-live` and `mssql-live` as the only standalone platform-dependent eval suites because they uniquely cover live `setup-ddl extract` behavior. Do not include them in the grouped package runs.

Move Oracle post-extract command coverage out of the standalone `oracle-regression` suite and into the existing package configs. Those command flows share the same post-extract code paths as the main package evals, even when the local extracted DDL artifacts are Oracle-flavored.

Do not add `eval:full`. The grouped runs are intended for fast local iteration, and a catch-all full suite would be too slow to be useful in that role.

## Why

The current eval harness only supports per-package runs or manual multi-command sequences. Grouped entrypoints provide a fast default path without changing eval content or command behavior.

Keeping smoke tags in the existing package configs preserves a single source of truth and makes package ownership obvious. Adding a second manifest for grouping would create drift risk with little benefit.

Keeping only the live Oracle and SQL Server suites separate preserves the real platform-specific coverage boundary: live extraction. Post-extract command coverage belongs with the shared package evals.

## Implementation Notes

- Add grouped npm scripts in `tests/evals/package.json`.
- Prefix one existing test description per package with `[smoke]`.
- Fold the Oracle regression scenarios into the relevant package config files.
- Remove stale `oracle-regression` script and documentation references.
- Update `repo-map.json` because the eval package structure and command inventory change.

## Verification

- Run `node --test scripts/run-workspace-extension.test.js`.
- Run the new grouped scripts with package-level coverage checks.
- Verify `eval:smoke` selects only the `[smoke]` scenario from each package.
