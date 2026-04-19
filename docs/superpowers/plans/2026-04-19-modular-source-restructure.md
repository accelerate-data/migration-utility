# Modular Source Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the strong and medium large-file candidates into focused modules, and split their oversized tests by behavior area without changing public CLI/API behavior.

**Architecture:** Use parallel workstreams with disjoint write scopes in separate worktrees, then integrate sequentially into one branch. Preserve public import paths with compatibility barrels/facades, move implementation behind support packages, and keep tests close to the module responsibility they verify.

**Tech Stack:** Python 3.11, Typer, Pydantic v2, pytest, MCP Python SDK, existing `shared` package and `mcp/ddl/ddl_mcp_support` patterns.

---

## Parallel Execution Model

Run this work in separate worktrees because parallel agents must not edit the same checkout.

- Main coordinator worktree: current repository checkout.
- Worktree A: `./scripts/worktree.sh modular-sandbox-services`
- Worktree B: `./scripts/worktree.sh modular-source-target-setup`
- Worktree C: `./scripts/worktree.sh modular-batch-plan`
- Worktree D: `./scripts/worktree.sh modular-ddl-mcp-server`
- Worktree E: `./scripts/worktree.sh modular-catalog-diagnostics-dryrun`
- Worktree F: `./scripts/worktree.sh modular-test-suite`

Parallelizable wave:

- Start A, B, C, D, and E at the same time. Their production and test write scopes are disjoint.
- Start F after A-E have opened draft diffs or after their file split names are stable, because F reorganizes broad test files and should follow the module names chosen by each owner.
- Merge/integrate in this order: A, B, C, D, E, F.
- After each integration, run that workstream's focused tests.
- After all integrations, run `cd lib && uv run pytest`, `cd mcp/ddl && uv run pytest`, and `cd tests/evals && npm run eval:smoke` if Node dependencies are present.

All workers must:

- Read `repo-map.json` first.
- Preserve public imports and CLI behavior.
- Use `git add <file>` only.
- Update `repo-map.json` when a module responsibility description becomes stale.
- Add no backward-compatibility shim except a barrel/facade that preserves an existing import path.
- Commit one concern per commit in their worktree.

## File Structure

### Workstream A: Sandbox Services

- Modify: `lib/shared/sandbox/sql_server_services.py`
- Modify: `lib/shared/sandbox/oracle_services.py`
- Create: `lib/shared/sandbox/sql_server_config.py`
- Create: `lib/shared/sandbox/oracle_config.py`
- Create: `lib/shared/sandbox/sql_server_connection.py`
- Create: `lib/shared/sandbox/oracle_connection.py`
- Create: `lib/shared/sandbox/sql_server_clone.py`
- Create: `lib/shared/sandbox/oracle_clone.py`
- Create: `lib/shared/sandbox/sql_server_lifecycle_core.py`
- Create: `lib/shared/sandbox/oracle_lifecycle_core.py`
- Modify: `tests/unit/test_harness/test_sqlserver_sandbox.py`
- Modify: `tests/unit/test_harness/test_oracle_sandbox.py`
- Create: `tests/unit/test_harness/test_sqlserver_sandbox_config.py`
- Create: `tests/unit/test_harness/test_sqlserver_sandbox_clone.py`
- Create: `tests/unit/test_harness/test_oracle_sandbox_config.py`
- Create: `tests/unit/test_harness/test_oracle_sandbox_clone.py`

### Workstream B: Source and Target Setup

- Modify: `lib/shared/generate_sources.py`
- Create: `lib/shared/generate_sources_support/__init__.py`
- Create: `lib/shared/generate_sources_support/candidates.py`
- Create: `lib/shared/generate_sources_support/sources_yaml.py`
- Create: `lib/shared/generate_sources_support/staging.py`
- Modify: `lib/shared/target_setup.py`
- Create: `lib/shared/target_setup_support/__init__.py`
- Create: `lib/shared/target_setup_support/runtime.py`
- Create: `lib/shared/target_setup_support/dbt_scaffold.py`
- Create: `lib/shared/target_setup_support/source_tables.py`
- Create: `lib/shared/target_setup_support/seeds.py`
- Create: `lib/shared/target_setup_support/dbt_commands.py`
- Modify: `tests/unit/generate_sources/test_generate_sources.py`
- Create: `tests/unit/generate_sources/test_candidates.py`
- Create: `tests/unit/generate_sources/test_sources_yaml.py`
- Create: `tests/unit/generate_sources/test_staging_artifacts.py`
- Modify: `tests/unit/target_setup/test_target_setup.py`
- Create: `tests/unit/target_setup/test_runtime.py`
- Create: `tests/unit/target_setup/test_dbt_scaffold.py`
- Create: `tests/unit/target_setup/test_source_tables.py`
- Create: `tests/unit/target_setup/test_seeds.py`
- Create: `tests/unit/target_setup/test_dbt_commands.py`

### Workstream C: Batch Plan

- Modify: `lib/shared/batch_plan.py`
- Create: `lib/shared/batch_plan_support/__init__.py`
- Create: `lib/shared/batch_plan_support/inventory.py`
- Create: `lib/shared/batch_plan_support/scheduling.py`
- Create: `lib/shared/batch_plan_support/nodes.py`
- Create: `lib/shared/batch_plan_support/diagnostics.py`
- Create: `lib/shared/batch_plan_support/dashboard.py`
- Modify: `tests/unit/batch_plan/test_catalog_and_phases.py`
- Modify: `tests/unit/batch_plan/test_scheduling.py`
- Modify: `tests/unit/batch_plan/test_diagnostics_and_exclusions.py`
- Modify: `tests/unit/batch_plan/test_pipeline_status.py`

### Workstream D: DDL MCP Server

- Modify: `mcp/ddl/server.py`
- Create: `mcp/ddl/ddl_mcp_support/tool_definitions.py`
- Create: `mcp/ddl/ddl_mcp_support/tool_handlers.py`
- Create: `mcp/ddl/ddl_mcp_support/server_context.py`
- Modify: `mcp/ddl/tests/unit/test_server.py`
- Create: `mcp/ddl/tests/unit/test_tool_definitions.py`
- Create: `mcp/ddl/tests/unit/test_tool_handlers.py`

### Workstream E: Medium Production Modules

- Modify: `lib/shared/catalog.py`
- Create: `lib/shared/catalog_preservation.py`
- Modify: `lib/shared/init.py`
- Create: `lib/shared/init_support/__init__.py`
- Create: `lib/shared/init_support/source_config.py`
- Create: `lib/shared/init_support/scaffold.py`
- Create: `lib/shared/init_support/local_env.py`
- Create: `lib/shared/diagnostics/__init__.py`
- Create: `lib/shared/diagnostics/registry.py`
- Create: `lib/shared/diagnostics/context.py`
- Create: `lib/shared/diagnostics/runner.py`
- Modify: `lib/shared/dry_run_support/reset.py`
- Create: `lib/shared/dry_run_support/reset_preserve_catalog.py`
- Modify: `lib/shared/dry_run_support/readiness.py`
- Create: `lib/shared/dry_run_support/readiness_context.py`
- Create: `lib/shared/dry_run_support/readiness_stages.py`
- Modify: `tests/unit/catalog/test_catalog.py`
- Create: `tests/unit/catalog/test_catalog_preservation.py`
- Modify: `tests/unit/init/test_init.py`
- Create: `tests/unit/init/test_source_config.py`
- Create: `tests/unit/init/test_scaffold.py`
- Create: `tests/unit/init/test_local_env.py`
- Modify: `tests/unit/diagnostics/test_diagnostics.py`
- Create: `tests/unit/diagnostics/test_registry.py`
- Create: `tests/unit/diagnostics/test_runner.py`
- Create: `tests/unit/diagnostics/test_context.py`
- Modify: `tests/unit/dry_run/test_reset_migration.py`
- Create: `tests/unit/dry_run/test_reset_preserve_catalog.py`
- Modify: `tests/unit/dry_run/test_readiness.py`
- Create: `tests/unit/dry_run/test_readiness_context.py`
- Create: `tests/unit/dry_run/test_readiness_stages.py`

### Workstream F: Oversized Test Suite Cleanup

- Modify only test files not already owned by A-E.
- Candidate files:
  - `tests/unit/migrate/test_migrate.py`
  - `tests/unit/profile/test_profile.py`
  - `tests/unit/refactor/test_refactor.py`
  - `tests/unit/cli/test_pipeline_cmds.py`
  - `tests/unit/cli/test_sandbox_cmds.py`
  - `tests/unit/test_harness/test_cli.py`
  - `tests/integration/sql_server/test_harness/test_compare_sql_sql_server.py`
  - `tests/integration/sql_server/test_harness/test_test_harness_sql_server.py`
  - `tests/integration/oracle/test_harness/test_test_harness_oracle.py`
- Create helper modules only when they remove real duplication, for example `tests/unit/migrate/helpers.py` or `tests/unit/test_harness/helpers.py`.

## Task 1: Workstream A - Split Sandbox Services

**Files:** Workstream A files only.

- [ ] Move SQL Server `from_env` configuration loading from `sql_server_services.py` into `sql_server_config.py`.
- [ ] Move Oracle `from_env` configuration loading from `oracle_services.py` into `oracle_config.py`.
- [ ] Move SQL Server connection context managers into `sql_server_connection.py`.
- [ ] Move Oracle connection context managers into `oracle_connection.py`.
- [ ] Move SQL Server table/view/procedure clone helpers into `sql_server_clone.py`.
- [ ] Move Oracle table/view/procedure clone helpers into `oracle_clone.py`.
- [ ] Move sandbox DB/PDB create/drop/schema lifecycle helpers into `*_lifecycle_core.py`.
- [ ] Keep `SqlServerSandbox` and `OracleSandbox` import behavior unchanged from the current public facades.
- [ ] Keep `sql_server_services.py` and `oracle_services.py` as compatibility modules that compose the new helpers and export the same symbols currently imported by tests.
- [ ] Split `test_sqlserver_sandbox.py` and `test_oracle_sandbox.py` by responsibility into config, clone, lifecycle, and remaining facade tests.
- [ ] Run: `cd lib && uv run pytest ../tests/unit/test_harness/test_sqlserver_sandbox.py ../tests/unit/test_harness/test_sqlserver_sandbox_config.py ../tests/unit/test_harness/test_sqlserver_sandbox_clone.py ../tests/unit/test_harness/test_oracle_sandbox.py ../tests/unit/test_harness/test_oracle_sandbox_config.py ../tests/unit/test_harness/test_oracle_sandbox_clone.py`
- [ ] Commit: `git add <Workstream A files> && git commit -m "refactor: split sandbox service modules"`

Acceptance checks:

- `rg "_SqlServerSandboxCore\\." lib/shared/sandbox` returns no delegation from new service classes back into old core behavior.
- `rg "_OracleSandboxCore\\." lib/shared/sandbox` returns no delegation from new service classes back into old core behavior.
- Existing sandbox public imports still work:

```bash
cd lib && uv run python - <<'PY'
from shared.sandbox.sql_server import SqlServerSandbox
from shared.sandbox.oracle import OracleSandbox
print(SqlServerSandbox, OracleSandbox)
PY
```

## Task 2: Workstream B - Split Source Generation and Target Setup

**Files:** Workstream B files only.

- [ ] Extract source-table catalog classification and source namespace validation into `generate_sources_support/candidates.py`.
- [ ] Extract dbt `sources.yml` construction, column tests, freshness, uniqueness, and relationship tests into `generate_sources_support/sources_yaml.py`.
- [ ] Extract staging wrapper SQL, staging model YAML, unit-test rendering, stale wrapper cleanup, and artifact writing into `generate_sources_support/staging.py`.
- [ ] Keep `generate_sources.py` as the Typer command and public orchestration facade exporting `generate_sources`, `write_sources_yml`, and `list_confirmed_source_tables`.
- [ ] Extract target runtime env writing into `target_setup_support/runtime.py`.
- [ ] Extract dbt project/profile rendering and scaffold creation into `target_setup_support/dbt_scaffold.py`.
- [ ] Extract target source-table spec loading and materialization into `target_setup_support/source_tables.py`.
- [ ] Extract seed-table spec loading, CSV rendering, seed YAML rendering, export, and `dbt seed` materialization into `target_setup_support/seeds.py`.
- [ ] Extract dbt compile/build command helpers into `target_setup_support/dbt_commands.py`.
- [ ] Keep `target_setup.py` as the public orchestration facade exporting all existing public functions.
- [ ] Split `test_generate_sources.py` into candidate, source YAML, staging artifact, and command/facade coverage.
- [ ] Split `test_target_setup.py` into runtime, dbt scaffold, source table, seed, dbt command, and facade orchestration coverage.
- [ ] Run: `cd lib && uv run pytest ../tests/unit/generate_sources ../tests/unit/target_setup`
- [ ] Commit: `git add <Workstream B files> && git commit -m "refactor: split source and target setup modules"`

Acceptance checks:

- `cd packages/ad-migration-internal && uv run generate-sources --help` exits `0`.
- `cd packages/ad-migration-cli && uv run ad-migration setup-target --help` exits `0`.
- `from shared.generate_sources import generate_sources, write_sources_yml` still works.
- `from shared.target_setup import run_setup_target, write_target_runtime_from_env` still works.

## Task 3: Workstream C - Split Batch Plan

**Files:** Workstream C files only.

- [ ] Move `_CatalogInventory`, `_enumerate_catalog`, and catalog object classification into `batch_plan_support/inventory.py`.
- [ ] Move `_topological_batches`, `_classify_phases`, and `_compute_blocking_deps` into `batch_plan_support/scheduling.py`.
- [ ] Move `_make_node`, `_resolve_excluded_type`, and `_build_plan_output` into `batch_plan_support/nodes.py`.
- [ ] Move `_collect_catalog_diagnostics` into `batch_plan_support/diagnostics.py`.
- [ ] Move `_runtime_role_is_configured`, `_test_gen_setup_block`, `_pipeline_cells`, `_build_pipeline_rows`, `_build_diagnostic_rows`, `_build_next_action`, and `_build_status_dashboard` into `batch_plan_support/dashboard.py`.
- [ ] Keep `batch_plan.py` as the public coordinator exposing `build_batch_plan`.
- [ ] Split or adjust existing `tests/unit/batch_plan/*` so each test file imports from the new support module that owns the behavior under test.
- [ ] Run: `cd lib && uv run pytest ../tests/unit/batch_plan`
- [ ] Commit: `git add <Workstream C files> && git commit -m "refactor: split batch plan support modules"`

Acceptance checks:

- `from shared.batch_plan import build_batch_plan` still works.
- Existing output model shape remains unchanged for representative tests in `tests/unit/batch_plan/test_scheduling.py`.

## Task 4: Workstream D - Split DDL MCP Server

**Files:** Workstream D files only.

- [ ] Move project-root, catalog, dialect, cache-token, argument validation, and column parsing helpers into `ddl_mcp_support/server_context.py`.
- [ ] Move the `types.Tool(...)` declarations from `list_tools` into `ddl_mcp_support/tool_definitions.py`.
- [ ] Move the `call_tool` dispatch branches into explicit handler functions in `ddl_mcp_support/tool_handlers.py`.
- [ ] Keep `mcp/ddl/server.py` as the MCP bootstrap exposing `list_tools`, `call_tool`, and `_main`.
- [ ] Add tool-definition tests that assert the tool names and required schemas are unchanged.
- [ ] Add handler tests for at least `list_tables`, `get_table_schema`, `get_dependencies`, and one missing-argument error.
- [ ] Run: `cd mcp/ddl && uv run pytest tests/unit/test_server.py tests/unit/test_tool_definitions.py tests/unit/test_tool_handlers.py`
- [ ] Commit: `git add <Workstream D files> && git commit -m "refactor: split ddl mcp server handlers"`

Acceptance checks:

- `cd mcp/ddl && uv run pytest` passes.
- `mcp/ddl/server.py` remains the entrypoint named by `repo-map.json`.

## Task 5: Workstream E - Split Medium Production Modules

**Files:** Workstream E files only.

- [ ] Move `snapshot_enriched_fields` and `restore_enriched_fields` from `catalog.py` into `catalog_preservation.py`.
- [ ] Re-export those functions from `catalog.py` to preserve existing imports.
- [ ] Move `SourceConfig`, source config registry, and `get_source_config` from `init.py` into `init_support/source_config.py`.
- [ ] Move project scaffold behavior from `init.py` into `init_support/scaffold.py`.
- [ ] Move local env override behavior from `init.py` into `init_support/local_env.py`.
- [ ] Keep `init.py` as public CLI/facade wrapper for existing Typer commands and public functions.
- [ ] Move diagnostics registry types and decorators into `diagnostics/registry.py`.
- [ ] Move catalog context construction helpers into `diagnostics/context.py`.
- [ ] Move check execution, result writing, and `run_diagnostics` into `diagnostics/runner.py`.
- [ ] Keep `diagnostics/__init__.py` as a compatibility barrel that exports `DiagnosticResult`, `CatalogContext`, `diagnostic`, `registry`, and `run_diagnostics`.
- [ ] Move preserve-catalog reset staging, rollback, deletion, and restore behavior into `dry_run_support/reset_preserve_catalog.py`.
- [ ] Move readiness object context loading into `dry_run_support/readiness_context.py`.
- [ ] Move per-stage readiness policy functions into `dry_run_support/readiness_stages.py`.
- [ ] Keep `reset.py` and `readiness.py` as public orchestration modules.
- [ ] Split affected tests into preservation, init source/scaffold/env, diagnostics registry/context/runner, reset preserve-catalog, and readiness context/stage tests.
- [ ] Run: `cd lib && uv run pytest ../tests/unit/catalog ../tests/unit/init ../tests/unit/diagnostics ../tests/unit/dry_run`
- [ ] Commit: `git add <Workstream E files> && git commit -m "refactor: split catalog diagnostics init and dry-run modules"`

Acceptance checks:

- Existing imports still work:

```bash
cd lib && uv run python - <<'PY'
from shared.catalog import snapshot_enriched_fields, restore_enriched_fields
from shared.init import get_source_config, run_scaffold_project
from shared.diagnostics import DiagnosticResult, CatalogContext, diagnostic, run_diagnostics
from shared.dry_run_support.readiness import run_ready
from shared.dry_run_support.reset import run_reset_migration
print("imports ok")
PY
```

## Task 6: Workstream F - Split Remaining Oversized Tests

**Files:** Workstream F files only, excluding files touched by A-E unless the owning workstream has already landed.

- [ ] Split `tests/unit/migrate/test_migrate.py` by behavior:
  - `test_context.py`
  - `test_context_views.py`
  - `test_write.py`
  - `test_write_generate.py`
  - keep shared fixtures in `tests/unit/migrate/conftest.py` or `tests/unit/migrate/helpers.py`.
- [ ] Split `tests/unit/profile/test_profile.py` by behavior:
  - `test_table_context.py`
  - `test_table_write.py`
  - `test_view_context.py`
  - `test_view_write.py`
  - keep writer-slice helpers in `tests/unit/profile/helpers.py`.
- [ ] Split `tests/unit/refactor/test_refactor.py` by behavior:
  - `test_context.py`
  - `test_write.py`
  - `test_view_context.py`
  - `test_models.py`.
- [ ] Split CLI tests by command family:
  - `tests/unit/cli/test_reset_cmd.py`
  - `tests/unit/cli/test_source_seed_cmds.py`
  - `tests/unit/cli/test_setup_sandbox_cmd.py`
  - `tests/unit/cli/test_teardown_sandbox_cmd.py`.
- [ ] Split test harness CLI tests by command family:
  - `tests/unit/test_harness/test_manifest_cli.py`
  - `tests/unit/test_harness/test_sandbox_cli.py`
  - `tests/unit/test_harness/test_execute_spec_cli.py`
  - `tests/unit/test_harness/test_corrupt_json.py`.
- [ ] Split integration harness tests only if the split is mechanical and does not duplicate expensive setup:
  - SQL Server compare SQL equivalence/not-equivalence/fixtures/rollback/validation files.
  - SQL Server sandbox lifecycle/scenario/identity/view/select files.
  - Oracle sandbox lifecycle/scenario/compare/view/PDB/select files.
- [ ] Run: `cd lib && uv run pytest ../tests/unit/migrate ../tests/unit/profile ../tests/unit/refactor ../tests/unit/cli ../tests/unit/test_harness`
- [ ] Run integration splits only if local services are available:
  - `cd lib && uv run pytest ../tests/integration/sql_server/test_harness`
  - `cd lib && uv run pytest ../tests/integration/oracle/test_harness`
- [ ] Commit: `git add <Workstream F files> && git commit -m "test: split oversized behavior suites"`

Acceptance checks:

- No test file touched by F remains over 700 lines unless it is an integration suite with shared expensive setup that would become slower or more duplicated after splitting.
- Test helper modules contain only reusable setup/builders, not assertions hidden from test cases.

## Task 7: Integration and Repo Metadata

**Files:**

- Modify: `repo-map.json`
- Modify: only files required to resolve import conflicts after merging A-F.

- [ ] Merge Workstream A into the integration branch and run its focused tests.
- [ ] Merge Workstream B into the integration branch and run its focused tests.
- [ ] Merge Workstream C into the integration branch and run its focused tests.
- [ ] Merge Workstream D into the integration branch and run its focused tests.
- [ ] Merge Workstream E into the integration branch and run its focused tests.
- [ ] Merge Workstream F into the integration branch and run its focused tests.
- [ ] Update `repo-map.json` module descriptions for every new support package that is now part of the architecture.
- [ ] Run: `cd lib && uv run pytest`
- [ ] Run: `cd mcp/ddl && uv run pytest`
- [ ] Run: `cd tests/evals && npm run eval:smoke` when `tests/evals/node_modules` exists or after `npm ci`.
- [ ] Run: `markdownlint docs/superpowers/plans/2026-04-19-modular-source-restructure.md`
- [ ] Commit: `git add repo-map.json docs/superpowers/plans/2026-04-19-modular-source-restructure.md <conflict-resolution-files> && git commit -m "docs: update module map after modular restructure"`

Acceptance checks:

- No public command entrypoint named in `repo-map.json` moved.
- `rg "from shared\\.(generate_sources|target_setup|batch_plan|catalog|init|diagnostics)" tests lib packages` does not reveal broken imports.
- Strong candidates are no longer above 500 lines unless the remaining file is a public facade.
- Medium candidates are split where responsibilities were separable; model/contract-only files may remain large.

## Final Verification

- [ ] Run `git status --short` and confirm only intended files are changed.
- [ ] Run `rg --files lib mcp packages scripts -g '!**/.venv/**' -g '!**/uv.lock' -g '!**/__pycache__/**' | xargs wc -l | awk '$1 > 300 && $2 != "total" {print $1, $2}' | sort -nr` and review remaining source files over 300 lines.
- [ ] Document any remaining large files intentionally left as cohesive contract, parser, extractor, or fixture modules in the PR body.
- [ ] Use `requesting-code-review` before opening a PR.
