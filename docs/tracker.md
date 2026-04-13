# Review Fix Tracker

This tracker is the working checklist for addressing the code review findings from 2026-04-13.

## Status Legend

- [ ] Not started
- [~] In progress
- [x] Done
- [!] Blocked

## Tasks

- [x] Task 7: Address follow-up review feedback from Claude
  Findings addressed:
  - Restore DDL MCP caching with safe invalidation.
  - Fix confirmed config, error-handling, and cleanup issues from the second review pass.
  Files:
  - `plugin/mcp/ddl/server.py`
  - `plugin/mcp/ddl/tests/unit/test_server.py`
  - `plugin/lib/shared/sandbox/base.py`
  - `plugin/lib/shared/sandbox/sql_server.py`
  - `plugin/lib/shared/sandbox/oracle.py`
  - `plugin/lib/shared/dry_run_core.py`
  - `plugin/lib/shared/test_harness.py`
  - `tests/unit/test_harness/test_test_harness.py`
  - `tests/unit/dry_run/test_dry_run.py`
  Verification:
  - `cd plugin/mcp/ddl && uv run pytest tests/unit/test_server.py -k 'reuses_cached_catalog_when_files_unchanged or reloads_catalog_after_ddl_changes or requires_name_argument or requires_table_name_argument or uses_cached_catalog_dialect'`
  - `cd plugin/lib && uv run pytest ../../tests/unit/dry_run/test_dry_run.py -k 'reset_migration_cli_corrupt_catalog_exits_2 or reset_migration_mixed_valid_and_missing or reset_migration_not_found_returns_without_mutation or ready_scope_no_catalog_file or status_single_missing_object_reports_not_found'`
  - `cd plugin/lib && uv run pytest ../../tests/unit/test_harness/test_test_harness.py -k 'from_env_uses_explicit_runtime_roles or TestSandboxDbNameGeneration or TestOracleSandboxName or partial_failure or TestExecuteSelectOracle or TestCompareTwoSqlOracle'`
  - `cd plugin/lib && uv run pytest ../../tests/unit/test_harness/test_test_harness.py`
  - `cd plugin/lib && uv run pytest ../../tests/unit/dry_run/test_dry_run.py`
  - `cd plugin/mcp/ddl && uv run pytest tests/unit/test_server.py`
  Results:
  - `test_harness`: 129 passed
  - `dry_run`: 67 passed
  - `ddl_mcp`: 34 passed

- [x] Task 1: Create execution tracker and keep it current
  Files:
  - `docs/tracker.md`
  Notes:
  - Tracker created at start of execution.

- [x] Task 2: Fix `execute_spec` partial-failure exit semantics
  Findings addressed:
  - `plugin/lib/shared/test_harness.py` exits `0` when some scenarios fail.
  Files:
  - `plugin/lib/shared/test_harness.py`
  - `tests/unit/test_harness/test_test_harness.py`
  Verification:
  - `cd plugin/lib && uv run pytest ../../tests/unit/test_harness/test_test_harness.py -k 'partial_failure or all_fail_exits_1 or writes_expect_rows'`

- [x] Task 3: Fix DDL MCP stale-cache behavior
  Findings addressed:
  - `plugin/mcp/ddl/server.py` reuses cached catalog state after startup.
  Files:
  - `plugin/mcp/ddl/server.py`
  - `plugin/mcp/ddl/tests/unit/test_server.py`
  Verification:
  - `cd plugin/mcp/ddl && uv run pytest tests/unit/test_server.py -k 'reloads_catalog_after_ddl_changes or uses_cached_catalog_dialect'`

- [x] Task 4: Fix dry-run missing-object handling and partial reset behavior
  Findings addressed:
  - Missing objects are silently treated as tables.
  - `run_reset_migration` blocks the full batch on a single bad target.
  Files:
  - `plugin/lib/shared/catalog.py`
  - `plugin/lib/shared/dry_run_core.py`
  - `tests/unit/dry_run/test_dry_run.py`
  Verification:
  - `cd plugin/lib && uv run pytest ../../tests/unit/dry_run/test_dry_run.py -k 'ready_scope_no_catalog_file or status_single_missing_object_reports_not_found or reset_migration_mixed_valid_and_missing or reset_migration_not_found_returns_without_mutation or status_single_object'`

- [x] Task 5: Reduce sandbox backend duplication at the execution/comparison seam
  Findings addressed:
  - Oversized duplicated logic in SQL Server and Oracle sandbox backends.
  - Dialect behavior drift in SQL comparison flow.
  Files:
  - `plugin/lib/shared/sandbox/base.py`
  - `plugin/lib/shared/sandbox/sql_server.py`
  - `plugin/lib/shared/sandbox/oracle.py`
  - `plugin/lib/shared/sandbox/duckdb.py`
  - `tests/unit/test_harness/test_test_harness.py`
  Verification:
  - `cd plugin/lib && uv run pytest ../../tests/unit/test_harness/test_test_harness.py -k 'partial_failure or all_fail_exits_1 or writes_expect_rows or TestExecuteSelectOracle or TestCompareTwoSqlOracle or compare_two_sql_reports_equivalence'`

- [x] Task 6: Run targeted verification and finalize tracker
  Verification commands:
  - `cd plugin/lib && uv run pytest ../../tests/unit/test_harness/test_test_harness.py`
  - `cd plugin/lib && uv run pytest ../../tests/unit/dry_run/test_dry_run.py`
  - `cd plugin/mcp/ddl && uv run pytest tests/unit/test_server.py`
  Results:
  - `test_harness`: 129 passed
  - `dry_run`: 66 passed
  - `ddl_mcp`: 31 passed
