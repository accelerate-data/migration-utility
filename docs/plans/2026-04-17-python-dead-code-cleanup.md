# Python Dead Code Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove high-confidence Python dead-code findings while preserving dynamic entrypoints, registry-driven functions, and public compatibility contracts.

**Architecture:** Treat this as import hygiene plus narrow test cleanup, not a broad deletion pass. Static tools report many false positives for Pydantic fields, Typer callbacks, diagnostic decorators, lazy exports, and console-script adapters; keep those intact unless a task explicitly names the symbol for removal.

**Tech Stack:** Python 3.11, uv, pytest, Ruff `F401/F841`, Vulture for post-cleanup signal checking.

---

## Baseline

Run from `/Users/hbanerjee/src/worktrees/dead-code-python-cleanup`.

- `cd lib && uv run pytest`
- Expected baseline: `1539 passed`
- `uvx ruff check lib/shared packages/ad-migration-cli/src packages/ad-migration-internal/src mcp/ddl scripts tests/helpers.py tests/conftest.py --select F401,F841 --statistics`
- Expected baseline: `72 F401 unused-import`

Do not remove:

- `packages/ad-migration-cli/src/ad_migration_cli/main.py` import of `app`.
- `packages/ad-migration-internal/src/ad_migration_internal/entrypoints.py` console-script imports.
- Pydantic model fields/classes only because Vulture reports them unused.
- Typer command functions only because Vulture reports them unused.
- Diagnostic functions decorated with `@diagnostic`.
- `output_models` classes used by skill contracts or lazy exports.
- Pytest fixture parameters used only for side effects.

---

### Task 1: Production Unused Imports

**Files:**

- Modify: `lib/shared/batch_plan.py`
- Modify: `lib/shared/catalog_dmf.py`
- Modify: `lib/shared/cli/setup_sandbox_cmd.py`
- Modify: `lib/shared/dbops/sql_server.py`
- Modify: `lib/shared/diagnostics/common.py`
- Modify: `lib/shared/diagnostics/sqlserver.py`
- Modify: `lib/shared/discover.py`
- Modify: `lib/shared/dry_run.py`
- Modify: `lib/shared/dry_run_core.py`
- Modify: `lib/shared/freetds.py`
- Modify: `lib/shared/init.py`
- Modify: `lib/shared/loader_io.py`
- Modify: `lib/shared/migrate.py`
- Modify: `lib/shared/refactor.py`
- Modify: `lib/shared/sandbox/oracle_services.py`
- Modify: `lib/shared/sandbox/sql_server_services.py`

- [ ] **Step 1: Confirm the failing static check**

Run:

```bash
uvx ruff check lib/shared --select F401,F841 --statistics
```

Expected: Ruff reports production unused imports.

- [ ] **Step 2: Remove only production unused imports**

Run:

```bash
uvx ruff check lib/shared --select F401 --fix
```

Then inspect the diff:

```bash
git diff -- lib/shared
```

Keep any import that is intentionally re-exported for external compatibility. In particular, preserve `derive_materialization`, `derive_schema_tests`, and `_load_refactored_sql` imports in `lib/shared/migrate.py` because tests and compatibility callers import them through `shared.migrate`.

- [ ] **Step 3: Verify production static cleanup**

Run:

```bash
uvx ruff check lib/shared --select F401,F841 --statistics
```

Expected: no `F401` or `F841` findings under `lib/shared`.

- [ ] **Step 4: Run focused unit tests**

Run:

```bash
cd lib && uv run pytest \
  ../tests/unit/dry_run \
  ../tests/unit/test_harness \
  ../tests/unit/migrate \
  ../tests/unit/init \
  ../tests/unit/diagnostics \
  ../tests/unit/cli
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add lib/shared
git commit -m "chore: remove stale production python imports"
```

---

### Task 2: Script And Top-Level Test Harness Imports

**Files:**

- Modify: `scripts/demo-warehouse/scripts/csv_to_inserts.py`
- Modify: `tests/conftest.py`

- [ ] **Step 1: Confirm the failing static check**

Run:

```bash
uvx ruff check scripts tests/conftest.py tests/helpers.py --select F401,F841 --statistics
```

Expected: `csv_to_inserts.py` and `tests/conftest.py` unused-import findings.

- [ ] **Step 2: Remove only the reported unused imports**

Run:

```bash
uvx ruff check scripts tests/conftest.py tests/helpers.py --select F401 --fix
```

Then inspect the diff:

```bash
git diff -- scripts tests/conftest.py tests/helpers.py
```

Expected removals:

- `hashlib` and `sys` from `scripts/demo-warehouse/scripts/csv_to_inserts.py`
- `pytest` from `tests/conftest.py`

- [ ] **Step 3: Verify script/static cleanup**

Run:

```bash
uvx ruff check scripts tests/conftest.py tests/helpers.py --select F401,F841 --statistics
```

Expected: no findings in these paths.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd lib && uv run pytest ../tests/unit/fixtures ../tests/unit/test_helpers.py
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

Run:

```bash
git add scripts/demo-warehouse/scripts/csv_to_inserts.py tests/conftest.py
git commit -m "chore: remove stale script python imports"
```

---

### Task 3: Unit And Integration Test Dead-Code Hygiene

**Files:**

- Modify: `tests/`
- Modify: `mcp/ddl/tests/`

- [ ] **Step 1: Confirm the failing static check**

Run:

```bash
uvx ruff check tests mcp/ddl/tests --select F401,F841 --statistics
```

Expected: many `F401` unused imports and a small number of `F841` unused local variables.

- [ ] **Step 2: Remove safe unused test imports**

Run:

```bash
uvx ruff check tests mcp/ddl/tests --select F401 --fix
```

Then inspect the diff:

```bash
git diff -- tests mcp/ddl/tests
```

Do not edit function parameters that are pytest fixtures, even if a dead-code tool says the parameter is unused.

- [ ] **Step 3: Fix unused local variables manually**

Run:

```bash
uvx ruff check tests mcp/ddl/tests --select F841 --output-format concise
```

For each `F841` local, remove the assignment when the value is not asserted later. Preserve any expression whose execution is the test assertion, and replace `result = call()` with `call()` when the return value is intentionally unused.

- [ ] **Step 4: Verify test static cleanup**

Run:

```bash
uvx ruff check tests mcp/ddl/tests --select F401,F841 --statistics
```

Expected: no `F401` or `F841` findings under tests.

- [ ] **Step 5: Run unit and MCP tests**

Run:

```bash
cd lib && uv run pytest
cd ../mcp/ddl && uv run pytest
```

Expected: both suites pass.

- [ ] **Step 6: Commit**

Run:

```bash
git add tests mcp/ddl/tests
git commit -m "chore: remove stale python test imports"
```

---

### Task 4: Final Audit Verification

**Files:**

- No planned source edits.

- [ ] **Step 1: Run full static check for cleaned scope**

Run:

```bash
uvx ruff check lib/shared packages/ad-migration-cli/src packages/ad-migration-internal/src mcp/ddl scripts tests/helpers.py tests/conftest.py tests mcp/ddl/tests --select F401,F841 --statistics
```

Expected: no `F401` or `F841` findings. The console-script adapter imports in `packages/` may be flagged by Ruff if included; if so, add narrow `# noqa: F401` comments to only those adapter imports and rerun.

- [ ] **Step 2: Run Vulture high-confidence pass**

Run:

```bash
uvx vulture lib/shared packages/ad-migration-cli/src packages/ad-migration-internal/src mcp/ddl/server.py mcp/ddl/ddl_mcp_support tests/unit mcp/ddl/tests/unit tests/integration scripts tests/helpers.py tests/conftest.py --min-confidence 80
```

Expected: no high-confidence production findings. Accept documented false positives only for console-script adapter imports and pytest fixture parameters.

- [ ] **Step 3: Run full Python unit suites**

Run:

```bash
cd lib && uv run pytest
cd ../mcp/ddl && uv run pytest
```

Expected: both suites pass.

- [ ] **Step 4: Record final status**

Run:

```bash
git status --short
git log --oneline --max-count=5
```

Expected: clean worktree after commits, with task commits visible.
