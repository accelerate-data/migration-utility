# VU-1078: CLI Error Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace raw Python tracebacks in all `ad-migration` commands with clean one-line errors and actionable hints.

**Architecture:** A single `cli_error_handler(operation)` context manager in `shared/cli/error_handler.py` catches driver-level and IO exceptions (`pyodbc.Error`, `oracledb.DatabaseError`, `OSError`, `ConnectionError`, `ValueError`), maps each to a user-facing message + hint, and raises `typer.Exit` with the correct code. Commands replace their existing narrow `except (OSError, ConnectionError)` blocks with the context manager.

**Tech Stack:** Python 3.11, Typer, pyodbc, oracledb, pytest, typer.testing.CliRunner

---

## File map

| File | Action |
|---|---|
| `lib/shared/cli/error_handler.py` | **Create** — context manager + `_classify` helper |
| `tests/unit/cli/test_error_handler.py` | **Create** — one test per exception mapping + integration tests |
| `lib/shared/cli/setup_source_cmd.py` | **Modify** — wrap `run_list_schemas` and `run_extract` |
| `lib/shared/cli/setup_sandbox_cmd.py` | **Modify** — wrap `backend.sandbox_up` |
| `lib/shared/cli/teardown_sandbox_cmd.py` | **Modify** — wrap `backend.sandbox_down` |
| `lib/shared/cli/reset_cmd.py` | **Modify** — wrap `_create_backend` + `backend.sandbox_down` in `_teardown_sandbox_if_configured` |
| `lib/shared/cli/setup_target_cmd.py` | **Modify** — wrap `run_setup_target` |
| `lib/shared/cli/exclude_table_cmd.py` | **Modify** — wrap `run_exclude` |
| `lib/shared/cli/add_source_table_cmd.py` | **Modify** — wrap `run_write_source` |

---

## Task 1: Create `error_handler.py` — write failing tests first

**Files:**
- Create: `lib/shared/cli/error_handler.py` (stub only — enough for imports)
- Create: `tests/unit/cli/test_error_handler.py`

- [ ] **Step 1: Create a minimal stub so imports don't fail**

Create `lib/shared/cli/error_handler.py`:

```python
"""cli_error_handler — clean error surface for ad-migration commands."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import typer

from shared.cli.output import console, error

logger = logging.getLogger(__name__)

# Guard driver imports — may not be installed in every environment.
try:
    import pyodbc as _pyodbc
    _PYODBC_INTERFACE_ERROR: type | None = _pyodbc.InterfaceError
    _PYODBC_PROGRAMMING_ERROR: type | None = _pyodbc.ProgrammingError
    _PYODBC_OPERATIONAL_ERROR: type | None = _pyodbc.OperationalError
    _PYODBC_ERROR: type | None = _pyodbc.Error
except ImportError:
    _PYODBC_INTERFACE_ERROR = None
    _PYODBC_PROGRAMMING_ERROR = None
    _PYODBC_OPERATIONAL_ERROR = None
    _PYODBC_ERROR = None

try:
    import oracledb as _oracledb
    _ORACLE_DATABASE_ERROR: type | None = _oracledb.DatabaseError
except ImportError:
    _ORACLE_DATABASE_ERROR = None


def _classify(exc: BaseException) -> tuple[int, str, str | None]:
    raise NotImplementedError


@contextmanager
def cli_error_handler(operation: str) -> Iterator[None]:
    raise NotImplementedError
    yield  # noqa: unreachable — makes this a generator
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/cli/test_error_handler.py`:

```python
"""Tests for cli_error_handler — one per exception type."""
from __future__ import annotations

from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

import shared.cli.error_handler as _mod
from shared.cli.error_handler import _classify, cli_error_handler

runner = CliRunner()


def _app_raising(exc: Exception) -> typer.Typer:
    """Build a one-command Typer app that raises exc inside cli_error_handler."""
    app = typer.Typer()

    @app.command()
    def cmd() -> None:
        with cli_error_handler("test operation"):
            raise exc

    return app


# ── _classify unit tests ─────────────────────────────────────────────────────

class _FakePyodbcBase(Exception): pass
class _FakePyodbcInterface(_FakePyodbcBase): pass
class _FakePyodbcProgramming(_FakePyodbcBase): pass
class _FakePyodbcOperational(_FakePyodbcBase): pass
class _FakeOracleDB(Exception): pass


def _patch_drivers():
    return patch.multiple(
        _mod,
        _PYODBC_INTERFACE_ERROR=_FakePyodbcInterface,
        _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
        _PYODBC_OPERATIONAL_ERROR=_FakePyodbcOperational,
        _PYODBC_ERROR=_FakePyodbcBase,
        _ORACLE_DATABASE_ERROR=_FakeOracleDB,
    )


def test_classify_pyodbc_interface_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcInterface("driver missing"))
    assert code == 2
    assert "ODBC Driver" in hint


def test_classify_pyodbc_programming_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcProgramming("bad db"))
    assert code == 2
    assert "MSSQL_DB" in hint


def test_classify_pyodbc_operational_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcOperational("conn refused"))
    assert code == 2
    assert "MSSQL_HOST" in hint


def test_classify_pyodbc_base_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakePyodbcBase("generic"))
    assert code == 2
    assert "SQL Server" in hint


def test_classify_oracle_database_error():
    with _patch_drivers():
        code, msg, hint = _classify(_FakeOracleDB("ora error"))
    assert code == 2
    assert "ORACLE_HOST" in hint


def test_classify_os_error():
    code, msg, hint = _classify(OSError("no such file"))
    assert code == 2
    assert "file path" in hint


def test_classify_connection_error():
    code, msg, hint = _classify(ConnectionError("refused"))
    assert code == 2
    assert "port" in hint


def test_classify_value_error():
    code, msg, hint = _classify(ValueError("bad input"))
    assert code == 1
    assert hint is None


def test_classify_unknown_exception():
    code, msg, hint = _classify(RuntimeError("oops"))
    assert code == 1
    assert hint is None


# ── cli_error_handler integration tests ─────────────────────────────────────

def test_handler_shows_operation_in_output():
    with _patch_drivers():
        app = _app_raising(_FakePyodbcProgramming("db error"))
        result = runner.invoke(app, [])
    assert "test operation" in result.output


def test_handler_shows_hint_for_connection_errors():
    with _patch_drivers():
        app = _app_raising(_FakePyodbcProgramming("db error"))
        result = runner.invoke(app, [])
    assert "Hint:" in result.output
    assert result.exit_code == 2


def test_handler_no_hint_for_value_error():
    app = _app_raising(ValueError("bad value"))
    result = runner.invoke(app, [])
    assert "Hint:" not in result.output
    assert result.exit_code == 1


def test_handler_exit_1_for_unknown_exception():
    app = _app_raising(RuntimeError("surprise"))
    result = runner.invoke(app, [])
    assert result.exit_code == 1
    assert "Unexpected error" in result.output


def test_handler_does_not_swallow_typer_exit():
    app = typer.Typer()

    @app.command()
    def cmd() -> None:
        with cli_error_handler("test"):
            raise typer.Exit(code=0)

    result = runner.invoke(app, [])
    assert result.exit_code == 0
```

- [ ] **Step 3: Run tests to confirm they all fail**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_error_handler.py -q
```

Expected: all tests fail with `NotImplementedError`.

---

## Task 2: Implement `_classify` and `cli_error_handler`

**Files:**
- Modify: `lib/shared/cli/error_handler.py`

- [ ] **Step 1: Replace the stubs with the full implementation**

Replace the entire contents of `lib/shared/cli/error_handler.py`:

```python
"""cli_error_handler — clean error surface for ad-migration commands."""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import typer

from shared.cli.output import console, error

logger = logging.getLogger(__name__)

# Guard driver imports — may not be installed in every environment.
try:
    import pyodbc as _pyodbc
    _PYODBC_INTERFACE_ERROR: type | None = _pyodbc.InterfaceError
    _PYODBC_PROGRAMMING_ERROR: type | None = _pyodbc.ProgrammingError
    _PYODBC_OPERATIONAL_ERROR: type | None = _pyodbc.OperationalError
    _PYODBC_ERROR: type | None = _pyodbc.Error
except ImportError:
    _PYODBC_INTERFACE_ERROR = None
    _PYODBC_PROGRAMMING_ERROR = None
    _PYODBC_OPERATIONAL_ERROR = None
    _PYODBC_ERROR = None

try:
    import oracledb as _oracledb
    _ORACLE_DATABASE_ERROR: type | None = _oracledb.DatabaseError
except ImportError:
    _ORACLE_DATABASE_ERROR = None


def _classify(exc: BaseException) -> tuple[int, str, str | None]:
    """Return (exit_code, message, hint_or_None) for a caught exception.

    pyodbc subtypes must be checked before the base pyodbc.Error class.
    """
    if _PYODBC_INTERFACE_ERROR and isinstance(exc, _PYODBC_INTERFACE_ERROR):
        return 2, str(exc), (
            "ODBC Driver 18 for SQL Server may not be installed — run: brew install msodbcsql18"
        )
    if _PYODBC_PROGRAMMING_ERROR and isinstance(exc, _PYODBC_PROGRAMMING_ERROR):
        return 2, str(exc), (
            "Verify MSSQL_DB is set to a valid database name and SA_PASSWORD grants access"
        )
    if _PYODBC_OPERATIONAL_ERROR and isinstance(exc, _PYODBC_OPERATIONAL_ERROR):
        return 2, str(exc), "Check MSSQL_HOST, MSSQL_PORT, and network connectivity"
    if _PYODBC_ERROR and isinstance(exc, _PYODBC_ERROR):
        return 2, str(exc), "Check SQL Server connection environment variables"
    if _ORACLE_DATABASE_ERROR and isinstance(exc, _ORACLE_DATABASE_ERROR):
        return 2, str(exc), (
            "Check Oracle connection environment variables "
            "(ORACLE_HOST, ORACLE_PORT, ORACLE_USER, ORACLE_PASSWORD)"
        )
    if isinstance(exc, ConnectionError):
        return 2, str(exc), "Check host, port, and network access"
    if isinstance(exc, OSError):
        return 2, str(exc), "Check network connectivity or file path permissions"
    if isinstance(exc, ValueError):
        return 1, str(exc), None
    return 1, str(exc), None


@contextmanager
def cli_error_handler(operation: str) -> Iterator[None]:
    """Catch known exceptions and surface a clean error + hint instead of a traceback.

    Re-raises typer.Exit, typer.Abort, KeyboardInterrupt, and SystemExit unchanged
    so normal control flow is never interrupted.
    """
    try:
        yield
    except (typer.Exit, typer.Abort, KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        exit_code, message, hint = _classify(exc)
        prefix = "Unexpected error" if exit_code == 1 and not isinstance(exc, ValueError) else "Error"
        error(f"{prefix} while {operation}: {message}")
        if hint:
            console.print(f"  Hint: {hint}")
        logger.error(
            "event=cli_error component=error_handler operation=%s error_type=%s",
            operation,
            type(exc).__name__,
        )
        raise typer.Exit(code=exit_code)
```

- [ ] **Step 2: Run tests to confirm they all pass**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_error_handler.py -q
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add lib/shared/cli/error_handler.py tests/unit/cli/test_error_handler.py
git commit -m "feat: add cli_error_handler context manager for clean CLI error surface (VU-1078)"
```

---

## Task 3: Apply handler in `setup_source_cmd.py`

**Files:**
- Modify: `lib/shared/cli/setup_source_cmd.py`
- Test: `tests/unit/cli/test_setup_source_cmd.py`

- [ ] **Step 1: Write a failing test for pyodbc error in setup-source**

Add to `tests/unit/cli/test_setup_source_cmd.py` (at the bottom, before the last function):

```python
def test_setup_source_shows_clean_error_on_db_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "BadDB")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    import shared.cli.error_handler as _mod
    class _FakePyodbcProgramming(Exception): pass

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract",
              side_effect=_FakePyodbcProgramming("Cannot open database")),
        patch.object(_mod, "_PYODBC_PROGRAMMING_ERROR", _FakePyodbcProgramming),
        patch.object(_mod, "_PYODBC_INTERFACE_ERROR", None),
        patch.object(_mod, "_PYODBC_OPERATIONAL_ERROR", None),
        patch.object(_mod, "_PYODBC_ERROR", _FakePyodbcProgramming),
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--schemas", "silver",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 2
    assert "Hint:" in result.output
    assert "MSSQL_DB" in result.output
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_setup_source_cmd.py::test_setup_source_shows_clean_error_on_db_failure -q
```

Expected: FAIL — exit code is 0 or 1, not 2 (traceback currently escapes).

- [ ] **Step 3: Apply `cli_error_handler` in `setup_source_cmd.py`**

Add the import after the existing imports at the top of `lib/shared/cli/setup_source_cmd.py`:

```python
from shared.cli.error_handler import cli_error_handler
```

Replace the `run_list_schemas` call block (the `if all_schemas:` block, around line 62):

```python
    if all_schemas:
        with cli_error_handler("discovering schemas in database"):
            discovered = run_list_schemas(root, database)
        schema_list = [s["schema"] for s in discovered.get("schemas", [])]
        if not schema_list:
            error("No schemas found in the database. Verify the connection and database name.")
            raise typer.Exit(code=1)
        console.print(f"Discovered schemas: [bold]{', '.join(schema_list)}[/bold]")
        if not yes:
            confirmed = typer.confirm(
                f"Extract all {len(schema_list)} schemas? This will overwrite existing DDL and catalog files.",
                default=False,
            )
            if not confirmed:
                console.print("Aborted.")
                return
    else:
        schema_list = [s.strip() for s in (schemas or "").split(",") if s.strip()]
```

Replace the `run_extract` block (around line 82):

```python
    console.print(f"Extracting DDL from schemas: [bold]{', '.join(schema_list)}[/bold]")
    with console.status("Extracting..."):
        with cli_error_handler("extracting DDL from source database"):
            result = run_extract(root, database, schema_list)
```

(Remove the old `try/except (OSError, ConnectionError)` block that was wrapping `run_extract` — `cli_error_handler` covers those exception types.)

- [ ] **Step 4: Run all setup-source tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_setup_source_cmd.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add lib/shared/cli/setup_source_cmd.py tests/unit/cli/test_setup_source_cmd.py
git commit -m "fix: wrap setup-source DB calls with cli_error_handler (VU-1078)"
```

---

## Task 4: Apply handler in `setup_sandbox_cmd.py` and `teardown_sandbox_cmd.py`

**Files:**
- Modify: `lib/shared/cli/setup_sandbox_cmd.py`
- Modify: `lib/shared/cli/teardown_sandbox_cmd.py`
- Test: `tests/unit/cli/test_sandbox_cmds.py`

- [ ] **Step 1: Write failing tests for both commands**

Add to `tests/unit/cli/test_sandbox_cmds.py` (at the bottom):

```python
import shared.cli.error_handler as _err_mod


def _patch_pyodbc_programming():
    class _FakePyodbcProgramming(Exception): pass
    return _FakePyodbcProgramming, patch.multiple(
        _err_mod,
        _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
        _PYODBC_INTERFACE_ERROR=None,
        _PYODBC_OPERATIONAL_ERROR=None,
        _PYODBC_ERROR=_FakePyodbcProgramming,
    )


def test_setup_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_up.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output


def test_teardown_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_down.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_sandbox_cmds.py::test_setup_sandbox_shows_clean_error_on_db_failure ../tests/unit/cli/test_sandbox_cmds.py::test_teardown_sandbox_shows_clean_error_on_db_failure -q
```

Expected: FAIL.

- [ ] **Step 3: Apply handler in `setup_sandbox_cmd.py`**

Add import at the top of `lib/shared/cli/setup_sandbox_cmd.py`:

```python
from shared.cli.error_handler import cli_error_handler
```

Replace the `backend.sandbox_up` block:

```python
    console.print(f"Provisioning sandbox for schemas: [bold]{', '.join(schemas)}[/bold]...")
    with console.status("Running sandbox_up..."):
        with cli_error_handler("provisioning sandbox database"):
            result = backend.sandbox_up(schemas=schemas)
```

(Remove the old `try/except (OSError, ConnectionError)` block.)

- [ ] **Step 4: Apply handler in `teardown_sandbox_cmd.py`**

Add import at the top of `lib/shared/cli/teardown_sandbox_cmd.py`:

```python
from shared.cli.error_handler import cli_error_handler
```

Replace the `backend.sandbox_down` block:

```python
    console.print(f"Tearing down sandbox database: [bold]{sandbox_db}[/bold]...")
    with console.status("Running sandbox_down..."):
        with cli_error_handler("tearing down sandbox database"):
            result = backend.sandbox_down(sandbox_db)
```

(Remove the old `try/except (OSError, ConnectionError)` block.)

- [ ] **Step 5: Run all sandbox tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_sandbox_cmds.py -q
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add lib/shared/cli/setup_sandbox_cmd.py lib/shared/cli/teardown_sandbox_cmd.py tests/unit/cli/test_sandbox_cmds.py
git commit -m "fix: wrap sandbox DB calls with cli_error_handler (VU-1078)"
```

---

## Task 5: Apply handler in `reset_cmd.py`

**Files:**
- Modify: `lib/shared/cli/reset_cmd.py`
- Test: `tests/unit/cli/test_pipeline_cmds.py`

- [ ] **Step 1: Write a failing test**

Add to `tests/unit/cli/test_pipeline_cmds.py` (at the end of the `# ── reset all` section):

```python
def test_reset_all_sandbox_db_error_warns_and_continues(tmp_path):
    """A pyodbc error from sandbox_down is treated as a teardown failure — warn, continue."""
    import shared.cli.error_handler as _err_mod
    class _FakePyodbcProgramming(Exception): pass

    mock_backend = MagicMock()
    mock_backend.sandbox_down.side_effect = _FakePyodbcProgramming("login failed")

    with (
        patch.multiple(
            _err_mod,
            _PYODBC_PROGRAMMING_ERROR=_FakePyodbcProgramming,
            _PYODBC_INTERFACE_ERROR=None,
            _PYODBC_OPERATIONAL_ERROR=None,
            _PYODBC_ERROR=_FakePyodbcProgramming,
        ),
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [])
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_pipeline_cmds.py::test_reset_all_sandbox_db_error_warns_and_continues -q
```

Expected: FAIL — pyodbc exception propagates as traceback.

- [ ] **Step 3: Apply handler in `reset_cmd.py`**

Add import at the top of `lib/shared/cli/reset_cmd.py`:

```python
from shared.cli.error_handler import cli_error_handler
```

In `_teardown_sandbox_if_configured`, replace the existing `try/except (OSError, ConnectionError)` block around `backend.sandbox_down`:

```python
    teardown_ok = False
    try:
        backend = _create_backend(manifest)
        with cli_error_handler("tearing down sandbox database"):
            result = backend.sandbox_down(sandbox_db)
        teardown_ok = result.status == "ok"
        if not teardown_ok:
            logger.warning(
                "event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s status=%s",
                sandbox_db, result.status,
            )
    except typer.Exit:
        # cli_error_handler raised Exit — treat as teardown failure and continue
        pass
    except Exception as exc:
        logger.warning(
            "event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s error=%s",
            sandbox_db, exc,
        )
```

Note: `cli_error_handler` raises `typer.Exit` on failure. Inside `_teardown_sandbox_if_configured` we catch `typer.Exit` and treat it as a teardown failure, then continue. This preserves the warn-and-continue behaviour from VU-1075.

- [ ] **Step 4: Run all pipeline tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_pipeline_cmds.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add lib/shared/cli/reset_cmd.py tests/unit/cli/test_pipeline_cmds.py
git commit -m "fix: wrap reset sandbox calls with cli_error_handler (VU-1078)"
```

---

## Task 6: Apply handler in remaining commands

**Files:**
- Modify: `lib/shared/cli/setup_target_cmd.py`
- Modify: `lib/shared/cli/exclude_table_cmd.py`
- Modify: `lib/shared/cli/add_source_table_cmd.py`

These commands don't make direct DB connections so no new tests are required — the existing test suites are sufficient to confirm no regressions.

- [ ] **Step 1: Apply handler in `setup_target_cmd.py`**

Add import:

```python
from shared.cli.error_handler import cli_error_handler
```

Replace the `run_setup_target` block:

```python
    console.print("Running target setup...")
    with console.status("Scaffolding dbt project and generating sources.yml..."):
        with cli_error_handler("running target setup"):
            try:
                result = run_setup_target(root)
            except ValueError as exc:
                error(str(exc))
                raise typer.Exit(code=1) from exc
```

- [ ] **Step 2: Apply handler in `exclude_table_cmd.py`**

Add import:

```python
from shared.cli.error_handler import cli_error_handler
```

Wrap the `run_exclude` call:

```python
    with cli_error_handler("excluding tables from catalog"):
        result = run_exclude(root, list(fqns))
```

- [ ] **Step 3: Apply handler in `add_source_table_cmd.py`**

Add import:

```python
from shared.cli.error_handler import cli_error_handler
```

Wrap `run_write_source` inside the per-FQN loop:

```python
        try:
            with cli_error_handler(f"marking {fqn} as source table"):
                write_result = run_write_source(root, fqn, value=True)
            success(f"source   {fqn} → is_source: true")
            ...
        except typer.Exit:
            raise
        except CatalogFileMissingError:
            warn(f"missing  {fqn} (no catalog file — run setup-source first)")
        except ValueError as exc:
            warn(f"skipped  {fqn} — {exc}")
```

- [ ] **Step 4: Run the full CLI test suite**

```bash
cd lib && uv run pytest ../tests/unit/cli/ -q
```

Expected: all pass.

- [ ] **Step 5: Commit and push**

```bash
git add lib/shared/cli/setup_target_cmd.py lib/shared/cli/exclude_table_cmd.py lib/shared/cli/add_source_table_cmd.py
git commit -m "fix: apply cli_error_handler to remaining commands (VU-1078)"
git push
```

---

## Self-review

**Spec coverage:**
- ✅ `error_handler.py` with `cli_error_handler` — Task 1–2
- ✅ All exception types mapped — Task 2
- ✅ `setup_source_cmd` wrapped — Task 3
- ✅ `setup_sandbox_cmd` + `teardown_sandbox_cmd` wrapped — Task 4
- ✅ `reset_cmd` wrapped — Task 5
- ✅ `setup_target_cmd`, `exclude_table_cmd`, `add_source_table_cmd` wrapped — Task 6
- ✅ Unit tests for each exception mapping — Task 1–2
- ✅ Exit codes preserved (2 for IO/connection, 1 for domain/unknown) — `_classify`

**Placeholder scan:** None found.

**Type consistency:** `_classify` returns `tuple[int, str, str | None]` — used consistently in Task 2 and referenced by tests in Task 1. `cli_error_handler(operation: str)` signature matches all call sites.
