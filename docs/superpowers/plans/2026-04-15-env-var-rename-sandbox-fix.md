# Env Var Rename + Sandbox Cold-Start Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename all connection env vars to `<ROLE>_<TECH>_<FIELD>` convention, remove Fabric/Snowflake/DuckDB from setup-target, and fix the sandbox cold-start gap so `setup-sandbox` reads env vars and writes `runtime.sandbox.connection` to `manifest.json` before connecting.

**Architecture:** `env_check.py` is the single source of truth for required env var definitions — each role (`source`, `sandbox`, `target`) gets its own `_VARS` dict keyed by technology. `setup-sandbox` gains the same validate → write → connect pattern that `setup-source` already follows. Old env var names are deleted everywhere — no aliases.

**Tech Stack:** Python, Typer, Pydantic (`RuntimeConnection`/`RuntimeRole`), pytest/monkeypatch

---

## File Map

| File | Change |
|---|---|
| `lib/shared/cli/env_check.py` | Rename `_SOURCE_VARS`; replace `_TARGET_VARS` with sql_server+oracle; add `_SANDBOX_VARS` + `require_sandbox_vars()` |
| `lib/shared/setup_ddl_support/manifest.py` | `get_connection_identity()` reads `SOURCE_MSSQL_*` / `SOURCE_ORACLE_*` |
| `lib/shared/target_setup.py` | `_TARGET_ENV_MAPS` → sql_server + oracle; remove Fabric/Snowflake/DuckDB |
| `lib/shared/cli/setup_target_cmd.py` | Help text updated to `sql_server or oracle` |
| `lib/shared/cli/setup_sandbox_cmd.py` | Add `_get_sandbox_technology()`, `require_sandbox_vars()` call, `_write_sandbox_connection_to_manifest()` |
| `lib/shared/init_templates.py` | `.envrc` template + pre-commit hook updated with new var names |
| `commands/init-ad-migration.md` | Env var tables updated for all three roles |
| `docs/wiki/CLI-Reference.md` | Full env var reference updated |
| `tests/unit/cli/test_env_check.py` | Rewritten for new names; sandbox tests added; target updated |
| `tests/unit/cli/test_sandbox_cmds.py` | New patches for `_get_sandbox_technology`, `require_sandbox_vars`, `_write_sandbox_connection_to_manifest`; existing tests updated |
| `tests/unit/cli/test_setup_source_cmd.py` | Env var patches updated to `SOURCE_MSSQL_*` |
| `tests/unit/cli/test_setup_target_cmd.py` | Rewritten for sql_server + oracle; snowflake test removed |

---

### Task 1: Rename env vars in `env_check.py` — new var definitions + `require_sandbox_vars()`

**Files:**

- Modify: `lib/shared/cli/env_check.py`
- Test: `tests/unit/cli/test_env_check.py`

- [ ] **Step 1: Write failing tests**

Replace `tests/unit/cli/test_env_check.py` entirely:

```python
import pytest
from shared.cli.env_check import require_source_vars, require_sandbox_vars, require_target_vars


def test_require_source_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SOURCE_MSSQL_PORT", "1433")
    monkeypatch.setenv("SOURCE_MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "sa")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "secret")
    require_source_vars("sql_server")  # must not raise or exit


def test_require_source_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PORT", "SOURCE_MSSQL_DB",
                "SOURCE_MSSQL_USER", "SOURCE_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("sql_server")
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "SOURCE_MSSQL_HOST" in err
    assert "SOURCE_MSSQL_PASSWORD" in err


def test_require_source_vars_oracle_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SOURCE_ORACLE_HOST", "localhost")
    monkeypatch.setenv("SOURCE_ORACLE_PORT", "1521")
    monkeypatch.setenv("SOURCE_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("SOURCE_ORACLE_USER", "sh")
    monkeypatch.setenv("SOURCE_ORACLE_PASSWORD", "secret")
    require_source_vars("oracle")  # must not raise or exit


def test_require_source_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("SOURCE_ORACLE_HOST", "SOURCE_ORACLE_PORT", "SOURCE_ORACLE_SERVICE",
                "SOURCE_ORACLE_USER", "SOURCE_ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("oracle")
    assert exc_info.value.code == 1
    assert "SOURCE_ORACLE_HOST" in capsys.readouterr().err


def test_require_sandbox_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1433")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sa")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "secret")
    require_sandbox_vars("sql_server")  # must not raise or exit


def test_require_sandbox_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("SANDBOX_MSSQL_HOST", "SANDBOX_MSSQL_PORT",
                "SANDBOX_MSSQL_USER", "SANDBOX_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_sandbox_vars("sql_server")
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "SANDBOX_MSSQL_HOST" in err
    assert "SANDBOX_MSSQL_PASSWORD" in err


def test_require_sandbox_vars_oracle_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "localhost")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1521")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "admin")
    monkeypatch.setenv("SANDBOX_ORACLE_PASSWORD", "secret")
    require_sandbox_vars("oracle")  # must not raise or exit


def test_require_sandbox_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("SANDBOX_ORACLE_HOST", "SANDBOX_ORACLE_PORT", "SANDBOX_ORACLE_SERVICE",
                "SANDBOX_ORACLE_USER", "SANDBOX_ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_sandbox_vars("oracle")
    assert exc_info.value.code == 1
    assert "SANDBOX_ORACLE_HOST" in capsys.readouterr().err


def test_require_target_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("TARGET_MSSQL_HOST", "localhost")
    monkeypatch.setenv("TARGET_MSSQL_PORT", "1433")
    monkeypatch.setenv("TARGET_MSSQL_DB", "target_db")
    monkeypatch.setenv("TARGET_MSSQL_USER", "sa")
    monkeypatch.setenv("TARGET_MSSQL_PASSWORD", "secret")
    require_target_vars("sql_server")  # must not raise or exit


def test_require_target_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("TARGET_MSSQL_HOST", "TARGET_MSSQL_PORT", "TARGET_MSSQL_DB",
                "TARGET_MSSQL_USER", "TARGET_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("sql_server")
    assert exc_info.value.code == 1
    assert "TARGET_MSSQL_HOST" in capsys.readouterr().err


def test_require_target_vars_oracle_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("TARGET_ORACLE_HOST", "localhost")
    monkeypatch.setenv("TARGET_ORACLE_PORT", "1521")
    monkeypatch.setenv("TARGET_ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("TARGET_ORACLE_USER", "target_user")
    monkeypatch.setenv("TARGET_ORACLE_PASSWORD", "secret")
    require_target_vars("oracle")  # must not raise or exit


def test_require_target_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("TARGET_ORACLE_HOST", "TARGET_ORACLE_PORT", "TARGET_ORACLE_SERVICE",
                "TARGET_ORACLE_USER", "TARGET_ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("oracle")
    assert exc_info.value.code == 1
    assert "TARGET_ORACLE_HOST" in capsys.readouterr().err


def test_require_source_vars_unknown_technology_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("unknown_db")
    assert exc_info.value.code == 1
    assert "unknown_db" in capsys.readouterr().err


def test_require_sandbox_vars_unknown_technology_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        require_sandbox_vars("unknown_db")
    assert exc_info.value.code == 1
    assert "unknown_db" in capsys.readouterr().err


def test_require_target_vars_unknown_technology_exits(capsys):
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("unknown_platform")
    assert exc_info.value.code == 1
    assert "unknown_platform" in capsys.readouterr().err
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/cli/test_env_check.py -v 2>&1 | tail -20
```

Expected: FAIL — `ImportError: cannot import name 'require_sandbox_vars'` and failures on old var names.

- [ ] **Step 3: Implement new `env_check.py`**

Replace `lib/shared/cli/env_check.py` entirely:

```python
"""Env var validation for ad-migration CLI commands.

Validates required env vars before a command runs.
Exits 1 with a clear message listing every missing var.
"""
from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

_SOURCE_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "SOURCE_MSSQL_HOST": "SQL Server hostname",
        "SOURCE_MSSQL_PORT": "SQL Server port",
        "SOURCE_MSSQL_DB": "SQL Server database name",
        "SOURCE_MSSQL_USER": "SQL Server username",
        "SOURCE_MSSQL_PASSWORD": "SQL Server password",
    },
    "oracle": {
        "SOURCE_ORACLE_HOST": "Oracle hostname",
        "SOURCE_ORACLE_PORT": "Oracle port",
        "SOURCE_ORACLE_SERVICE": "Oracle service name",
        "SOURCE_ORACLE_USER": "Oracle username",
        "SOURCE_ORACLE_PASSWORD": "Oracle password",
    },
}

_SANDBOX_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "SANDBOX_MSSQL_HOST": "Sandbox SQL Server hostname",
        "SANDBOX_MSSQL_PORT": "Sandbox SQL Server port",
        "SANDBOX_MSSQL_USER": "Sandbox SQL Server username",
        "SANDBOX_MSSQL_PASSWORD": "Sandbox SQL Server password",
    },
    "oracle": {
        "SANDBOX_ORACLE_HOST": "Sandbox Oracle hostname",
        "SANDBOX_ORACLE_PORT": "Sandbox Oracle port",
        "SANDBOX_ORACLE_SERVICE": "Sandbox Oracle service name",
        "SANDBOX_ORACLE_USER": "Sandbox Oracle admin username",
        "SANDBOX_ORACLE_PASSWORD": "Sandbox Oracle admin password",
    },
}

_TARGET_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "TARGET_MSSQL_HOST": "Target SQL Server hostname",
        "TARGET_MSSQL_PORT": "Target SQL Server port",
        "TARGET_MSSQL_DB": "Target SQL Server database name",
        "TARGET_MSSQL_USER": "Target SQL Server username",
        "TARGET_MSSQL_PASSWORD": "Target SQL Server password",
    },
    "oracle": {
        "TARGET_ORACLE_HOST": "Target Oracle hostname",
        "TARGET_ORACLE_PORT": "Target Oracle port",
        "TARGET_ORACLE_SERVICE": "Target Oracle service name",
        "TARGET_ORACLE_USER": "Target Oracle username",
        "TARGET_ORACLE_PASSWORD": "Target Oracle password",
    },
}


def require_source_vars(technology: str) -> None:
    """Validate source env vars. Exits 1 if any are missing or technology unknown."""
    if technology not in _SOURCE_VARS:
        print(
            f"Error: unknown source technology '{technology}'. Valid: {list(_SOURCE_VARS)}",
            file=sys.stderr,
        )
        logger.error(
            "event=env_check status=failure component=env_check technology=%s reason=unknown_technology",
            technology,
        )
        sys.exit(1)
    _check(_SOURCE_VARS[technology], technology, "setup-source")


def require_sandbox_vars(technology: str) -> None:
    """Validate sandbox env vars. Exits 1 if any are missing or technology unknown."""
    if technology not in _SANDBOX_VARS:
        print(
            f"Error: unknown sandbox technology '{technology}'. Valid: {list(_SANDBOX_VARS)}",
            file=sys.stderr,
        )
        logger.error(
            "event=env_check status=failure component=env_check technology=%s reason=unknown_technology",
            technology,
        )
        sys.exit(1)
    _check(_SANDBOX_VARS[technology], technology, "setup-sandbox")


def require_target_vars(technology: str) -> None:
    """Validate target env vars. Exits 1 if any are missing or technology unknown."""
    if technology not in _TARGET_VARS:
        print(
            f"Error: unknown target technology '{technology}'. Valid: {list(_TARGET_VARS)}",
            file=sys.stderr,
        )
        logger.error(
            "event=env_check status=failure component=env_check technology=%s reason=unknown_technology",
            technology,
        )
        sys.exit(1)
    _check(_TARGET_VARS[technology], technology, "setup-target")


def _check(required: dict[str, str], technology: str, command: str) -> None:
    missing = [var for var in required if not os.environ.get(var)]
    if not missing:
        logger.debug(
            "event=env_check status=success component=env_check technology=%s command=%s",
            technology,
            command,
        )
        return
    col = max(len(v) for v in missing) + 2
    lines = [f"Error: missing required environment variables for {technology}:\n"]
    for var in missing:
        lines.append(f"  {var:<{col}} not set")
    lines.append(f"\nSet these in your shell or .envrc before running {command}.")
    print("\n".join(lines), file=sys.stderr)
    logger.error(
        "event=env_check status=failure component=env_check technology=%s command=%s missing_count=%d",
        technology,
        command,
        len(missing),
    )
    sys.exit(1)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/cli/test_env_check.py -v 2>&1 | tail -10
```

Expected: 15 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/shared/cli/env_check.py tests/unit/cli/test_env_check.py
git commit -m "refactor: rename env vars to ROLE_TECH_FIELD convention in env_check.py"
```

---

### Task 2: Update `get_connection_identity()` and fix `test_setup_source_cmd.py`

**Files:**

- Modify: `lib/shared/setup_ddl_support/manifest.py`
- Test: `tests/unit/cli/test_setup_source_cmd.py`

- [ ] **Step 1: Update env var names in `test_setup_source_cmd.py`**

In `tests/unit/cli/test_setup_source_cmd.py`, replace every `monkeypatch.setenv` and `monkeypatch.delenv` that uses old names. The replacements are:

```
"MSSQL_HOST"  → "SOURCE_MSSQL_HOST"
"MSSQL_PORT"  → "SOURCE_MSSQL_PORT"
"MSSQL_DB"    → "SOURCE_MSSQL_DB"
"SA_PASSWORD" → "SOURCE_MSSQL_PASSWORD"
```

Also add `monkeypatch.setenv("SOURCE_MSSQL_USER", "sa")` wherever the other SQL Server vars are set (the new `_SOURCE_VARS` requires it).

For the missing-env test, also delete `SOURCE_MSSQL_USER`:

```python
def test_setup_source_fails_fast_on_missing_env(tmp_path, monkeypatch):
    for var in ("SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PORT", "SOURCE_MSSQL_DB",
                "SOURCE_MSSQL_USER", "SOURCE_MSSQL_PASSWORD"):
        monkeypatch.delenv(var, raising=False)

    result = runner.invoke(
        app,
        ["setup-source", "--technology", "sql_server", "--schemas", "silver",
         "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/cli/test_setup_source_cmd.py -v 2>&1 | tail -15
```

Expected: FAIL — `test_setup_source_fails_fast_on_missing_env` exits 0 instead of 1 because `require_source_vars` now looks for `SOURCE_MSSQL_*` (set by the test) while the monkeypatch calls haven't been updated yet. After the test file update, failures should be on old var names not being deleted.

- [ ] **Step 3: Update `get_connection_identity()` in `manifest.py`**

Find `get_connection_identity` in `lib/shared/setup_ddl_support/manifest.py` (around line 105) and replace the function body:

```python
def get_connection_identity(technology: str, database: str) -> dict[str, Any]:
    if technology == "sql_server":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                host=os.environ.get("SOURCE_MSSQL_HOST", "") or None,
                port=os.environ.get("SOURCE_MSSQL_PORT", "") or None,
                database=database or None,
                user=os.environ.get("SOURCE_MSSQL_USER", "") or None,
                password_env="SOURCE_MSSQL_PASSWORD",
                driver=os.environ.get("MSSQL_DRIVER", "FreeTDS") or None,
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    if technology == "oracle":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                dsn=os.environ.get("SOURCE_ORACLE_DSN", "") or None,
                host=os.environ.get("SOURCE_ORACLE_HOST", "") or None,
                port=os.environ.get("SOURCE_ORACLE_PORT", "") or None,
                service=os.environ.get("SOURCE_ORACLE_SERVICE", "") or None,
                user=os.environ.get("SOURCE_ORACLE_USER", "") or None,
                schema=database or os.environ.get("SOURCE_ORACLE_SCHEMA", "") or None,
                password_env="SOURCE_ORACLE_PASSWORD",
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    raise ValueError(
        f"Unknown technology: {technology}. Must be one of {sorted(TECH_DIALECT)}."
    )
```

Note: `MSSQL_DRIVER` stays unchanged — it is a machine-specific driver override written to `.env`, not a role credential.

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/cli/test_setup_source_cmd.py tests/unit/cli/test_env_check.py -v 2>&1 | tail -10
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/shared/setup_ddl_support/manifest.py tests/unit/cli/test_setup_source_cmd.py
git commit -m "refactor: rename source env vars to SOURCE_MSSQL_* / SOURCE_ORACLE_* in manifest.py"
```

---

### Task 3: Narrow `setup-target` to sql_server + oracle; rename `TARGET_*` vars

**Files:**

- Modify: `lib/shared/target_setup.py`
- Modify: `lib/shared/cli/setup_target_cmd.py`
- Test: `tests/unit/cli/test_setup_target_cmd.py`

- [ ] **Step 1: Write failing tests**

Replace `tests/unit/cli/test_setup_target_cmd.py` entirely:

```python
import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.target_setup import SetupTargetOutput

runner = CliRunner()

_SETUP_TARGET_OUT = SetupTargetOutput(
    files=["dbt/dbt_project.yml"],
    sources_path="dbt/models/staging/sources.yml",
    target_source_schema="bronze",
    created_tables=["silver.DimCustomer"],
    existing_tables=[],
    desired_tables=["silver.DimCustomer"],
)


def _write_manifest(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps({"schema_version": "1", "technology": "sql_server"}), encoding="utf-8"
    )


def test_setup_target_sql_server_writes_runtime_and_runs_orchestrator(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "sql_server", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "sql_server", "bronze")


def test_setup_target_oracle_writes_runtime_and_runs_orchestrator(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "oracle", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "oracle", "bronze")


def test_setup_target_exits_1_on_missing_manifest(tmp_path):
    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch(
            "shared.cli.setup_target_cmd.write_target_runtime_from_env",
            side_effect=ValueError("manifest.json not found"),
        ),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "sql_server", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 1


def test_setup_target_rejects_snowflake(tmp_path):
    """Snowflake has no backend — must be rejected."""
    result = runner.invoke(
        app,
        ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/cli/test_setup_target_cmd.py -v 2>&1 | tail -15
```

Expected: FAIL — `test_setup_target_sql_server_*` passes but `test_setup_target_rejects_snowflake` may pass already; `test_setup_target_oracle_*` fails because `require_target_vars("oracle")` currently exits (oracle not in old `_TARGET_VARS`).

- [ ] **Step 3: Update `_TARGET_ENV_MAPS` in `target_setup.py`**

Find `_TARGET_ENV_MAPS` in `lib/shared/target_setup.py` (lines 21-41) and replace:

```python
_TARGET_ENV_MAPS: dict[str, dict[str, str]] = {
    "sql_server": {
        "host": "TARGET_MSSQL_HOST",
        "port": "TARGET_MSSQL_PORT",
        "database": "TARGET_MSSQL_DB",
        "user": "TARGET_MSSQL_USER",
        "password_env": "TARGET_MSSQL_PASSWORD",
    },
    "oracle": {
        "host": "TARGET_ORACLE_HOST",
        "port": "TARGET_ORACLE_PORT",
        "service": "TARGET_ORACLE_SERVICE",
        "user": "TARGET_ORACLE_USER",
        "password_env": "TARGET_ORACLE_PASSWORD",
    },
}
```

- [ ] **Step 4: Update help text in `setup_target_cmd.py`**

In `lib/shared/cli/setup_target_cmd.py`, change the `--technology` option help string:

```python
technology: str = typer.Option(..., "--technology", help="Target technology: sql_server or oracle"),
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/unit/cli/test_setup_target_cmd.py -v 2>&1 | tail -10
```

Expected: all 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add lib/shared/target_setup.py lib/shared/cli/setup_target_cmd.py tests/unit/cli/test_setup_target_cmd.py
git commit -m "refactor: narrow setup-target to sql_server+oracle; rename TARGET_* env vars"
```

---

### Task 4: Fix sandbox cold-start in `setup_sandbox_cmd.py`

**Files:**

- Modify: `lib/shared/cli/setup_sandbox_cmd.py`
- Test: `tests/unit/cli/test_sandbox_cmds.py`

- [ ] **Step 1: Write failing tests**

Update `tests/unit/cli/test_sandbox_cmds.py`:

Replace `test_setup_sandbox_runs_sandbox_up` with the version that includes new patches, and add three new tests at the end of the file. Replace only the `test_setup_sandbox_runs_sandbox_up` and `test_setup_sandbox_shows_clean_error_on_db_failure` functions and append the three new tests:

```python
def test_setup_sandbox_runs_sandbox_up(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_up.assert_called_once()


def test_setup_sandbox_shows_clean_error_on_db_failure(tmp_path):
    _FakePyodbcProgramming, driver_patch = _patch_pyodbc_programming()
    mock_backend = MagicMock()
    mock_backend.sandbox_up.side_effect = _FakePyodbcProgramming("login failed")

    with (
        driver_patch,
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 2
    assert "Hint:" in result.output


def test_setup_sandbox_calls_require_sandbox_vars(tmp_path):
    """setup-sandbox must call require_sandbox_vars with the sandbox technology."""
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars") as mock_require,
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_require.assert_called_once_with("sql_server")


def test_setup_sandbox_writes_connection_to_manifest(tmp_path):
    """setup-sandbox must write sandbox connection to manifest before creating backend."""
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", return_value="sql_server"),
        patch("shared.cli.setup_sandbox_cmd.require_sandbox_vars"),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_connection_to_manifest", return_value={}) as mock_write,
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once()


def test_setup_sandbox_exits_1_when_sandbox_role_missing(tmp_path):
    """setup-sandbox exits 1 if manifest has no runtime.sandbox technology."""
    _write_manifest(tmp_path)

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._get_sandbox_technology", side_effect=SystemExit(1)),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/cli/test_sandbox_cmds.py -v 2>&1 | tail -20
```

Expected: FAIL — `test_setup_sandbox_runs_sandbox_up` fails because `_get_sandbox_technology` doesn't exist yet in `setup_sandbox_cmd`; `test_setup_sandbox_calls_require_sandbox_vars` fails with `AttributeError`.

- [ ] **Step 3: Implement new `setup_sandbox_cmd.py`**

Replace `lib/shared/cli/setup_sandbox_cmd.py` entirely:

```python
"""setup-sandbox command — provision sandbox database from manifest config."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import typer

from shared.cli.env_check import require_sandbox_vars
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, success
from shared.loader_io import write_manifest_sandbox
from shared.runtime_config import get_extracted_schemas, get_runtime_role, set_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from shared.sandbox.base import SandboxBackend
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Thin wrapper around test_harness_support._load_manifest for patching."""
    return _th_load_manifest(project_root)


def _create_backend(manifest: dict[str, Any]) -> SandboxBackend:
    """Thin wrapper around test_harness_support._create_backend for patching."""
    return _th_create_backend(manifest)


def _get_schemas(manifest: dict[str, Any]) -> list[str]:
    """Return extracted schemas from manifest."""
    return get_extracted_schemas(manifest)


def _get_sandbox_technology(manifest: dict[str, Any]) -> str:
    """Read sandbox technology from manifest. Exits 1 if runtime.sandbox is absent."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        error("manifest.json is missing runtime.sandbox. Run init-ad-migration first.")
        raise typer.Exit(code=1)
    return sandbox_role.technology


def _write_sandbox_connection_to_manifest(
    root: Path, manifest: dict[str, Any], technology: str
) -> dict[str, Any]:
    """Read SANDBOX_* env vars and write runtime.sandbox.connection into manifest.json."""
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        raise ValueError("manifest.json is missing runtime.sandbox")

    if technology == "sql_server":
        connection = RuntimeConnection(
            host=os.environ.get("SANDBOX_MSSQL_HOST") or None,
            port=os.environ.get("SANDBOX_MSSQL_PORT") or None,
            user=os.environ.get("SANDBOX_MSSQL_USER") or None,
            password_env="SANDBOX_MSSQL_PASSWORD",
            driver=os.environ.get("MSSQL_DRIVER", "FreeTDS") or None,
        )
    elif technology == "oracle":
        connection = RuntimeConnection(
            host=os.environ.get("SANDBOX_ORACLE_HOST") or None,
            port=os.environ.get("SANDBOX_ORACLE_PORT") or None,
            service=os.environ.get("SANDBOX_ORACLE_SERVICE") or None,
            user=os.environ.get("SANDBOX_ORACLE_USER") or None,
            password_env="SANDBOX_ORACLE_PASSWORD",
        )
    else:
        raise ValueError(f"Unsupported sandbox technology: {technology}")

    updated_role = RuntimeRole(
        technology=sandbox_role.technology,
        dialect=sandbox_role.dialect,
        connection=connection,
        schemas=sandbox_role.schemas,
    )
    updated_manifest = set_runtime_role(manifest, "sandbox", updated_role)
    manifest_path = root / "manifest.json"
    manifest_path.write_text(
        json.dumps(updated_manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "event=sandbox_connection_written component=setup_sandbox_cmd technology=%s",
        technology,
    )
    return updated_manifest


def _write_sandbox_to_manifest(project_root: Path, sandbox_database: str) -> None:
    """Persist sandbox database name into manifest.json."""
    write_manifest_sandbox(project_root, sandbox_database)


def setup_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Provision sandbox schema from manifest runtime.sandbox configuration."""
    root = project_root if project_root is not None else Path.cwd()

    manifest = _load_manifest(root)
    technology = _get_sandbox_technology(manifest)
    require_sandbox_vars(technology)
    manifest = _write_sandbox_connection_to_manifest(root, manifest, technology)

    schemas = _get_schemas(manifest)

    if not yes:
        confirmed = typer.confirm(
            f"Create sandbox database cloning schemas: {', '.join(schemas) or '(none)'}?"
        )
        if not confirmed:
            console.print("Aborted.")
            raise typer.Exit(code=0)

    backend = _create_backend(manifest)

    console.print(f"Provisioning sandbox for schemas: [bold]{', '.join(schemas)}[/bold]...")
    with console.status("Running sandbox_up..."):
        with cli_error_handler("provisioning sandbox database"):
            result = backend.sandbox_up(schemas=schemas)

    logger.info(
        "event=sandbox_up status=%s sandbox_database=%s tables=%d views=%d procedures=%d errors=%d",
        result.status,
        result.sandbox_database,
        len(result.tables_cloned),
        len(result.views_cloned),
        len(result.procedures_cloned),
        len(result.errors),
    )

    if result.status == "error":
        if result.errors:
            for entry in result.errors:
                error(f"[{entry.code}] {entry.message}")
        raise typer.Exit(code=1)

    _write_sandbox_to_manifest(root, result.sandbox_database)

    print_table(
        "Sandbox Setup",
        [
            ("Database", result.sandbox_database),
            ("Tables cloned", str(len(result.tables_cloned))),
            ("Views cloned", str(len(result.views_cloned))),
            ("Procedures cloned", str(len(result.procedures_cloned))),
            ("Status", result.status),
        ],
        columns=("", ""),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/cli/test_sandbox_cmds.py -v 2>&1 | tail -15
```

Expected: all PASS.

- [ ] **Step 5: Run full CLI suite**

```bash
uv run pytest tests/unit/cli/ -q 2>&1 | tail -5
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add lib/shared/cli/setup_sandbox_cmd.py tests/unit/cli/test_sandbox_cmds.py
git commit -m "fix: sandbox cold-start — read SANDBOX_* env vars and write manifest before connecting"
```

---

### Task 5: Update `.envrc` template and pre-commit hook in `init_templates.py`

**Files:**

- Modify: `lib/shared/init_templates.py`

No unit tests — verify by printing template output.

- [ ] **Step 1: Find SQL Server template function**

```bash
grep -n "_envrc_sql_server\|SA_PASSWORD\|MSSQL_HOST\|MSSQL_PORT\|MSSQL_DB\|MSSQL_USER" \
  lib/shared/init_templates.py | head -20
```

Note the line numbers of the string content inside `_envrc_sql_server()`.

- [ ] **Step 2: Replace SQL Server `.envrc` template content**

Find `_envrc_sql_server()` and replace the returned string with:

```python
def _envrc_sql_server() -> str:
    return """\
# Source database (SQL Server)
export SOURCE_MSSQL_HOST=
export SOURCE_MSSQL_PORT=1433
export SOURCE_MSSQL_DB=
export SOURCE_MSSQL_USER=sa
export SOURCE_MSSQL_PASSWORD=

# Sandbox database (SQL Server — may be a different server from source)
export SANDBOX_MSSQL_HOST=
export SANDBOX_MSSQL_PORT=1433
export SANDBOX_MSSQL_USER=sa
export SANDBOX_MSSQL_PASSWORD=

# Machine-specific driver override (optional, default: FreeTDS)
# export MSSQL_DRIVER=FreeTDS
"""
```

- [ ] **Step 3: Replace Oracle `.envrc` template content**

Find `_envrc_oracle()` and replace the returned string with:

```python
def _envrc_oracle() -> str:
    return """\
# Source database (Oracle)
export SOURCE_ORACLE_HOST=
export SOURCE_ORACLE_PORT=1521
export SOURCE_ORACLE_SERVICE=
export SOURCE_ORACLE_USER=
export SOURCE_ORACLE_PASSWORD=

# Sandbox database (Oracle — may be a different server from source)
export SANDBOX_ORACLE_HOST=
export SANDBOX_ORACLE_PORT=1521
export SANDBOX_ORACLE_SERVICE=
export SANDBOX_ORACLE_USER=
export SANDBOX_ORACLE_PASSWORD=
"""
```

- [ ] **Step 4: Update pre-commit hook credential scan patterns**

Run to find where the hook scans for credential names:

```bash
grep -n "SA_PASSWORD\|MSSQL_HOST\|ORACLE_PASSWORD\|ORACLE_HOST\|mssql_env_vars" \
  lib/shared/init_templates.py
```

Replace every occurrence of old credential var names in the pre-commit hook pattern string. The hook grep pattern that blocks commits should change from referencing `SA_PASSWORD`, `MSSQL_HOST`, etc. to the new names. The pattern in the hook typically looks like:

```bash
# OLD pattern in hook:
if git show :"$f" 2>/dev/null | grep -qE '(MSSQL_HOST|MSSQL_PORT|MSSQL_DB|SA_PASSWORD)=.+'; then

# NEW pattern:
if git show :"$f" 2>/dev/null | grep -qE '(SOURCE_MSSQL_PASSWORD|SANDBOX_MSSQL_PASSWORD|TARGET_MSSQL_PASSWORD|SOURCE_ORACLE_PASSWORD|SANDBOX_ORACLE_PASSWORD|TARGET_ORACLE_PASSWORD)=.+'; then
```

Also update the README template string that mentions `MSSQL_HOST, MSSQL_PORT, MSSQL_DB, SA_PASSWORD` (the `mssql_env_vars` key) to list the new names.

- [ ] **Step 5: Verify template output**

```bash
cd lib && uv run python -c "
from shared.init_templates import _envrc_sql_server, _envrc_oracle
print('=== SQL Server ===')
print(_envrc_sql_server())
print('=== Oracle ===')
print(_envrc_oracle())
"
```

Expected: output shows `SOURCE_MSSQL_HOST`, `SANDBOX_MSSQL_HOST`, `SOURCE_ORACLE_HOST`, `SANDBOX_ORACLE_HOST` — no old names.

- [ ] **Step 6: Commit**

```bash
git add lib/shared/init_templates.py
git commit -m "refactor: update .envrc template and pre-commit hook for new env var names"
```

---

### Task 6: Update `init-ad-migration.md` and `docs/wiki/CLI-Reference.md`

**Files:**

- Modify: `commands/init-ad-migration.md`
- Modify: `docs/wiki/CLI-Reference.md`

- [ ] **Step 1: Update `commands/init-ad-migration.md`**

Find and replace every occurrence of old env var names. Key locations:

Step 3 (SQL Server checks): update `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_DB`, `SA_PASSWORD` references to `SOURCE_MSSQL_*`.

Step 8 (Handoff) SQL Server block — replace:

```markdown
- **toolbox installed and all MSSQL vars set**: ready to run `ad-migration setup-source ...`
- **toolbox missing or MSSQL vars unset**: Set credentials in `.envrc` ...
```

With:

```markdown
- **toolbox installed and all SOURCE_MSSQL_* + SANDBOX_MSSQL_* vars set**: ready to run `ad-migration setup-source ...`
- **toolbox missing or SOURCE_MSSQL_* vars unset**: Set source and sandbox credentials in `.envrc`, run `direnv allow`, install `toolbox`, then run `ad-migration setup-source`.
```

Step 8 Oracle block — same pattern replacing `ORACLE_*` with `SOURCE_ORACLE_*` and noting `SANDBOX_ORACLE_*`.

Lint:

```bash
markdownlint commands/init-ad-migration.md
```

- [ ] **Step 2: Update env var reference in `docs/wiki/CLI-Reference.md`**

Find the environment variables section and replace with:

```markdown
## Environment variables

All connection env vars follow the `<ROLE>_<TECH>_<FIELD>` convention. Set them in `.envrc` (via `direnv`) or export them in your shell before running the relevant command.

### setup-source

**SQL Server**

| Variable | Description |
|---|---|
| `SOURCE_MSSQL_HOST` | SQL Server hostname |
| `SOURCE_MSSQL_PORT` | SQL Server port |
| `SOURCE_MSSQL_DB` | Source database name |
| `SOURCE_MSSQL_USER` | SQL Server username |
| `SOURCE_MSSQL_PASSWORD` | SQL Server password |

**Oracle**

| Variable | Description |
|---|---|
| `SOURCE_ORACLE_HOST` | Oracle hostname |
| `SOURCE_ORACLE_PORT` | Oracle port |
| `SOURCE_ORACLE_SERVICE` | Oracle service name |
| `SOURCE_ORACLE_USER` | Oracle username |
| `SOURCE_ORACLE_PASSWORD` | Oracle password |

### setup-sandbox

**SQL Server**

| Variable | Description |
|---|---|
| `SANDBOX_MSSQL_HOST` | Sandbox SQL Server hostname |
| `SANDBOX_MSSQL_PORT` | Sandbox SQL Server port |
| `SANDBOX_MSSQL_USER` | Sandbox SQL Server username |
| `SANDBOX_MSSQL_PASSWORD` | Sandbox SQL Server password |

**Oracle**

| Variable | Description |
|---|---|
| `SANDBOX_ORACLE_HOST` | Sandbox Oracle hostname |
| `SANDBOX_ORACLE_PORT` | Sandbox Oracle port |
| `SANDBOX_ORACLE_SERVICE` | Sandbox Oracle service name |
| `SANDBOX_ORACLE_USER` | Sandbox Oracle admin username |
| `SANDBOX_ORACLE_PASSWORD` | Sandbox Oracle admin password |

### setup-target

**SQL Server**

| Variable | Description |
|---|---|
| `TARGET_MSSQL_HOST` | Target SQL Server hostname |
| `TARGET_MSSQL_PORT` | Target SQL Server port |
| `TARGET_MSSQL_DB` | Target database name |
| `TARGET_MSSQL_USER` | Target SQL Server username |
| `TARGET_MSSQL_PASSWORD` | Target SQL Server password |

**Oracle**

| Variable | Description |
|---|---|
| `TARGET_ORACLE_HOST` | Target Oracle hostname |
| `TARGET_ORACLE_PORT` | Target Oracle port |
| `TARGET_ORACLE_SERVICE` | Target Oracle service name |
| `TARGET_ORACLE_USER` | Target Oracle username |
| `TARGET_ORACLE_PASSWORD` | Target Oracle password |
```

Lint:

```bash
markdownlint docs/wiki/CLI-Reference.md
```

- [ ] **Step 3: Commit**

```bash
git add commands/init-ad-migration.md docs/wiki/CLI-Reference.md
git commit -m "docs: update env var references to SOURCE_*/SANDBOX_*/TARGET_* convention"
```

---

### Task 7: Full sweep — run suite and verify no stale env var references

- [ ] **Step 1: Run full lib suite**

```bash
cd lib && uv run pytest -q --ignore=tests/integration 2>&1 | tail -5
```

Expected: all pass.

- [ ] **Step 2: Run CLI test suite**

```bash
uv run pytest tests/unit/cli/ -q 2>&1 | tail -5
```

Expected: all pass.

- [ ] **Step 3: Scan for stale old env var names in source**

```bash
grep -rn \
  "MSSQL_HOST\|MSSQL_PORT\b\|MSSQL_DB\b\|SA_PASSWORD\|ORACLE_HOST\|ORACLE_PORT\|ORACLE_SERVICE\|ORACLE_USER\b\|ORACLE_PASSWORD\|TARGET_WORKSPACE\|TARGET_ACCOUNT\|TARGET_LAKEHOUSE\|TARGET_WAREHOUSE" \
  lib/shared/ tests/unit/cli/ commands/ docs/wiki/ \
  --include="*.py" --include="*.md" \
  | grep -v "__pycache__"
```

Expected: zero matches in `.py` files. Any remaining `.md` matches must be in prose explanations (not code blocks or var references). Fix any that slip through.

- [ ] **Step 4: Push**

```bash
git push
```
