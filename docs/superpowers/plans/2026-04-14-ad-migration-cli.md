# ad-migration CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `ad-migration` standalone Typer CLI as a human-facing layer over the existing `run_*` functions, distributed via pip/uv and Homebrew.

**Architecture:** New `lib/shared/cli/` package with one module per command. Each module calls existing `run_*` functions and formats output with `rich`. Two new helper functions are added to `init.py` and `target_setup.py`. Existing agent-facing CLIs (`migrate-util`, `setup-ddl`, etc.) are left untouched.

**Tech Stack:** Python 3.11+, Typer, Rich, uv; existing `shared` library.

**Design doc:** `docs/design/ad-migration-cli/README.md`

---

## File Map

**Create:**

```text
lib/shared/cli/__init__.py
lib/shared/cli/env_check.py          # env var validation, exits 1 with clear message
lib/shared/cli/output.py             # rich console helpers and spinner
lib/shared/cli/git_ops.py            # stage_and_commit helper
lib/shared/cli/main.py               # top-level Typer app, registers all commands
lib/shared/cli/setup_source_cmd.py
lib/shared/cli/setup_target_cmd.py
lib/shared/cli/setup_sandbox_cmd.py
lib/shared/cli/teardown_sandbox_cmd.py
lib/shared/cli/reset_cmd.py
lib/shared/cli/exclude_table_cmd.py
lib/shared/cli/add_source_table_cmd.py
tests/unit/cli/__init__.py
tests/unit/cli/test_env_check.py
tests/unit/cli/test_setup_source_cmd.py
tests/unit/cli/test_setup_target_cmd.py
tests/unit/cli/test_sandbox_cmds.py
tests/unit/cli/test_pipeline_cmds.py
scripts/cleanup-worktrees.sh
scripts/commit.sh
scripts/commit-push-pr.sh
```

**Modify:**

```text
lib/pyproject.toml                   # add rich dep + ad-migration entrypoint
lib/shared/target_setup.py          # add write_target_runtime_from_env()
```

**Delete:**

```text
commands/setup-ddl.md
commands/setup-target.md
commands/setup-sandbox.md
commands/teardown-sandbox.md
commands/reset-migration.md
commands/exclude-table.md
commands/add-source-tables.md
commands/commit.md
commands/commit-push-pr.md
```

---

## Task 1: Package skeleton, shared helpers, pyproject.toml

**Files:**

- Create: `lib/shared/cli/__init__.py`
- Create: `lib/shared/cli/env_check.py`
- Create: `lib/shared/cli/output.py`
- Create: `lib/shared/cli/git_ops.py`
- Create: `lib/shared/cli/main.py`
- Create: `tests/unit/cli/__init__.py`
- Create: `tests/unit/cli/test_env_check.py`
- Modify: `lib/pyproject.toml`

- [ ] **Step 1: Write failing tests for env_check**

```python
# tests/unit/cli/test_env_check.py
import os
import pytest
from shared.cli.env_check import require_source_vars, require_target_vars


def test_require_source_vars_sql_server_passes_when_all_set(monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SA_PASSWORD", "secret")
    require_source_vars("sql_server")  # should not raise or exit


def test_require_source_vars_sql_server_exits_on_missing(monkeypatch, capsys):
    for var in ("MSSQL_HOST", "MSSQL_PORT", "MSSQL_DB", "SA_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("sql_server")
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "MSSQL_HOST" in captured.err
    assert "MSSQL_PORT" in captured.err
    assert "SA_PASSWORD" in captured.err


def test_require_source_vars_oracle_exits_on_missing(monkeypatch, capsys):
    for var in ("ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "ORACLE_USER", "ORACLE_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_source_vars("oracle")
    assert exc_info.value.code == 1
    assert "ORACLE_HOST" in capsys.readouterr().err


def test_require_target_vars_snowflake_exits_on_missing(monkeypatch, capsys):
    for var in ("TARGET_ACCOUNT", "TARGET_DATABASE", "TARGET_SCHEMA",
                "TARGET_WAREHOUSE", "TARGET_USER", "TARGET_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(SystemExit) as exc_info:
        require_target_vars("snowflake")
    assert exc_info.value.code == 1
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_env_check.py -v
```

Expected: `ModuleNotFoundError` (package not yet created).

- [ ] **Step 3: Create package files**

```python
# lib/shared/cli/__init__.py
```

```python
# lib/shared/cli/env_check.py
"""Env var validation for ad-migration CLI commands.

Validates required env vars before a command runs.
Exits 1 with a clear message listing every missing var.
"""
from __future__ import annotations

import os
import sys

_SOURCE_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "MSSQL_HOST": "SQL Server hostname",
        "MSSQL_PORT": "SQL Server port",
        "MSSQL_DB": "SQL Server database name",
        "SA_PASSWORD": "SQL Server SA password",
    },
    "oracle": {
        "ORACLE_HOST": "Oracle hostname",
        "ORACLE_PORT": "Oracle port",
        "ORACLE_SERVICE": "Oracle service name",
        "ORACLE_USER": "Oracle username",
        "ORACLE_PASSWORD": "Oracle password",
    },
}

_TARGET_VARS: dict[str, dict[str, str]] = {
    "fabric": {
        "TARGET_WORKSPACE": "Microsoft Fabric workspace name",
        "TARGET_LAKEHOUSE": "Microsoft Fabric lakehouse name",
        "TARGET_CLIENT_ID": "Azure service principal client ID",
        "TARGET_CLIENT_SECRET": "Azure service principal client secret",
        "TARGET_TENANT_ID": "Azure tenant ID",
    },
    "snowflake": {
        "TARGET_ACCOUNT": "Snowflake account identifier",
        "TARGET_DATABASE": "Snowflake target database",
        "TARGET_SCHEMA": "Snowflake target schema",
        "TARGET_WAREHOUSE": "Snowflake virtual warehouse",
        "TARGET_USER": "Snowflake username",
        "TARGET_PASSWORD": "Snowflake password",
    },
    "duckdb": {
        "TARGET_PATH": "DuckDB file path (e.g. /path/to/warehouse.duckdb)",
    },
}


def require_source_vars(technology: str) -> None:
    """Validate source env vars. Exits 1 if any are missing."""
    _check(_SOURCE_VARS.get(technology, {}), technology, "setup-source")


def require_target_vars(technology: str) -> None:
    """Validate target env vars. Exits 1 if any are missing."""
    _check(_TARGET_VARS.get(technology, {}), technology, "setup-target")


def _check(required: dict[str, str], technology: str, command: str) -> None:
    missing = [var for var in required if not os.environ.get(var)]
    if not missing:
        return
    lines = [f"Error: missing required environment variables for {technology}:\n"]
    for var in missing:
        lines.append(f"  {var:<30} not set")
    lines.append(f"\nSet these in your shell or .envrc before running {command}.")
    print("\n".join(lines), file=sys.stderr)
    sys.exit(1)
```

```python
# lib/shared/cli/output.py
"""Rich formatting helpers for ad-migration CLI commands."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


def success(message: str) -> None:
    console.print(f"[green]✓[/green] {message}")


def warn(message: str) -> None:
    err_console.print(f"[yellow]![/yellow] {message}")


def error(message: str) -> None:
    err_console.print(f"[red]✗[/red] {message}")


def print_table(title: str, rows: list[tuple[str, str]], columns: tuple[str, str] = ("Item", "Status")) -> None:
    table = Table(title=title, show_header=True, header_style="bold")
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*row)
    console.print(table)


@contextmanager
def spinner(message: str) -> Iterator[None]:
    with Progress(SpinnerColumn(), TextColumn(message), transient=True) as progress:
        progress.add_task("", total=None)
        yield
```

```python
# lib/shared/cli/git_ops.py
"""Git helpers for ad-migration CLI commands."""
from __future__ import annotations

import subprocess
from pathlib import Path


def stage_and_commit(files: list[Path], message: str, project_root: Path) -> bool:
    """Stage specific files and commit. Returns True if a commit was made.

    Returns False silently when there is nothing to commit.
    Raises RuntimeError on git failures.
    """
    try:
        subprocess.run(
            ["git", "add", "--"] + [str(f) for f in files],
            cwd=project_root,
            check=True,
            capture_output=True,
        )
        result = subprocess.run(
            ["git", "commit", "-m", message],
            cwd=project_root,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return True
        if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
            return False
        raise subprocess.CalledProcessError(result.returncode, "git commit", result.stderr)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"git operation failed: {exc.stderr or exc}") from exc


def is_git_repo(project_root: Path) -> bool:
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"],
        cwd=project_root,
        capture_output=True,
    )
    return result.returncode == 0
```

```python
# lib/shared/cli/main.py
"""Top-level ad-migration Typer app."""
import typer

from shared.cli.add_source_table_cmd import add_source_table
from shared.cli.exclude_table_cmd import exclude_table
from shared.cli.reset_cmd import reset
from shared.cli.setup_sandbox_cmd import setup_sandbox
from shared.cli.setup_source_cmd import setup_source
from shared.cli.setup_target_cmd import setup_target
from shared.cli.teardown_sandbox_cmd import teardown_sandbox

app = typer.Typer(
    name="ad-migration",
    help="Migration pipeline CLI for warehouses to dbt.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)

app.command("setup-source")(setup_source)
app.command("setup-target")(setup_target)
app.command("setup-sandbox")(setup_sandbox)
app.command("teardown-sandbox")(teardown_sandbox)
app.command("reset")(reset)
app.command("exclude-table")(exclude_table)
app.command("add-source-table")(add_source_table)
```

- [ ] **Step 4: Add `rich` dependency and `ad-migration` entrypoint to `lib/pyproject.toml`**

In `[project] dependencies`, add `"rich>=13.0"`.

In `[project.scripts]`, add:

```toml
ad-migration = "shared.cli.main:app"
```

- [ ] **Step 5: Run tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_env_check.py -v
```

Expected: 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add lib/shared/cli/ tests/unit/cli/__init__.py tests/unit/cli/test_env_check.py lib/pyproject.toml
git commit -m "feat: add ad-migration CLI package skeleton and env_check"
```

---

## Task 2: `setup-source` command

**Files:**

- Create: `lib/shared/cli/setup_source_cmd.py`
- Create: `tests/unit/cli/test_setup_source_cmd.py`

The command: validates env vars → checks technology prereqs → scaffolds source-specific files (`run_scaffold_project`) → runs extraction (`run_extract`) → commits.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/cli/test_setup_source_cmd.py
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.init import ScaffoldHooksOutput, ScaffoldProjectOutput

runner = CliRunner()

_SCAFFOLD_OUT = ScaffoldProjectOutput(files_created=["CLAUDE.md", ".envrc"], files_updated=[], files_skipped=[])
_HOOKS_OUT = ScaffoldHooksOutput(hook_created=True, hooks_path_configured=True)
_EXTRACT_OUT = {"tables": 5, "procedures": 3, "views": 2, "functions": 0}


def test_setup_source_sql_server_runs_extraction(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SA_PASSWORD", "secret")

    with (
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
        patch("shared.cli.setup_source_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_source_cmd.stage_and_commit", return_value=True),
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--schemas", "silver,gold",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_called_once_with(tmp_path, "AdventureWorks2022", ["silver", "gold"])


def test_setup_source_fails_fast_on_missing_env(tmp_path, monkeypatch):
    for var in ("MSSQL_HOST", "MSSQL_PORT", "MSSQL_DB", "SA_PASSWORD"):
        monkeypatch.delenv(var, raising=False)

    result = runner.invoke(
        app,
        ["setup-source", "--technology", "sql_server", "--schemas", "silver",
         "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1


def test_setup_source_no_commit_flag(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "h")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT),
        patch("shared.cli.setup_source_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_source_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--schemas", "silver",
             "--no-commit", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_commit.assert_not_called()
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_setup_source_cmd.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement `setup_source_cmd.py`**

```python
# lib/shared/cli/setup_source_cmd.py
"""setup-source command — extract DDL from source database."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Optional

import typer

from shared.cli.env_check import require_source_vars
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, success, warn
from shared.env_config import resolve_project_root
from shared.setup_ddl_support.extract import run_extract


def setup_source(
    technology: str = typer.Option(..., "--technology", help="Source technology: sql_server or oracle"),
    schemas: str = typer.Option(..., "--schemas", help="Comma-separated schema names to extract (e.g. silver,gold)"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after extraction"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Validate source env vars and extract DDL from the source database.

    Run /init-ad-migration (plugin command) first to install the CLI, check prerequisites, and scaffold project files.
    """
    root = resolve_project_root(project_root)
    schema_list = [s.strip() for s in schemas.split(",") if s.strip()]

    require_source_vars(technology)

    database = os.environ.get("MSSQL_DB") if technology == "sql_server" else None

    console.print(f"Extracting DDL from schemas: [bold]{', '.join(schema_list)}[/bold]")
    with console.status("Extracting..."):
        result = run_extract(root, database, schema_list)

    _report_extract(result)

    if no_commit:
        return

    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return

    commit_files = [root / "ddl", root / "catalog", root / "manifest.json"]
    stage_and_commit(
        [f for f in commit_files if f.exists()],
        f"extract DDL ({technology}, schemas: {', '.join(schema_list)})",
        root,
    )
    success("Extraction committed.")


def _check_source_prereqs(technology: str) -> None:
    if technology == "sql_server":
        result = subprocess.run(
            ["brew", "list", "--formula", "freetds"],
            capture_output=True,
        )
        if result.returncode != 0:
            console.print("[red]✗[/red] freetds not installed. Run: brew install freetds")
            raise typer.Exit(code=1)
        success("freetds installed")
    elif technology == "oracle":
        for cmd, name in [(["sql", "-V"], "sqlcl"), (["java", "-version"], "java")]:
            r = subprocess.run(cmd, capture_output=True)
            if r.returncode != 0:
                console.print(f"[red]✗[/red] {name} not found. Install SQLcl and Java 11+.")
                raise typer.Exit(code=1)
            success(f"{name} available")


def _report_extract(result: dict) -> None:
    for key, label in (("tables", "Tables"), ("procedures", "Procedures"), ("views", "Views"), ("functions", "Functions")):
        count = result.get(key, 0)
        if isinstance(count, list):
            count = len(count)
        success(f"{label:<15} {count}")
```

- [ ] **Step 4: Run tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_setup_source_cmd.py -v
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/shared/cli/setup_source_cmd.py tests/unit/cli/test_setup_source_cmd.py
git commit -m "feat: add ad-migration setup-source command"
```

---

## Task 3: `setup-target` command

**Files:**

- Modify: `lib/shared/target_setup.py` — add `write_target_runtime_from_env()`
- Create: `lib/shared/cli/setup_target_cmd.py`
- Create: `tests/unit/cli/test_setup_target_cmd.py`

- [ ] **Step 1: Add `write_target_runtime_from_env` to `lib/shared/target_setup.py`**

Add after the existing imports and before `scaffold_target_project`:

```python
import json
import os

from shared.runtime_config_models import RuntimeConnection, RuntimeRole, RuntimeSchemas
from shared.setup_ddl_support.manifest import read_manifest_strict


_TARGET_ENV_MAPS: dict[str, dict[str, str]] = {
    "snowflake": {
        "host": "TARGET_ACCOUNT",
        "database": "TARGET_DATABASE",
        "schema": "TARGET_SCHEMA",
        "driver": "TARGET_WAREHOUSE",
        "user": "TARGET_USER",
        "password_env": "TARGET_PASSWORD",
    },
    "fabric": {
        "database": "TARGET_WORKSPACE",
        "schema": "TARGET_LAKEHOUSE",
        "user": "TARGET_CLIENT_ID",
        "password_env": "TARGET_CLIENT_SECRET",
    },
    "duckdb": {
        "path": "TARGET_PATH",
        "schema": "TARGET_SCHEMA",
    },
}


def write_target_runtime_from_env(
    project_root: Path,
    technology: str,
    source_schema: str = "bronze",
) -> RuntimeRole:
    """Read TARGET_* env vars and write runtime.target to manifest.json.

    Returns the RuntimeRole written. Raises ValueError if manifest is missing.
    """
    env_map = _TARGET_ENV_MAPS.get(technology, {})
    connection_kwargs: dict[str, str] = {}
    for field, env_var in env_map.items():
        value = os.environ.get(env_var, "")
        if value:
            connection_kwargs[field] = value

    role = RuntimeRole(
        technology=technology,
        dialect=technology,
        connection=RuntimeConnection(**connection_kwargs),
        schemas=RuntimeSchemas(source=source_schema, marts=None),
    )

    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"manifest.json not found at {manifest_path}. Run setup-source first.")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if "runtime" not in manifest or not isinstance(manifest["runtime"], dict):
        manifest["runtime"] = {}
    manifest["runtime"]["target"] = role.model_dump(mode="json", exclude_none=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return role
```

- [ ] **Step 2: Write failing tests**

```python
# tests/unit/cli/test_setup_target_cmd.py
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

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


def test_setup_target_writes_runtime_and_runs_orchestrator(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET_ACCOUNT", "acme.snowflakecomputing.com")
    monkeypatch.setenv("TARGET_DATABASE", "WAREHOUSE")
    monkeypatch.setenv("TARGET_SCHEMA", "bronze")
    monkeypatch.setenv("TARGET_WAREHOUSE", "COMPUTE_WH")
    monkeypatch.setenv("TARGET_USER", "loader")
    monkeypatch.setenv("TARGET_PASSWORD", "secret")
    _write_manifest(tmp_path)

    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env") as mock_write,
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT) as mock_setup,
        patch("shared.cli.setup_target_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_target_cmd.stage_and_commit", return_value=True),
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "snowflake", "bronze")
    mock_setup.assert_called_once_with(tmp_path)


def test_setup_target_no_commit(tmp_path, monkeypatch):
    _write_manifest(tmp_path)
    monkeypatch.setenv("TARGET_PATH", "/tmp/warehouse.duckdb")

    with (
        patch("shared.cli.setup_target_cmd.require_target_vars"),
        patch("shared.cli.setup_target_cmd.write_target_runtime_from_env"),
        patch("shared.cli.setup_target_cmd.run_setup_target", return_value=_SETUP_TARGET_OUT),
        patch("shared.cli.setup_target_cmd.is_git_repo", return_value=True),
        patch("shared.cli.setup_target_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "duckdb", "--no-commit", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_commit.assert_not_called()
```

- [ ] **Step 3: Run tests to confirm failure**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_setup_target_cmd.py -v
```

Expected: `ImportError`.

- [ ] **Step 4: Implement `setup_target_cmd.py`**

```python
# lib/shared/cli/setup_target_cmd.py
"""setup-target command — configure target runtime and scaffold dbt."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from shared.cli.env_check import require_target_vars
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, success, warn
from shared.env_config import resolve_project_root
from shared.target_setup import run_setup_target, write_target_runtime_from_env


def setup_target(
    technology: str = typer.Option(..., "--technology", help="Target technology: fabric, snowflake, or duckdb"),
    source_schema: str = typer.Option("bronze", "--source-schema", help="Target source schema (default: bronze)"),
    no_commit: bool = typer.Option(False, "--no-commit"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Configure target runtime, scaffold dbt project, and generate sources.yml."""
    root = resolve_project_root(project_root)

    require_target_vars(technology)

    console.print(f"\nWriting runtime.target for [bold]{technology}[/bold]...")
    write_target_runtime_from_env(root, technology, source_schema)
    success(f"runtime.target written (source_schema={source_schema})")

    console.print("Running target setup...")
    with console.status("Scaffolding dbt project and generating sources.yml..."):
        result = run_setup_target(root)

    for f in result.files:
        success(f"created  {f}")
    if result.sources_path:
        success(f"sources  {result.sources_path}")
    console.print(
        f"\n  tables in sources.yml: {len(result.desired_tables)} desired, "
        f"{len(result.created_tables)} new, {len(result.existing_tables)} existing"
    )

    if no_commit or not is_git_repo(root):
        if not is_git_repo(root):
            warn("Not a git repository — skipping commit.")
        return

    commit_files = [root / "manifest.json", root / "dbt"]
    stage_and_commit(
        [f for f in commit_files if f.exists()],
        f"feat: setup target ({technology}, source_schema={source_schema})",
        root,
    )
    success("Target setup committed.")
```

- [ ] **Step 5: Run tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_setup_target_cmd.py -v
```

Expected: 2 tests pass.

- [ ] **Step 6: Commit**

```bash
git add lib/shared/target_setup.py lib/shared/cli/setup_target_cmd.py tests/unit/cli/test_setup_target_cmd.py
git commit -m "feat: add ad-migration setup-target command"
```

---

## Task 4: Sandbox commands

**Files:**

- Create: `lib/shared/cli/setup_sandbox_cmd.py`
- Create: `lib/shared/cli/teardown_sandbox_cmd.py`
- Create: `tests/unit/cli/test_sandbox_cmds.py`

Sandbox commands call the sandbox backend directly via `test_harness_support.manifest` helpers.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/cli/test_sandbox_cmds.py
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.sandbox import SandboxDownOutput, SandboxUpOutput

runner = CliRunner()


def _write_manifest(tmp_path: Path, with_sandbox: bool = False) -> None:
    manifest = {
        "schema_version": "1",
        "technology": "sql_server",
        "runtime": {"source": {"technology": "sql_server", "dialect": "tsql", "connection": {}}},
        "extraction": {"schemas": ["silver"]},
    }
    if with_sandbox:
        manifest["runtime"]["sandbox"] = {"technology": "sql_server", "dialect": "tsql", "connection": {}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


_SANDBOX_UP_OUT = SandboxUpOutput(
    sandbox_database="__test_abc123",
    status="ok",
    tables_cloned=["silver.DimCustomer"],
    views_cloned=[],
    procedures_cloned=["silver.usp_load"],
    errors=[],
)
_SANDBOX_DOWN_OUT = SandboxDownOutput(sandbox_database="__test_abc123", status="ok")


def test_setup_sandbox_runs_sandbox_up(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_up.return_value = _SANDBOX_UP_OUT

    with (
        patch("shared.cli.setup_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.setup_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.setup_sandbox_cmd._get_schemas", return_value=["silver"]),
        patch("shared.cli.setup_sandbox_cmd._write_sandbox_to_manifest"),
    ):
        result = runner.invoke(app, ["setup-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_backend.sandbox_up.assert_called_once()


def test_teardown_sandbox_requires_confirmation(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)

    with patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={"runtime": {"sandbox": {}}}):
        # User enters 'n' at the prompt
        result = runner.invoke(app, ["teardown-sandbox", "--project-root", str(tmp_path)], input="n\n")

    assert result.exit_code == 0
    assert "Aborted" in result.output or result.exit_code == 0


def test_teardown_sandbox_yes_flag_skips_prompt(tmp_path):
    _write_manifest(tmp_path, with_sandbox=True)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = _SANDBOX_DOWN_OUT

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 0
    mock_backend.sandbox_down.assert_called_once_with("__test_abc123")
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_sandbox_cmds.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement `setup_sandbox_cmd.py`**

```python
# lib/shared/cli/setup_sandbox_cmd.py
"""setup-sandbox command — create throwaway sandbox execution environment."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from shared.cli.output import console, error, success, warn
from shared.env_config import resolve_project_root
from shared.test_harness_support.manifest import (
    _create_backend,
    _load_manifest,
)
from shared.runtime_config import get_extracted_schemas, get_sandbox_name
from shared.setup_ddl_support.manifest import write_manifest_sandbox_runtime


def _get_schemas(manifest: dict) -> list[str]:
    return get_extracted_schemas(manifest)


def _write_sandbox_to_manifest(project_root: Path, sandbox_db: str) -> None:
    write_manifest_sandbox_runtime(project_root, sandbox_db)


def setup_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Create a throwaway sandbox execution environment from the source runtime."""
    root = resolve_project_root(project_root)

    manifest = _load_manifest(root)
    backend = _create_backend(manifest)
    schemas = _get_schemas(manifest)

    console.print(f"\nSandbox setup — schemas: [bold]{', '.join(schemas)}[/bold]")

    if not yes:
        typer.confirm("Create sandbox?", abort=True)

    with console.status("Creating sandbox..."):
        result = backend.sandbox_up(schemas=schemas)

    if result.status == "error":
        for e in result.errors:
            error(f"{e.code}: {e.message}")
        raise typer.Exit(code=1)

    _write_sandbox_to_manifest(root, result.sandbox_database)
    success(f"Sandbox created: {result.sandbox_database}")
    console.print(
        f"  tables: {len(result.tables_cloned)}  "
        f"procedures: {len(result.procedures_cloned)}  "
        f"views: {len(result.views_cloned)}"
    )
    if result.status == "partial":
        warn("Some objects failed to clone — sandbox is usable but incomplete.")
        for e in result.errors:
            warn(f"  {e.code}: {e.message}")
```

- [ ] **Step 4: Implement `teardown_sandbox_cmd.py`**

```python
# lib/shared/cli/teardown_sandbox_cmd.py
"""teardown-sandbox command — drop active sandbox."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from shared.cli.output import console, error, success
from shared.env_config import resolve_project_root
from shared.test_harness_support.manifest import _create_backend, _load_manifest
from shared.runtime_config import get_sandbox_name


def _get_sandbox_name(manifest: dict) -> str | None:
    return get_sandbox_name(manifest)


def teardown_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Drop the active sandbox created by setup-sandbox."""
    root = resolve_project_root(project_root)

    manifest = _load_manifest(root)
    sandbox_db = _get_sandbox_name(manifest)
    if not sandbox_db:
        error("No sandbox configured in manifest.json. Run setup-sandbox first.")
        raise typer.Exit(code=1)

    console.print(f"\nWill drop sandbox: [bold]{sandbox_db}[/bold]")
    if not yes:
        typer.confirm("Proceed?", abort=True)

    backend = _create_backend(manifest)
    with console.status(f"Dropping {sandbox_db}..."):
        result = backend.sandbox_down(sandbox_db)

    if result.status == "error":
        for e in result.errors:
            error(f"{e.code}: {e.message}")
        raise typer.Exit(code=1)

    success(f"Sandbox dropped: {sandbox_db}")
```

- [ ] **Step 5: Check `write_manifest_sandbox_runtime` import**

The import `from shared.setup_ddl_support.manifest import write_manifest_sandbox_runtime` may not exist by that name. Find the correct function that writes the sandbox database name to `manifest.json` by running:

```bash
grep -rn "sandbox" lib/shared/setup_ddl_support/manifest.py lib/shared/runtime_config.py | grep "def "
```

Update the import in `setup_sandbox_cmd.py` to match the actual function name.

- [ ] **Step 6: Run tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_sandbox_cmds.py -v
```

Expected: 3 tests pass.

- [ ] **Step 7: Commit**

```bash
git add lib/shared/cli/setup_sandbox_cmd.py lib/shared/cli/teardown_sandbox_cmd.py tests/unit/cli/test_sandbox_cmds.py
git commit -m "feat: add ad-migration setup-sandbox and teardown-sandbox commands"
```

---

## Task 5: Pipeline state commands — `reset`, `exclude-table`, `add-source-table`

**Files:**

- Create: `lib/shared/cli/reset_cmd.py`
- Create: `lib/shared/cli/exclude_table_cmd.py`
- Create: `lib/shared/cli/add_source_table_cmd.py`
- Create: `tests/unit/cli/test_pipeline_cmds.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/cli/test_pipeline_cmds.py
import json
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.dry_run import ExcludeOutput, ResetMigrationOutput

runner = CliRunner()


def _write_manifest(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(json.dumps({"schema_version": "1"}), encoding="utf-8")


# ── reset ────────────────────────────────────────────────────────────────────

_RESET_OUT = ResetMigrationOutput(
    stage="scope",
    targets=[],
    reset=["silver.DimCustomer"],
    noop=[],
    blocked=[],
    not_found=[],
)


def test_reset_runs_after_confirmation(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=_RESET_OUT) as mock_reset:
        result = runner.invoke(
            app,
            ["reset", "scope", "silver.DimCustomer", "--yes", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "scope", ["silver.DimCustomer"])


def test_reset_aborts_on_no(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        result = runner.invoke(
            app,
            ["reset", "scope", "silver.DimCustomer", "--project-root", str(tmp_path)],
            input="n\n",
        )
    mock_reset.assert_not_called()


def test_reset_rejects_invalid_stage(tmp_path):
    result = runner.invoke(app, ["reset", "invalid-stage", "silver.Foo", "--yes",
                                  "--project-root", str(tmp_path)])
    assert result.exit_code == 1


# ── exclude-table ────────────────────────────────────────────────────────────

_EXCLUDE_OUT = ExcludeOutput(marked=["silver.AuditLog"], not_found=[])


def test_exclude_table_marks_and_commits(tmp_path):
    _write_manifest(tmp_path)
    (tmp_path / "catalog").mkdir()
    (tmp_path / "catalog" / "tables").mkdir()

    with (
        patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT),
        patch("shared.cli.exclude_table_cmd.is_git_repo", return_value=True),
        patch("shared.cli.exclude_table_cmd.stage_and_commit", return_value=True) as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_commit.assert_called_once()


def test_exclude_table_no_commit_flag(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT),
        patch("shared.cli.exclude_table_cmd.stage_and_commit") as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--no-commit", "--project-root", str(tmp_path)],
        )
    mock_commit.assert_not_called()


# ── add-source-table ─────────────────────────────────────────────────────────

from shared.output_models.catalog_writer import WriteSourceOutput


def test_add_source_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = {"ready": True, "reason": "scope complete"}
    write_out = WriteSourceOutput(written="catalog/tables/silver.audittest.json", is_source=True, status="ok")

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source", return_value=write_out),
        patch("shared.cli.add_source_table_cmd.is_git_repo", return_value=True),
        patch("shared.cli.add_source_table_cmd.stage_and_commit", return_value=True) as mock_commit,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_commit.assert_called_once()


def test_add_source_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = {"ready": False, "reason": "scope not complete"}

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source") as mock_write,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_write.assert_not_called()
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_pipeline_cmds.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement `reset_cmd.py`**

```python
# lib/shared/cli/reset_cmd.py
"""reset command — reset migration stage for one or more objects."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

from shared.cli.output import console, error, success, warn
from shared.dry_run_core import RESETTABLE_STAGES, run_reset_migration
from shared.env_config import resolve_project_root

_STAGE_CLEARS: dict[str, str] = {
    "scope": "clears: scoping, profile, test_gen, refactor; deletes test-specs",
    "profile": "clears: profile, test_gen, refactor; deletes test-specs",
    "generate-tests": "clears: test_gen, refactor; deletes test-specs",
    "refactor": "clears: refactor only",
}


def reset(
    stage: str = typer.Argument(..., help="Stage to reset from: scope, profile, generate-tests, refactor"),
    fqns: List[str] = typer.Argument(..., help="Fully-qualified table names (e.g. silver.DimCustomer)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Reset migration stage for one or more tables."""
    root = resolve_project_root(project_root)

    if stage not in RESETTABLE_STAGES:
        error(f"Invalid stage '{stage}'. Valid stages: {', '.join(sorted(RESETTABLE_STAGES))}")
        raise typer.Exit(code=1)

    console.print(f"\nReset [bold]{stage}[/bold] for: {', '.join(fqns)}")
    console.print(f"  {_STAGE_CLEARS[stage]}")
    console.print("  Generated dbt model artifacts are NOT removed.")

    if not yes:
        typer.confirm("\nProceed?", abort=True)

    result = run_reset_migration(root, stage, list(fqns))

    for fqn in result.reset:
        success(f"reset    {fqn}")
    for fqn in result.noop:
        console.print(f"  [dim]no-op    {fqn}[/dim]")
    for fqn in result.blocked:
        warn(f"blocked  {fqn} (model generation complete — cannot reset)")
    for fqn in result.not_found:
        warn(f"missing  {fqn} (no catalog file)")
```

- [ ] **Step 4: Implement `exclude_table_cmd.py`**

```python
# lib/shared/cli/exclude_table_cmd.py
"""exclude-table command — mark objects as excluded from pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, success, warn
from shared.dry_run_core import run_exclude
from shared.env_config import resolve_project_root
from shared.name_resolver import normalize


def exclude_table(
    fqns: List[str] = typer.Argument(..., help="Fully-qualified names to exclude (e.g. silver.AuditLog)"),
    no_commit: bool = typer.Option(False, "--no-commit"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Mark tables or views as excluded from the migration pipeline."""
    root = resolve_project_root(project_root)
    result = run_exclude(root, list(fqns))

    for fqn in result.marked:
        success(f"excluded  {fqn}")
    for fqn in result.not_found:
        warn(f"missing   {fqn} (no catalog file — run setup-source first)")

    if not result.marked or no_commit:
        return

    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return

    fqn_list = " ".join(result.marked)
    msg = f"chore: exclude {fqn_list[:60]} from migration pipeline"
    catalog_files = []
    for fqn in result.marked:
        norm = normalize(fqn)
        for subdir in ("tables", "views"):
            p = root / "catalog" / subdir / f"{norm}.json"
            if p.exists():
                catalog_files.append(p)
                break
    stage_and_commit(catalog_files, msg, root)
    success("Exclusion committed.")
```

- [ ] **Step 5: Implement `add_source_table_cmd.py`**

```python
# lib/shared/cli/add_source_table_cmd.py
"""add-source-table command — mark tables as dbt sources."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

from shared.catalog_writer import run_write_source
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, success, warn
from shared.dry_run_core import run_ready
from shared.env_config import resolve_project_root
from shared.loader_data import CatalogFileMissingError
from shared.name_resolver import normalize


def add_source_table(
    fqns: List[str] = typer.Argument(..., help="Fully-qualified table names to mark as dbt sources"),
    no_commit: bool = typer.Option(False, "--no-commit"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Mark tables as dbt sources (is_source: true)."""
    root = resolve_project_root(project_root)
    written: list[Path] = []

    for fqn in fqns:
        ready = run_ready(root, "scope", fqn)
        if not ready.get("ready", False):
            warn(f"skipped  {fqn} — {ready.get('reason', 'scope not complete')}")
            continue

        try:
            result = run_write_source(root, fqn, value=True)
            success(f"source   {fqn} → is_source: true")
            written.append(root / result.written)
        except CatalogFileMissingError:
            warn(f"missing  {fqn} (no catalog file — run setup-source first)")
        except ValueError as exc:
            warn(f"skipped  {fqn} — {exc}")

    if not written or no_commit:
        return

    if not is_git_repo(root):
        warn("Not a git repository — skipping commit.")
        return

    fqn_list = " ".join(fqns[:3])
    stage_and_commit(written, f"feat: mark {fqn_list} as dbt sources", root)
    success("Source tables committed.")
```

- [ ] **Step 6: Run tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/test_pipeline_cmds.py -v
```

Expected: 7 tests pass.

- [ ] **Step 7: Commit**

```bash
git add lib/shared/cli/reset_cmd.py lib/shared/cli/exclude_table_cmd.py lib/shared/cli/add_source_table_cmd.py tests/unit/cli/test_pipeline_cmds.py
git commit -m "feat: add ad-migration reset, exclude-table, and add-source-table commands"
```

---

## Task 6: Shell scripts

**Files:**

- Create: `scripts/cleanup-worktrees.sh`
- Create: `scripts/commit.sh`
- Create: `scripts/commit-push-pr.sh`

- [ ] **Step 1: Create `scripts/cleanup-worktrees.sh`**

```bash
#!/usr/bin/env bash
# cleanup-worktrees.sh — remove worktrees whose PRs have been merged.
# Usage: ./scripts/cleanup-worktrees.sh [branch-name]
set -euo pipefail

BRANCH_FILTER="${1:-}"

cleaned=0
skipped=0

git worktree list --porcelain | awk '/^worktree /{wt=$2} /^branch /{print wt, $2}' | \
while read -r worktree_path branch_ref; do
  branch="${branch_ref#refs/heads/}"
  [[ "$worktree_path" == "$(git rev-parse --show-toplevel)" ]] && continue
  [[ -n "$BRANCH_FILTER" && "$branch" != "$BRANCH_FILTER" ]] && continue

  merged=$(gh pr list --head "$branch" --state merged --json number --jq 'length' 2>/dev/null || echo 0)
  if [[ "$merged" -gt 0 ]]; then
    echo "  removing worktree: $worktree_path ($branch)"
    git worktree remove "$worktree_path" --force 2>/dev/null || true
    git branch -d "$branch" 2>/dev/null || true
    git push origin --delete "$branch" 2>/dev/null || true
    (( cleaned++ )) || true
  else
    echo "  skipping: $branch (no merged PR)"
    (( skipped++ )) || true
  fi
done

git fetch --prune --quiet

echo ""
echo "cleanup-worktrees complete"
```

- [ ] **Step 2: Create `scripts/commit.sh`**

```bash
#!/usr/bin/env bash
# commit.sh — stage specific files and commit with a message.
# Usage: ./scripts/commit.sh "commit message" file1 [file2 ...]
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <message> <file> [file ...]" >&2
  exit 1
fi

MESSAGE="$1"
shift

git add -- "$@"

if git diff --cached --quiet; then
  echo "Nothing to commit."
  exit 0
fi

git commit -m "$MESSAGE"
echo "Committed: $MESSAGE"
```

- [ ] **Step 3: Create `scripts/commit-push-pr.sh`**

```bash
#!/usr/bin/env bash
# commit-push-pr.sh — commit, push, and open a PR.
# Usage: ./scripts/commit-push-pr.sh "commit message" "PR title" [file1 file2 ...]
# If no files given, commits all staged changes.
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <commit-message> <pr-title> [file ...]" >&2
  exit 1
fi

COMMIT_MSG="$1"
PR_TITLE="$2"
shift 2

if [[ $# -gt 0 ]]; then
  git add -- "$@"
fi

if ! git diff --cached --quiet; then
  git commit -m "$COMMIT_MSG"
fi

BRANCH=$(git branch --show-current)
git push -u origin "$BRANCH"

gh pr create \
  --title "$PR_TITLE" \
  --body "$(cat <<'EOF'
## Summary

Auto-generated PR.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"

echo "PR created for branch: $BRANCH"
```

- [ ] **Step 4: Make scripts executable**

```bash
chmod +x scripts/cleanup-worktrees.sh scripts/commit.sh scripts/commit-push-pr.sh
```

- [ ] **Step 5: Smoke-test `commit.sh` with no args**

```bash
./scripts/commit.sh 2>&1 | grep "Usage:"
```

Expected: `Usage: ./scripts/commit.sh <message> <file> [file ...]`

- [ ] **Step 6: Commit**

```bash
git add scripts/cleanup-worktrees.sh scripts/commit.sh scripts/commit-push-pr.sh
git commit -m "feat: add cleanup-worktrees, commit, and commit-push-pr scripts"
```

---

## Task 7: Remove plugin commands and update init-ad-migration

**Files to delete** (9 files):

```text
commands/setup-ddl.md
commands/setup-target.md
commands/setup-sandbox.md
commands/teardown-sandbox.md
commands/reset-migration.md
commands/exclude-table.md
commands/add-source-tables.md
commands/commit.md
commands/commit-push-pr.md
```

**Files to update** (1 file):

```text
commands/init-ad-migration.md   # add CLI installation step before existing prereq checks
```

- [ ] **Step 1: Delete the command files**

```bash
git rm commands/setup-ddl.md \
       commands/setup-target.md \
       commands/setup-sandbox.md \
       commands/teardown-sandbox.md \
       commands/reset-migration.md \
       commands/exclude-table.md \
       commands/add-source-tables.md \
       commands/commit.md \
       commands/commit-push-pr.md
```

- [ ] **Step 2: Update `commands/init-ad-migration.md` to install the CLI**

Insert a new **Step 1.5: Install ad-migration CLI** between the existing "Step 1: Pre-check" and "Step 2: Source selection" sections of `commands/init-ad-migration.md`:

```markdown
## Step 1.5: Install ad-migration CLI

Check whether `ad-migration` is already on PATH:

\`\`\`bash
ad-migration --version 2>/dev/null && echo "INSTALLED" || echo "NOT_FOUND"
\`\`\`

If already installed, print the version and continue to Step 2.

If not installed, install via Homebrew:

\`\`\`bash
brew tap accelerate-data/homebrew-tap
brew install ad-migration
\`\`\`

After installing, verify:

\`\`\`bash
ad-migration --version
\`\`\`

If Homebrew is not available on the user's machine, tell them:

> Install Homebrew first: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
> Then re-run `/init-ad-migration`.

Do not continue if `ad-migration --version` still fails after installation.
```

Also update the **Step 8: Handoff** section to replace the `/setup-ddl` references with `ad-migration setup-source`:

Replace:

```text
- **toolbox installed and all MSSQL vars set**: ready to run `/setup-ddl` to extract DDL from the live database.
- **toolbox missing or MSSQL vars unset**: DDL file mode (`listing-objects`, `analyzing-table`, `scoping`) is fully available. Live-database skills (`/setup-ddl`) require both `toolbox` and all four MSSQL env vars.
```

With:

```text
- **toolbox installed and all MSSQL vars set**: ready to run `ad-migration setup-source --technology sql_server --schemas <schema>` to extract DDL from the live database.
- **toolbox missing or MSSQL vars unset**: Set credentials in `.envrc`, run `direnv allow`, install `toolbox`, then run `ad-migration setup-source`.
```

Replace:

```text
- **SQLcl + Java installed and all Oracle vars set**: ready to run `/setup-ddl` to extract DDL from the live database. Remember: the Oracle MCP server requires a manual connect step at the start of each session.
- **SQLcl/Java missing or Oracle vars unset**: DDL file mode (`listing-objects`, `analyzing-table`, `scoping`) is fully available. Live-database skills (`/setup-ddl`) require SQLcl, Java 11+, and all five Oracle env vars.
```

With:

```text
- **SQLcl + Java installed and all Oracle vars set**: ready to run `ad-migration setup-source --technology oracle --schemas <schema>` to extract DDL from the live database.
- **SQLcl/Java missing or Oracle vars unset**: Set credentials in `.envrc`, run `direnv allow`, ensure SQLcl and Java 11+ are installed, then run `ad-migration setup-source`.
```

- [ ] **Step 3: Verify remaining commands**

```bash
ls commands/
```

Expected: `init-ad-migration.md`, `scope.md`, `profile.md`, `generate-model.md`, `generate-tests.md`, `refactor.md`, `status.md`.

- [ ] **Step 4: Commit**

```bash
git add commands/
git commit -m "feat: remove deterministic plugin commands — now in ad-migration CLI; update init-ad-migration to install CLI"
```

---

## Task 8: Update repo-map.json

**Files:**

- Modify: `repo-map.json`

- [ ] **Step 1: Update `entrypoints` section**

Add `ad_migration_cli` to `entrypoints`:

```json
"ad_migration_cli": "lib/shared/cli/main.py"
```

- [ ] **Step 2: Update `migration_commands` module description**

In `modules.migration_commands`, update `description` to:

```text
"Stage-specific commands (LLM-driven only): scope.md, profile.md, generate-model.md, generate-tests.md, refactor.md, status.md. Deterministic setup and state-mutation commands have moved to the ad-migration CLI (lib/shared/cli/)."
```

- [ ] **Step 3: Update `commands` section**

Remove entries for deleted commands. Add:

```json
"ad_migration_setup_source": "cd lib && uv run ad-migration setup-source --technology sql_server|oracle --schemas silver,gold",
"ad_migration_setup_target": "cd lib && uv run ad-migration setup-target --technology fabric|snowflake|duckdb",
"ad_migration_setup_sandbox": "cd lib && uv run ad-migration setup-sandbox [--yes]",
"ad_migration_teardown_sandbox": "cd lib && uv run ad-migration teardown-sandbox [--yes]",
"ad_migration_reset": "cd lib && uv run ad-migration reset <stage> <fqn> [fqn ...] [--yes]",
"ad_migration_exclude_table": "cd lib && uv run ad-migration exclude-table <fqn> [--no-commit]",
"ad_migration_add_source_table": "cd lib && uv run ad-migration add-source-table <fqn> [--no-commit]"
```

- [ ] **Step 4: Commit**

```bash
git add repo-map.json
git commit -m "docs: update repo-map.json for ad-migration CLI"
```

---

## Task 9: Full test run and smoke test

- [ ] **Step 1: Run all CLI unit tests**

```bash
cd lib && uv run pytest ../tests/unit/cli/ -v
```

Expected: all tests pass.

- [ ] **Step 2: Run full test suite**

```bash
cd lib && uv run pytest
```

Expected: no regressions.

- [ ] **Step 3: Smoke test CLI help**

```bash
cd lib && uv run ad-migration --help
```

Expected: lists all 7 commands (`setup-source`, `setup-target`, `setup-sandbox`, `teardown-sandbox`, `reset`, `exclude-table`, `add-source-table`).

```bash
cd lib && uv run ad-migration reset --help
```

Expected: shows `STAGE` and `FQNS` arguments plus `--yes` and `--project-root` options.

- [ ] **Step 4: Final commit if anything was adjusted**

```bash
git add -p  # review and stage any final adjustments
git commit -m "fix: ad-migration CLI smoke test adjustments"
```
