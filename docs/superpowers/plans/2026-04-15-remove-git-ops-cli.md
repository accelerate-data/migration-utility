# Remove Git Ops from CLI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strip all git operations from the ad-migration CLI, delete git_ops.py, prune git-related tests, and replace scattered CLI wiki docs with a single CLI-Reference.md page.

**Architecture:** Pure removal — no new abstractions. Each task touches one file or one small group of related files. Tests are cleaned in the same task as the command they cover so the suite stays green at every checkpoint.

**Tech Stack:** Python, Typer, pytest, Markdown

---

## File map

| Action | Path |
|---|---|
| Delete | `lib/shared/cli/git_ops.py` |
| Delete | `tests/unit/cli/test_git_ops.py` |
| Modify | `lib/shared/cli/setup_source_cmd.py` |
| Modify | `lib/shared/cli/setup_target_cmd.py` |
| Modify | `lib/shared/cli/setup_sandbox_cmd.py` |
| Modify | `lib/shared/cli/teardown_sandbox_cmd.py` |
| Modify | `lib/shared/cli/reset_cmd.py` |
| Modify | `lib/shared/cli/exclude_table_cmd.py` |
| Modify | `lib/shared/cli/add_source_table_cmd.py` |
| Modify | `tests/unit/cli/test_setup_source_cmd.py` |
| Modify | `tests/unit/cli/test_setup_target_cmd.py` |
| Modify | `tests/unit/cli/test_sandbox_cmds.py` |
| Modify | `tests/unit/cli/test_pipeline_cmds.py` |
| Create | `docs/wiki/CLI-Reference.md` |
| Delete | `docs/wiki/Command-Setup-Source.md` |
| Modify | `docs/wiki/Stage-2-DDL-Extraction.md` |
| Modify | `docs/wiki/Stage-3-dbt-Scaffolding.md` |
| Modify | `docs/wiki/Testing-the-CLI.md` |
| Modify | `docs/wiki/Command-Reference.md` |

---

### Task 1: Delete git_ops.py and test_git_ops.py

**Files:**

- Delete: `lib/shared/cli/git_ops.py`
- Delete: `tests/unit/cli/test_git_ops.py`

- [ ] **Step 1: Delete both files**

```bash
rm lib/shared/cli/git_ops.py
rm tests/unit/cli/test_git_ops.py
```

- [ ] **Step 2: Verify the command files still import from git_ops (they do — not cleaned yet)**

```bash
grep -r "from shared.cli.git_ops" lib/shared/cli/
```

Expected: 7 files listed. This is expected — we clean them in subsequent tasks.

- [ ] **Step 3: Commit**

```bash
git add lib/shared/cli/git_ops.py tests/unit/cli/test_git_ops.py
git commit -m "refactor: delete git_ops.py and test_git_ops.py"
```

---

### Task 2: Strip setup_source_cmd.py and its tests

**Files:**

- Modify: `lib/shared/cli/setup_source_cmd.py`
- Modify: `tests/unit/cli/test_setup_source_cmd.py`

- [ ] **Step 1: Write the final setup_source_cmd.py**

```python
"""setup-source command — extract DDL from source database."""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import typer

from shared.cli.env_check import require_source_vars
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, success
from shared.init import run_scaffold_hooks, run_scaffold_project
from shared.setup_ddl_support.extract import run_extract, run_list_schemas

logger = logging.getLogger(__name__)
def setup_source(
    technology: str = typer.Option(..., "--technology", help="Source technology: sql_server or oracle"),
    schemas: str | None = typer.Option(None, "--schemas", help="Comma-separated schema names to extract (e.g. silver,gold)"),
    all_schemas: bool = typer.Option(False, "--all-schemas", help="Discover and extract all schemas in the database"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt (only applies to --all-schemas)"),
    project_root: Path | None = typer.Option(None, "--project-root"),
) -> None:
    """Validate source env vars and extract DDL from the source database.

    Run /init-ad-migration (plugin command) first to install the CLI, check prerequisites, and scaffold project files.
    """
    root = project_root if project_root is not None else Path.cwd()

    if schemas and all_schemas:
        error("--schemas and --all-schemas are mutually exclusive. Use one or the other.")
        raise typer.Exit(code=1)
    if not schemas and not all_schemas:
        error("Provide --schemas <list> or --all-schemas to extract every schema in the database.")
        raise typer.Exit(code=1)

    require_source_vars(technology)
    _check_source_prereqs(technology)

    scaffold_result = run_scaffold_project(root, technology)
    logger.info(
        "event=scaffold_project status=success component=setup_source_cmd files_created=%s files_updated=%s",
        scaffold_result.files_created,
        scaffold_result.files_updated,
    )

    hooks_result = run_scaffold_hooks(root, technology)
    logger.info(
        "event=scaffold_hooks status=success component=setup_source_cmd hook_created=%s",
        hooks_result.hook_created,
    )

    database = os.environ.get("MSSQL_DB") if technology == "sql_server" else None

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

    console.print(f"Extracting DDL from schemas: [bold]{', '.join(schema_list)}[/bold]")
    with console.status("Extracting..."):
        with cli_error_handler("extracting DDL from source database"):
            result = run_extract(root, database, schema_list)

    _report_extract(result)
def _check_source_prereqs(technology: str) -> None:
    if technology == "sql_server":
        if sys.platform == "darwin":
            result = subprocess.run(
                ["brew", "list", "--formula", "freetds"],
                capture_output=True,
            )
            if result.returncode != 0:
                console.print("[red]✗[/red] freetds not installed. Run: brew install freetds")
                raise typer.Exit(code=1)
            success("freetds installed")
        else:
            if shutil.which("tsql") is None:
                console.print("[red]✗[/red] FreeTDS not found. Install via your package manager (e.g. apt-get install freetds-dev).")
                raise typer.Exit(code=1)
            success("freetds available")
    elif technology == "oracle":
        sqlcl_bin = shutil.which("sql") or shutil.which("sqlcl")
        if sqlcl_bin is None:
            console.print("[red]✗[/red] sqlcl not found. Install SQLcl and ensure it is on PATH.")
            raise typer.Exit(code=1)
        success(f"sqlcl available ({sqlcl_bin})")
        r = subprocess.run(["java", "-version"], capture_output=True)
        if r.returncode != 0:
            console.print("[red]✗[/red] java not found. Install Java 11+.")
            raise typer.Exit(code=1)
        success("java available")
def _report_extract(result: dict[str, Any]) -> None:
    rows = []
    for key, label in (
        ("tables", "Tables"),
        ("procedures", "Procedures"),
        ("views", "Views"),
        ("functions", "Functions"),
    ):
        count = result.get(key, 0)
        if isinstance(count, list):
            count = len(count)
        rows.append((label, str(count)))
    print_table("Extraction Summary", rows, columns=("Object Type", "Count"))
```

- [ ] **Step 2: Write the final test_setup_source_cmd.py**

```python
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
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
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
def test_setup_source_all_schemas_requires_confirmation(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    list_out = {"schemas": [{"schema": "silver"}, {"schema": "gold"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out),
        patch("shared.cli.setup_source_cmd.run_extract") as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas",
             "--project-root", str(tmp_path)],
            input="n\n",
        )

    assert result.exit_code == 0
    mock_extract.assert_not_called()
def test_setup_source_all_schemas_yes_flag_skips_confirmation(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    list_out = {"schemas": [{"schema": "silver"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas", "--yes",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_called_once()
def test_setup_source_all_schemas_discovers_and_extracts(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "AdventureWorks2022")
    monkeypatch.setenv("SA_PASSWORD", "secret")

    list_out = {"schemas": [{"schema": "silver"}, {"schema": "gold"}, {"schema": "bronze"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out) as mock_list,
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas", "--yes",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_list.assert_called_once()
    mock_extract.assert_called_once_with(tmp_path, "AdventureWorks2022", ["silver", "gold", "bronze"])
def test_setup_source_all_schemas_prints_discovered_schemas(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    list_out = {"schemas": [{"schema": "silver"}, {"schema": "gold"}]}

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value=list_out),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT),
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas", "--yes",
             "--project-root", str(tmp_path)],
        )

    assert "silver" in result.output
    assert "gold" in result.output
def test_setup_source_all_schemas_empty_discovery_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_list_schemas", return_value={"schemas": []}),
        patch("shared.cli.setup_source_cmd.run_extract") as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--all-schemas",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 1
    mock_extract.assert_not_called()
def test_setup_source_all_schemas_and_schemas_are_mutually_exclusive(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    result = runner.invoke(
        app,
        ["setup-source", "--technology", "sql_server", "--schemas", "silver", "--all-schemas",
         "--project-root", str(tmp_path)],
    )
    assert result.exit_code == 1
def test_setup_source_neither_schemas_nor_all_schemas_exits_1(tmp_path, monkeypatch):
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_DB", "db")
    monkeypatch.setenv("SA_PASSWORD", "pw")

    with patch("shared.cli.setup_source_cmd._check_source_prereqs"):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "sql_server", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 1
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
def test_setup_source_oracle_passes_none_database(tmp_path, monkeypatch):
    monkeypatch.setenv("ORACLE_HOST", "localhost")
    monkeypatch.setenv("ORACLE_PORT", "1521")
    monkeypatch.setenv("ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("ORACLE_USER", "sh")
    monkeypatch.setenv("ORACLE_PASSWORD", "pw")

    with (
        patch("shared.cli.setup_source_cmd._check_source_prereqs"),
        patch("shared.cli.setup_source_cmd.run_scaffold_project", return_value=_SCAFFOLD_OUT),
        patch("shared.cli.setup_source_cmd.run_scaffold_hooks", return_value=_HOOKS_OUT),
        patch("shared.cli.setup_source_cmd.run_extract", return_value=_EXTRACT_OUT) as mock_extract,
    ):
        result = runner.invoke(
            app,
            ["setup-source", "--technology", "oracle", "--schemas", "sh",
             "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_extract.assert_called_once_with(tmp_path, None, ["sh"])
```

- [ ] **Step 3: Run the tests**

```bash
python -m pytest tests/unit/cli/test_setup_source_cmd.py -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add lib/shared/cli/setup_source_cmd.py tests/unit/cli/test_setup_source_cmd.py
git commit -m "refactor: remove git ops from setup-source"
```

---

### Task 3: Strip setup_target_cmd.py and its tests

**Files:**

- Modify: `lib/shared/cli/setup_target_cmd.py`
- Modify: `tests/unit/cli/test_setup_target_cmd.py`

- [ ] **Step 1: Write the final setup_target_cmd.py**

```python
"""setup-target command — configure target runtime and scaffold dbt."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.cli.env_check import require_target_vars
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, success
from shared.target_setup import run_setup_target, write_target_runtime_from_env

logger = logging.getLogger(__name__)
def setup_target(
    technology: str = typer.Option(..., "--technology", help="Target technology: fabric, snowflake, or duckdb"),
    source_schema: str = typer.Option("bronze", "--source-schema", help="Target source schema (default: bronze)"),
    project_root: Path | None = typer.Option(None, "--project-root"),
) -> None:
    """Configure target runtime, scaffold dbt project, and generate sources.yml."""
    root = project_root if project_root is not None else Path.cwd()

    require_target_vars(technology)

    console.print(f"\nWriting runtime.target for [bold]{technology}[/bold]...")
    try:
        write_target_runtime_from_env(root, technology, source_schema)
    except ValueError as exc:
        error(str(exc))
        raise typer.Exit(code=1) from exc
    success(f"runtime.target written (source_schema={source_schema})")

    console.print("Running target setup...")
    with console.status("Scaffolding dbt project and generating sources.yml..."):
        with cli_error_handler("running target setup"):
            try:
                result = run_setup_target(root)
            except ValueError as exc:
                error(str(exc))
                raise typer.Exit(code=1) from exc

    for f in result.files:
        success(f"created  {f}")
    if result.sources_path:
        success(f"sources  {result.sources_path}")
    console.print(
        f"\n  tables in sources.yml: {len(result.desired_tables)} desired, "
        f"{len(result.created_tables)} new, {len(result.existing_tables)} existing"
    )
```

- [ ] **Step 2: Write the final test_setup_target_cmd.py**

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
    ):
        result = runner.invoke(
            app,
            ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "snowflake", "bronze")
    mock_setup.assert_called_once_with(tmp_path)
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
            ["setup-target", "--technology", "snowflake", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 1
```

- [ ] **Step 3: Run the tests**

```bash
python -m pytest tests/unit/cli/test_setup_target_cmd.py -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add lib/shared/cli/setup_target_cmd.py tests/unit/cli/test_setup_target_cmd.py
git commit -m "refactor: remove git ops from setup-target"
```

---

### Task 4: Strip setup_sandbox_cmd.py, teardown_sandbox_cmd.py, and their tests

**Files:**

- Modify: `lib/shared/cli/setup_sandbox_cmd.py`
- Modify: `lib/shared/cli/teardown_sandbox_cmd.py`
- Modify: `tests/unit/cli/test_sandbox_cmds.py`

- [ ] **Step 1: Write the final setup_sandbox_cmd.py**

```python
"""setup-sandbox command — provision sandbox database from manifest config."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, success
from shared.loader_io import write_manifest_sandbox
from shared.runtime_config import get_extracted_schemas
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
    success("Sandbox ready.")
```

- [ ] **Step 2: Write the final teardown_sandbox_cmd.py**

```python
"""teardown-sandbox command — drop sandbox database from manifest config."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, success
from shared.loader_io import clear_manifest_sandbox
from shared.runtime_config import get_sandbox_name
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
def _get_sandbox_name(manifest: dict[str, Any]) -> str | None:
    """Return active sandbox database name from manifest."""
    return get_sandbox_name(manifest)
def teardown_sandbox(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Tear down sandbox schema from manifest runtime.sandbox configuration."""
    root = project_root if project_root is not None else Path.cwd()

    manifest = _load_manifest(root)

    sandbox_db = _get_sandbox_name(manifest)
    if not sandbox_db:
        error("No sandbox database name found in manifest.json. Run setup-sandbox first.")
        raise typer.Exit(code=1)

    if not yes:
        confirmed = typer.confirm("Tear down sandbox database? This action cannot be undone.")
        if not confirmed:
            console.print("Aborted.")
            raise typer.Exit(code=0)

    backend = _create_backend(manifest)

    console.print(f"Tearing down sandbox database: [bold]{sandbox_db}[/bold]...")
    with console.status("Running sandbox_down..."):
        with cli_error_handler("tearing down sandbox database"):
            result = backend.sandbox_down(sandbox_db)

    logger.info(
        "event=sandbox_down status=%s sandbox_database=%s",
        result.status,
        result.sandbox_database,
    )

    if result.status == "ok":
        clear_manifest_sandbox(root)
        print_table(
            "Teardown Summary",
            [("Database", result.sandbox_database), ("Status", result.status)],
            columns=("", ""),
        )
        success("Sandbox torn down.")
    else:
        error(f"Sandbox teardown failed: {result.status}")
        for entry in (result.errors or []):
            error(f"[{entry.code}] {entry.message}")
        raise typer.Exit(code=1)
```

- [ ] **Step 3: Write the final test_sandbox_cmds.py**

```python
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

import shared.cli.error_handler as _err_mod
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

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={"runtime": {"sandbox": {}}}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--project-root", str(tmp_path)], input="n\n")

    assert result.exit_code == 0
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
def test_teardown_sandbox_no_sandbox_exits_1(tmp_path):
    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value=None),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
def test_teardown_sandbox_error_exits_nonzero(tmp_path):
    from shared.output_models.sandbox import SandboxDownOutput
    error_out = SandboxDownOutput(sandbox_database="__test_abc123", status="error")
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = error_out

    with (
        patch("shared.cli.teardown_sandbox_cmd._load_manifest", return_value={}),
        patch("shared.cli.teardown_sandbox_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.teardown_sandbox_cmd._get_sandbox_name", return_value="__test_abc123"),
    ):
        result = runner.invoke(app, ["teardown-sandbox", "--yes", "--project-root", str(tmp_path)])

    assert result.exit_code == 1
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

- [ ] **Step 4: Run the tests**

```bash
python -m pytest tests/unit/cli/test_sandbox_cmds.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/shared/cli/setup_sandbox_cmd.py lib/shared/cli/teardown_sandbox_cmd.py tests/unit/cli/test_sandbox_cmds.py
git commit -m "refactor: remove git ops from setup-sandbox and teardown-sandbox"
```

---

### Task 5: Strip reset_cmd.py and its tests

**Files:**

- Modify: `lib/shared/cli/reset_cmd.py`
- Modify: `tests/unit/cli/test_pipeline_cmds.py` (reset section only — exclude/add-source sections cleaned in Task 6)

- [ ] **Step 1: Write the final reset_cmd.py**

```python
"""reset command — reset pipeline state for a given stage and objects."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import typer

from shared.dry_run_core import RESET_GLOBAL_MANIFEST_SECTIONS, RESET_GLOBAL_PATHS, RESETTABLE_STAGES, run_reset_migration
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, print_table, success, warn
from shared.loader_io import clear_manifest_sandbox
from shared.runtime_config import get_sandbox_name
from shared.test_harness_support.manifest import _create_backend as _th_create_backend
from shared.test_harness_support.manifest import _load_manifest as _th_load_manifest

logger = logging.getLogger(__name__)

_GLOBAL_BLAST_RADIUS = (
    "Directories:  " + ", ".join(RESET_GLOBAL_PATHS) + "\n"
    "Manifest:     " + ", ".join(RESET_GLOBAL_MANIFEST_SECTIONS)
)
def _load_manifest(project_root: Path) -> dict[str, Any]:
    return _th_load_manifest(project_root)
def _create_backend(manifest: dict[str, Any]) -> Any:
    return _th_create_backend(manifest)
def _get_sandbox_name(manifest: dict[str, Any]) -> str | None:
    return get_sandbox_name(manifest)
def _manual_cleanup_instructions(sandbox_db: str) -> str:
    return (
        f"  SQL Server:  DROP DATABASE [{sandbox_db}]\n"
        f"  Oracle:      DROP USER {sandbox_db} CASCADE"
    )
def _teardown_sandbox_if_configured(root: Path) -> None:
    """Tear down sandbox if configured. On failure, warns and continues — never blocks the reset."""
    try:
        manifest = _load_manifest(root)
    except Exception:
        return

    sandbox_db = _get_sandbox_name(manifest)
    if not sandbox_db:
        return

    console.print(f"Sandbox configured: [bold]{sandbox_db}[/bold] — tearing down first...")
    logger.info("event=global_reset_sandbox_teardown component=reset_cmd sandbox=%s", sandbox_db)

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
        pass
    except Exception as exc:
        logger.warning(
            "event=global_reset_sandbox_teardown_failed component=reset_cmd sandbox=%s error=%s",
            sandbox_db, exc,
        )

    if not teardown_ok:
        warn(
            f"Sandbox teardown failed for [bold]{sandbox_db}[/bold] — continuing reset.\n"
            f"Clean up the database manually:\n{_manual_cleanup_instructions(sandbox_db)}"
        )

    clear_manifest_sandbox(root)
    if teardown_ok:
        logger.info("event=global_reset_sandbox_teardown_ok component=reset_cmd sandbox=%s", sandbox_db)
def reset(
    stage: str = typer.Argument(..., help="Pipeline stage to reset (scope|profile|generate-tests|refactor|all)"),
    fqns: list[str] = typer.Argument(default=None, help="Fully-qualified table names (not used for 'all')"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Reset pipeline state for the given stage and objects.

    Use 'all' to wipe the full migration state and return the project to a
    clean post-init state ready for setup-source. Any configured sandbox is
    torn down first.
    """
    root = project_root if project_root is not None else Path.cwd()

    if stage == "all":
        if fqns:
            error("Global reset ('all') does not accept table arguments.")
            raise typer.Exit(code=1)

        if not yes:
            console.print(f"[bold red]Global reset[/bold red] will permanently delete:\n{_GLOBAL_BLAST_RADIUS}")
            confirmed = typer.confirm("This cannot be undone. Continue?", default=False)
            if not confirmed:
                console.print("Aborted.")
                return

        logger.info("event=global_reset_start component=reset_cmd stage=all")

        _teardown_sandbox_if_configured(root)

        result = run_reset_migration(root, "all", [])

        logger.info(
            "event=global_reset_complete component=reset_cmd deleted_paths=%s missing_paths=%s cleared=%s",
            result.deleted_paths, result.missing_paths, result.cleared_manifest_sections,
        )

        print_table(
            "Global Reset Summary",
            [
                ("Deleted", ", ".join(result.deleted_paths) if result.deleted_paths else "—"),
                ("Missing", ", ".join(result.missing_paths) if result.missing_paths else "—"),
                ("Manifest cleared", ", ".join(result.cleared_manifest_sections) if result.cleared_manifest_sections else "—"),
            ],
            columns=("", ""),
        )
        success("Project reset to post-init state. Run setup-source to restart the pipeline.")
        return

    if stage not in RESETTABLE_STAGES:
        error(f"Invalid stage {stage!r}. Must be one of: {', '.join(sorted(RESETTABLE_STAGES))} or 'all'")
        raise typer.Exit(code=1)

    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    if not yes:
        fqn_list = ", ".join(fqns)
        confirmed = typer.confirm(f"Reset stage '{stage}' for: {fqn_list}?", default=False)
        if not confirmed:
            console.print("Aborted.")
            return

    logger.info("event=reset_start component=reset_cmd stage=%s fqns=%s", stage, fqns)

    result = run_reset_migration(root, stage, list(fqns))

    logger.info(
        "event=reset_complete component=reset_cmd stage=%s reset=%s noop=%s blocked=%s not_found=%s",
        stage, result.reset, result.noop, result.blocked, result.not_found,
    )

    print_table(
        "Reset Summary",
        [
            ("Reset", str(len(result.reset))),
            ("No-op", str(len(result.noop))),
            ("Blocked", str(len(result.blocked))),
            ("Not found", str(len(result.not_found))),
        ],
        columns=("Category", "Count"),
    )

    if result.blocked:
        error(f"Blocked: {', '.join(result.blocked)}")
    if result.not_found:
        error(f"Not found: {', '.join(result.not_found)}")
    if result.blocked or result.not_found:
        raise typer.Exit(code=1)
```

- [ ] **Step 2: Write the final test_pipeline_cmds.py**

```python
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.dry_run import DryRunOutput, ExcludeOutput, ObjectReadiness, ReadinessDetail, ResetMigrationOutput
from shared.output_models.sandbox import SandboxDownOutput

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
    assert result.exit_code == 0, result.output
def test_reset_rejects_invalid_stage(tmp_path):
    result = runner.invoke(app, ["reset", "invalid-stage", "silver.Foo", "--yes",
                                  "--project-root", str(tmp_path)])
    assert result.exit_code == 1
def test_reset_exits_1_on_not_found(tmp_path):
    _write_manifest(tmp_path)
    out = ResetMigrationOutput(stage="scope", targets=[], reset=[], noop=[], blocked=[], not_found=["silver.Missing"])
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=out):
        result = runner.invoke(app, ["reset", "scope", "silver.Missing", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1
def test_reset_exits_1_on_blocked(tmp_path):
    _write_manifest(tmp_path)
    out = ResetMigrationOutput(stage="scope", targets=[], reset=[], noop=[], blocked=["silver.Locked"], not_found=[])
    with patch("shared.cli.reset_cmd.run_reset_migration", return_value=out):
        result = runner.invoke(app, ["reset", "scope", "silver.Locked", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1
# ── reset all (global) ───────────────────────────────────────────────────────

_GLOBAL_RESET_OUT = ResetMigrationOutput(
    stage="all",
    targets=[],
    reset=[],
    noop=[],
    blocked=[],
    not_found=[],
    deleted_paths=["catalog", "ddl", ".staging"],
    missing_paths=["test-specs", "dbt"],
    cleared_manifest_sections=["runtime.source", "runtime.target"],
)
def test_reset_all_no_sandbox_delegates_to_core(tmp_path):
    _write_manifest(tmp_path)
    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value=None),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [])
def test_reset_all_with_sandbox_tears_down_before_reset(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="__test_abc", status="ok")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_backend.sandbox_down.assert_called_once_with("__test_abc")
    mock_reset.assert_called_once_with(tmp_path, "all", [])
def test_reset_all_sandbox_teardown_failure_warns_and_continues(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="__test_abc", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT) as mock_reset,
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 0, result.output
    mock_reset.assert_called_once_with(tmp_path, "all", [])
def test_reset_all_sandbox_teardown_failure_prints_manual_instructions(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="__test_abc", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox"),
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT),
    ):
        result = runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    assert "__test_abc" in result.output
def test_reset_all_sandbox_teardown_failure_clears_manifest_sandbox(tmp_path):
    _write_manifest(tmp_path)
    mock_backend = MagicMock()
    mock_backend.sandbox_down.return_value = SandboxDownOutput(sandbox_database="__test_abc", status="error")

    with (
        patch("shared.cli.reset_cmd._load_manifest", return_value={}),
        patch("shared.cli.reset_cmd._get_sandbox_name", return_value="__test_abc"),
        patch("shared.cli.reset_cmd._create_backend", return_value=mock_backend),
        patch("shared.cli.reset_cmd.clear_manifest_sandbox") as mock_clear,
        patch("shared.cli.reset_cmd.run_reset_migration", return_value=_GLOBAL_RESET_OUT),
    ):
        runner.invoke(app, ["reset", "all", "--yes", "--project-root", str(tmp_path)])
    mock_clear.assert_called_once()
def test_reset_all_aborts_without_confirmation(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.reset_cmd.run_reset_migration") as mock_reset:
        result = runner.invoke(app, ["reset", "all", "--project-root", str(tmp_path)], input="n\n")
    mock_reset.assert_not_called()
    assert result.exit_code == 0, result.output
def test_reset_all_rejects_fqn_arguments(tmp_path):
    _write_manifest(tmp_path)
    result = runner.invoke(app, ["reset", "all", "silver.Foo", "--yes", "--project-root", str(tmp_path)])
    assert result.exit_code == 1
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
# ── exclude-table ────────────────────────────────────────────────────────────

_EXCLUDE_OUT = ExcludeOutput(marked=["silver.AuditLog"], not_found=[])
def test_exclude_table_marks_tables(tmp_path):
    _write_manifest(tmp_path)
    with patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT) as mock_exclude:
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--project-root", str(tmp_path)],
        )
    assert result.exit_code == 0, result.output
    mock_exclude.assert_called_once()
# ── add-source-table ─────────────────────────────────────────────────────────

from shared.output_models.catalog_writer import WriteSourceOutput
def test_add_source_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        object=ObjectReadiness(object="silver.audittest", ready=True, reason="scope complete"),
    )
    write_out = WriteSourceOutput(written="catalog/tables/silver.audittest.json", is_source=True, status="ok")

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once()
def test_add_source_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        object=ObjectReadiness(object="silver.audittest", ready=False, reason="scope not complete"),
    )

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

- [ ] **Step 3: Run the tests**

```bash
python -m pytest tests/unit/cli/test_pipeline_cmds.py -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add lib/shared/cli/reset_cmd.py tests/unit/cli/test_pipeline_cmds.py
git commit -m "refactor: remove git ops from reset, exclude-table, add-source-table"
```

---

### Task 6: Strip exclude_table_cmd.py and add_source_table_cmd.py

**Files:**

- Modify: `lib/shared/cli/exclude_table_cmd.py`
- Modify: `lib/shared/cli/add_source_table_cmd.py`

- [ ] **Step 1: Write the final exclude_table_cmd.py**

```python
"""exclude-table command — mark tables as excluded from migration."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, success
from shared.dry_run_core import run_exclude

logger = logging.getLogger(__name__)
def exclude_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to exclude"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Mark one or more source tables as excluded from migration."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    root = project_root if project_root is not None else Path.cwd()

    logger.info(
        "event=exclude_table_start component=exclude_table_cmd operation=exclude_table fqns=%s",
        fqns,
    )

    with cli_error_handler("excluding tables from catalog"):
        result = run_exclude(root, list(fqns))

    logger.info(
        "event=exclude_table_complete component=exclude_table_cmd operation=exclude_table "
        "marked=%s not_found=%s",
        result.marked,
        result.not_found,
    )

    if result.marked:
        success(f"Excluded ({len(result.marked)}): {', '.join(result.marked)}")
    if result.not_found:
        error(f"Not found ({len(result.not_found)}): {', '.join(result.not_found)}")
```

- [ ] **Step 2: Write the final add_source_table_cmd.py**

```python
"""add-source-table command — add source tables to the migration catalog."""
from __future__ import annotations

import logging
from pathlib import Path

import typer

from shared.catalog_writer import run_write_source
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import success, warn
from shared.dry_run_core import run_ready
from shared.loader_data import CatalogFileMissingError
from shared.output_models.dry_run import DryRunOutput

logger = logging.getLogger(__name__)
def add_source_table(
    fqns: list[str] = typer.Argument(default=None, help="One or more fully-qualified table names to add"),
    project_root: Path | None = typer.Option(None, "--project-root", help="Project root directory"),
) -> None:
    """Add one or more source tables to the migration catalog."""
    if not fqns:
        raise typer.BadParameter("At least one FQN is required.", param_hint="fqns")

    root = project_root if project_root is not None else Path.cwd()

    logger.info(
        "event=add_source_table_start component=add_source_table_cmd operation=add_source_table fqns=%s",
        fqns,
    )

    for fqn in fqns:
        ready_result: DryRunOutput = run_ready(root, "scope", fqn)

        if ready_result.object is not None:
            is_ready = ready_result.object.ready
            reason = ready_result.object.reason
        elif ready_result.project is not None:
            is_ready = ready_result.project.ready
            reason = ready_result.project.reason
        else:
            raise AssertionError(f"run_ready returned neither object nor project payload for {fqn}")

        if not is_ready:
            warn(f"skipped  {fqn} — {reason}")
            logger.info(
                "event=add_source_table_skip component=add_source_table_cmd "
                "operation=add_source_table fqn=%s reason=%s",
                fqn,
                reason,
            )
            continue

        try:
            with cli_error_handler(f"marking {fqn} as source table"):
                run_write_source(root, fqn, value=True)
            success(f"source   {fqn} → is_source: true")
            logger.info(
                "event=add_source_table_written component=add_source_table_cmd "
                "operation=add_source_table fqn=%s status=success",
                fqn,
            )
        except typer.Exit:
            raise
        except CatalogFileMissingError:
            warn(f"missing  {fqn} (no catalog file — run setup-source first)")
        except ValueError as exc:
            warn(f"skipped  {fqn} — {exc}")
```

- [ ] **Step 3: Run the full CLI suite**

```bash
python -m pytest tests/unit/cli/ -v
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add lib/shared/cli/exclude_table_cmd.py lib/shared/cli/add_source_table_cmd.py
git commit -m "refactor: remove git ops from exclude-table and add-source-table"
```

---

### Task 7: Run full lib suite

**Files:** none

- [ ] **Step 1: Run all lib tests**

```bash
cd lib && uv run pytest -q
```

Expected: all tests pass, no errors.

- [ ] **Step 2: Verify no remaining git_ops imports**

```bash
grep -r "git_ops\|stage_and_commit\|is_git_repo\|git_push\|no.commit\|no_commit" lib/shared/cli/ tests/unit/cli/
```

Expected: no matches.

---

### Task 8: Create CLI-Reference.md

**Files:**

- Create: `docs/wiki/CLI-Reference.md`
- Delete: `docs/wiki/Command-Setup-Source.md`

- [ ] **Step 1: Verify the file exists**

The file `docs/wiki/CLI-Reference.md` has already been written. It contains these sections in order: Git workflow (recommended branch → CLI → commit → merge flow), Installation, Setup commands (setup-source, setup-target, setup-sandbox, teardown-sandbox each with options/env vars/files written), Pipeline state commands (reset, exclude-table, add-source-table), Exit codes.

Verify it is present:

```bash
head -5 docs/wiki/CLI-Reference.md
```

Expected: first line is `# CLI Reference`.

- [ ] **Step 2: Delete the old per-command doc**

```bash
rm docs/wiki/Command-Setup-Source.md
```

- [ ] **Step 3: Lint the new doc**

```bash
markdownlint docs/wiki/CLI-Reference.md
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add docs/wiki/CLI-Reference.md docs/wiki/Command-Setup-Source.md
git commit -m "docs: add CLI-Reference.md, delete Command-Setup-Source.md"
```

---

### Task 9: Update remaining wiki docs

**Files:**

- Modify: `docs/wiki/Stage-2-DDL-Extraction.md`
- Modify: `docs/wiki/Stage-3-dbt-Scaffolding.md`
- Modify: `docs/wiki/Testing-the-CLI.md`
- Modify: `docs/wiki/Command-Reference.md`

- [ ] **Step 1: Update Stage-2-DDL-Extraction.md**

Remove the `--no-commit` row from the options table and remove step 5 from the "How it works" list. The final options table:

```markdown
| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `sql_server` or `oracle` |
| `--schemas` | yes | Comma-separated list of schemas to extract |
| `--project-root` | no | Defaults to current working directory |
```

The final "How it works" list:

```markdown
1. Validates required environment variables (exits 1 with a list of missing vars if absent)
2. Runs extraction via `run_extract`
3. Writes `manifest.json`, `ddl/`, and `catalog/`
4. Runs AST enrichment
```

- [ ] **Step 2: Update Stage-3-dbt-Scaffolding.md**

Remove the `--no-commit` row from the options table. The final options table:

```markdown
| Option | Required | Description |
|---|---|---|
| `--technology` | yes | `fabric`, `snowflake`, or `duckdb` |
| `--source-schema` | no | Source schema for `sources.yml` (defaults to `bronze`) |
```

- [ ] **Step 3: Update Testing-the-CLI.md**

Make these three changes to `docs/wiki/Testing-the-CLI.md`:

1. Under the `setup-source` per-command block, remove the two lines that test `--no-commit` (the comment `# Verify --no-commit skips git commit`, the command with `--no-commit`, and the `git status` line).

2. In the `exclude-table / add-source-table` block, remove `--no-commit` from both commands so they read `ad-migration exclude-table silver.DimCurrency` and `ad-migration add-source-table silver.DimGeography`.

3. Remove the entire `## CI integration` section at the bottom of the file (it describes `--no-commit` and `--yes` as CI-safe — no longer accurate).

- [ ] **Step 4: Update Command-Reference.md**

In `docs/wiki/Command-Reference.md`, make these changes to the `## ad-migration CLI` section:

1. After the install/dev-usage blocks, add one sentence: `See [[CLI Reference]] for the full command reference, options, environment variables, and recommended git workflow.`

2. Remove the "Git workflow scripts" section entirely (the table listing `scripts/commit.sh`, `scripts/commit-push-pr.sh`, `scripts/cleanup-worktrees.sh`).

3. Remove the note at the bottom: "Successful items are committed as they complete."

- [ ] **Step 5: Lint all modified docs**

```bash
markdownlint docs/wiki/Stage-2-DDL-Extraction.md docs/wiki/Stage-3-dbt-Scaffolding.md docs/wiki/Testing-the-CLI.md docs/wiki/Command-Reference.md
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add docs/wiki/Stage-2-DDL-Extraction.md docs/wiki/Stage-3-dbt-Scaffolding.md docs/wiki/Testing-the-CLI.md docs/wiki/Command-Reference.md
git commit -m "docs: remove --no-commit references, point to CLI-Reference.md"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Covered by |
|---|---|
| Delete git_ops.py | Task 1 |
| Delete test_git_ops.py | Task 1 |
| Remove git ops from 7 command files | Tasks 2, 3, 4, 5, 6 |
| Prune git tests from 4 test files | Tasks 2, 3, 4, 5 |
| Create CLI-Reference.md with git workflow section | Task 8 |
| Delete Command-Setup-Source.md | Task 8 |
| Update Stage-2, Stage-3, Testing-the-CLI, Command-Reference | Task 9 |

**Placeholder scan:** No TBDs, no TODOs — all code blocks are complete.

**Type consistency:** No new types introduced — this is purely a removal.
