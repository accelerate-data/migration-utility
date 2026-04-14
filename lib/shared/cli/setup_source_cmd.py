"""setup-source command — extract DDL from source database."""
from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path
from typing import Any

import typer

from shared.cli.env_check import require_source_vars
from shared.cli.git_ops import is_git_repo, stage_and_commit
from shared.cli.output import console, success, warn
from shared.init import run_scaffold_hooks, run_scaffold_project
from shared.setup_ddl_support.extract import run_extract

logger = logging.getLogger(__name__)


def setup_source(
    technology: str = typer.Option(..., "--technology", help="Source technology: sql_server or oracle"),
    schemas: str = typer.Option(..., "--schemas", help="Comma-separated schema names to extract (e.g. silver,gold)"),
    no_commit: bool = typer.Option(False, "--no-commit", help="Skip git commit after extraction"),
    project_root: Path | None = typer.Option(None, "--project-root"),
) -> None:
    """Validate source env vars and extract DDL from the source database.

    Run /init-ad-migration (plugin command) first to install the CLI, check prerequisites, and scaffold project files.
    """
    root = project_root if project_root is not None else Path.cwd()
    schema_list = [s.strip() for s in schemas.split(",") if s.strip()]

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


def _report_extract(result: dict[str, Any]) -> None:
    for key, label in (
        ("tables", "Tables"),
        ("procedures", "Procedures"),
        ("views", "Views"),
        ("functions", "Functions"),
    ):
        count = result.get(key, 0)
        if isinstance(count, list):
            count = len(count)
        success(f"{label:<15} {count}")
