"""setup-source command — extract DDL from source database."""
from __future__ import annotations

import json
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
from shared.runtime_config import get_runtime_role
from shared.setup_ddl_support.extract import run_extract, run_list_schemas

logger = logging.getLogger(__name__)


def _get_source_technology(root: Path) -> str:
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        error("manifest.json not found. Run init-ad-migration first.")
        raise typer.Exit(code=1)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        error("manifest.json is not valid JSON. Run init-ad-migration again.")
        raise typer.Exit(code=1)

    source_role = get_runtime_role(manifest, "source")
    if source_role is None:
        error("manifest.json is missing runtime.source. Run init-ad-migration first.")
        raise typer.Exit(code=1)
    return source_role.technology


def setup_source(
    schemas: str | None = typer.Option(None, "--schemas", help="Comma-separated schema names to extract (e.g. silver,gold)"),
    all_schemas: bool = typer.Option(False, "--all-schemas", help="Discover and extract all schemas in the database"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt (only applies to --all-schemas)"),
    project_root: Path | None = typer.Option(None, "--project-root"),
) -> None:
    """Validate source env vars and extract DDL from the source database.

    Run /init-ad-migration (plugin command) first to install the CLI, check prerequisites, and scaffold project files.
    """
    root = project_root if project_root is not None else Path.cwd()
    technology = _get_source_technology(root)

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

    database = os.environ.get("SOURCE_MSSQL_DB") if technology == "sql_server" else None

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
