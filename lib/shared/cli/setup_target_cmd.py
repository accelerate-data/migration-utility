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

