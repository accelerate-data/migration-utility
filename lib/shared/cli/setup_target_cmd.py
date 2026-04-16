"""setup-target command — configure target runtime and scaffold dbt."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import typer

from shared.cli.env_check import require_target_vars
from shared.cli.error_handler import cli_error_handler
from shared.cli.output import console, error, remind_review_and_commit, success
from shared.runtime_config import get_runtime_role
from shared.target_setup import run_setup_target, write_target_runtime_from_env

logger = logging.getLogger(__name__)


def _get_target_technology(root: Path) -> str:
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        error("manifest.json not found. Run init-ad-migration first.")
        raise typer.Exit(code=1)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        error("manifest.json is not valid JSON. Run init-ad-migration again.")
        raise typer.Exit(code=1)

    target_role = get_runtime_role(manifest, "target")
    if target_role is None:
        error("manifest.json is missing runtime.target. Run init-ad-migration first.")
        raise typer.Exit(code=1)
    return target_role.technology


def setup_target(
    source_schema: str = typer.Option("bronze", "--source-schema", help="Target source schema (default: bronze)"),
    project_root: Path | None = typer.Option(None, "--project-root"),
) -> None:
    """Configure target runtime, scaffold dbt project, and generate sources.yml."""
    root = project_root if project_root is not None else Path.cwd()
    technology = _get_target_technology(root)

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
    for seed_file in result.seed_files:
        success(f"seed     {seed_file}")
    console.print(
        f"\n  tables in sources.yml: {len(result.desired_tables)} desired, "
        f"{len(result.created_tables)} new, {len(result.existing_tables)} existing"
    )
    if result.seed_files:
        seed_status = "materialized" if result.dbt_seed_ran else "not materialized"
        console.print(f"  seed files: {len(result.seed_files)} exported, {seed_status}")
    remind_review_and_commit()
