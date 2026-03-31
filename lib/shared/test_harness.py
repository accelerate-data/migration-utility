"""Test harness CLI — sandbox lifecycle and scenario execution.

Reads manifest.json to determine technology, then routes to a
technology-specific backend (SQL Server via pyodbc first).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import typer

from shared.env_config import resolve_project_root
from shared.loader_io import read_manifest
from shared.sandbox import get_backend

logger = logging.getLogger(__name__)
app = typer.Typer(name="test-harness", no_args_is_help=True)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Load manifest.json via shared loader, converting errors to typer.Exit."""
    try:
        manifest = read_manifest(project_root)
    except ValueError as exc:
        typer.echo(json.dumps({"status": "error", "errors": [
            {"code": "MANIFEST_INVALID", "message": str(exc)}
        ]}))
        raise typer.Exit(code=1) from exc

    if "technology" not in manifest:
        typer.echo(json.dumps({"status": "error", "errors": [
            {"code": "MANIFEST_NOT_FOUND",
             "message": f"manifest.json not found or missing technology at {project_root}"}
        ]}))
        raise typer.Exit(code=1)
    return manifest


def _create_backend(manifest: dict[str, Any]) -> Any:
    """Instantiate the sandbox backend for the manifest's technology."""
    technology = manifest.get("technology", "sql_server")
    backend_cls = get_backend(technology)

    try:
        return backend_cls.from_env(manifest)
    except ValueError as exc:
        typer.echo(json.dumps({"status": "error", "errors": [
            {"code": "MISSING_ENV_VARS", "message": str(exc)}
        ]}))
        raise typer.Exit(code=1) from exc


@app.command()
def sandbox_up(
    run_id: str = typer.Option(..., help="UUID for the sandbox run"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Create a sandbox database and clone schema from the source."""
    logger.info("event=cli_invoked command=sandbox_up run_id=%s", run_id)
    root = resolve_project_root(Path(project_root))
    manifest = _load_manifest(root)
    backend = _create_backend(manifest)

    schemas = manifest.get("extracted_schemas", [])

    result = backend.sandbox_up(
        run_id=run_id,
        schemas=schemas,
        source_database=backend.database,
    )
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_up run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)


@app.command()
def sandbox_down(
    run_id: str = typer.Option(..., help="UUID of the sandbox to tear down"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Drop a sandbox database."""
    logger.info("event=cli_invoked command=sandbox_down run_id=%s", run_id)
    root = resolve_project_root(Path(project_root))
    manifest = _load_manifest(root)
    backend = _create_backend(manifest)

    result = backend.sandbox_down(run_id=run_id)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_down run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)


@app.command()
def execute(
    run_id: str = typer.Option(..., help="UUID of the sandbox"),
    scenario: str = typer.Option(..., help="Path to scenario JSON file"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Execute a test scenario in the sandbox and capture ground truth."""
    logger.info("event=cli_invoked command=execute run_id=%s scenario=%s", run_id, scenario)
    root = resolve_project_root(Path(project_root))
    manifest = _load_manifest(root)
    backend = _create_backend(manifest)

    scenario_path = Path(scenario)
    try:
        with scenario_path.open() as f:
            scenario_data = json.load(f)
    except FileNotFoundError:
        typer.echo(json.dumps({"status": "error", "errors": [
            {"code": "SCENARIO_NOT_FOUND",
             "message": f"Scenario file not found: {scenario}"}
        ]}))
        raise typer.Exit(code=1)

    result = backend.execute_scenario(run_id=run_id, scenario=scenario_data)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=execute run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
