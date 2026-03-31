"""Test harness CLI — sandbox lifecycle and scenario execution.

Reads manifest.json to determine technology, then routes to a
technology-specific backend (SQL Server via pyodbc first).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, NoReturn

import typer

from shared.env_config import resolve_project_root
from shared.loader_io import read_manifest
from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend

logger = logging.getLogger(__name__)
app = typer.Typer(name="test-harness", no_args_is_help=True)


def _load_manifest(project_root: Path) -> dict[str, Any]:
    """Load manifest.json via shared loader, converting errors to typer.Exit."""
    try:
        manifest = read_manifest(project_root)
    except ValueError as exc:
        _error_exit("MANIFEST_INVALID", str(exc), exc)

    if "technology" not in manifest:
        _error_exit(
            "MISSING_TECHNOLOGY",
            f"manifest.json is missing required 'technology' key at {project_root}",
        )
    return manifest


def _create_backend(manifest: dict[str, Any]) -> SandboxBackend:
    """Instantiate the sandbox backend for the manifest's technology."""
    technology = manifest["technology"]
    backend_cls = get_backend(technology)

    try:
        return backend_cls.from_env(manifest)
    except ValueError as exc:
        _error_exit("MISSING_ENV_VARS", str(exc), exc)


def _error_exit(code: str, message: str, exc: Exception | None = None) -> NoReturn:
    """Emit a JSON error and raise typer.Exit."""
    typer.echo(json.dumps({"status": "error", "errors": [
        {"code": code, "message": message}
    ]}))
    if exc is not None:
        raise typer.Exit(code=1) from exc
    raise typer.Exit(code=1)


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
    if not schemas:
        _error_exit(
            "NO_SCHEMAS",
            "extracted_schemas is missing or empty in manifest.json — nothing to clone",
        )

    try:
        result = backend.sandbox_up(
            run_id=run_id,
            schemas=schemas,
        )
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_UP_INVALID_INPUT", str(exc), exc)
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

    try:
        result = backend.sandbox_down(run_id=run_id)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_DOWN_INVALID_INPUT", str(exc), exc)
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
        _error_exit("SCENARIO_NOT_FOUND", f"Scenario file not found: {scenario}")
    except json.JSONDecodeError as exc:
        _error_exit("SCENARIO_INVALID_JSON", f"Scenario file is not valid JSON: {exc}", exc)

    try:
        result = backend.execute_scenario(run_id=run_id, scenario=scenario_data)
    except (ValueError, KeyError) as exc:
        _error_exit("EXECUTE_INVALID_INPUT", str(exc), exc)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=execute run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
