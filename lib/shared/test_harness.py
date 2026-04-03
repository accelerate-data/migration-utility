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
from shared.loader_io import clear_manifest_sandbox, read_manifest, write_manifest_sandbox
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


def _resolve_run_id(run_id: str | None, project_root: Path) -> str:
    """Return run_id if provided, otherwise fall back to manifest.sandbox.run_id."""
    if run_id is not None:
        return run_id
    manifest = read_manifest(project_root)
    sandbox = manifest.get("sandbox")
    if not sandbox:
        _error_exit(
            "MISSING_RUN_ID",
            "No --run-id provided and no sandbox section in manifest.json",
        )
    return sandbox["run_id"]


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
    if result.get("status") != "error":
        write_manifest_sandbox(root, run_id, result["sandbox_database"])
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_up run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)


@app.command()
def sandbox_down(
    run_id: str | None = typer.Option(None, help="UUID of the sandbox to tear down"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Drop a sandbox database."""
    logger.info("event=cli_invoked command=sandbox_down")
    root = resolve_project_root(Path(project_root))
    run_id = _resolve_run_id(run_id, root)
    logger.info("event=cli_resolved command=sandbox_down run_id=%s", run_id)
    manifest = _load_manifest(root)
    backend = _create_backend(manifest)

    try:
        result = backend.sandbox_down(run_id=run_id)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_DOWN_INVALID_INPUT", str(exc), exc)
    if result.get("status") != "error":
        clear_manifest_sandbox(root)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_down run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)


@app.command()
def sandbox_status(
    run_id: str | None = typer.Option(None, help="UUID of the sandbox to check"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Check whether a sandbox database exists."""
    logger.info("event=cli_invoked command=sandbox_status")
    root = resolve_project_root(Path(project_root))
    run_id = _resolve_run_id(run_id, root)
    logger.info("event=cli_resolved command=sandbox_status run_id=%s", run_id)
    manifest = _load_manifest(root)
    backend = _create_backend(manifest)

    try:
        result = backend.sandbox_status(run_id=run_id)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_STATUS_INVALID_INPUT", str(exc), exc)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_status run_id=%s status=%s", run_id, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)
    if not result.get("exists"):
        raise typer.Exit(code=1)


@app.command()
def execute(
    run_id: str | None = typer.Option(None, help="UUID of the sandbox"),
    scenario: str = typer.Option(..., help="Path to scenario JSON file"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Execute a test scenario in the sandbox and capture ground truth."""
    logger.info("event=cli_invoked command=execute")
    root = resolve_project_root(Path(project_root))
    run_id = _resolve_run_id(run_id, root)
    logger.info("event=cli_resolved command=execute run_id=%s scenario=%s", run_id, scenario)
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


@app.command()
def execute_spec(
    spec: str = typer.Option(..., help="Path to test spec JSON file"),
    run_id: str | None = typer.Option(None, help="UUID of the sandbox"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Bulk-execute all scenarios in a test spec and write expect.rows back."""
    logger.info("event=cli_invoked command=execute_spec")
    root = resolve_project_root(Path(project_root))
    run_id = _resolve_run_id(run_id, root)
    logger.info("event=cli_resolved command=execute_spec run_id=%s spec=%s", run_id, spec)
    manifest = _load_manifest(root)
    backend = _create_backend(manifest)

    spec_path = Path(spec)
    try:
        with spec_path.open() as f:
            spec_data = json.load(f)
    except FileNotFoundError:
        _error_exit("SPEC_NOT_FOUND", f"Test spec file not found: {spec}")
    except json.JSONDecodeError as exc:
        _error_exit("SPEC_INVALID_JSON", f"Test spec file is not valid JSON: {exc}", exc)

    unit_tests = spec_data.get("unit_tests", [])
    if not unit_tests:
        _error_exit("SPEC_EMPTY", "Test spec has no unit_tests entries")

    results: list[dict[str, Any]] = []
    ok_count = 0
    failed_count = 0

    for test_entry in unit_tests:
        scenario = {
            "name": test_entry["name"],
            "target_table": test_entry["target_table"],
            "procedure": test_entry["procedure"],
            "given": test_entry["given"],
        }

        try:
            result = backend.execute_scenario(run_id=run_id, scenario=scenario)
        except (ValueError, KeyError) as exc:
            result = {
                "scenario_name": test_entry["name"],
                "status": "error",
                "ground_truth_rows": [],
                "row_count": 0,
                "errors": [{"code": "EXECUTE_INVALID_INPUT", "message": str(exc)}],
            }

        results.append({
            "scenario_name": result.get("scenario_name", test_entry["name"]),
            "status": result["status"],
            "row_count": result.get("row_count", 0),
            "errors": result.get("errors", []),
        })

        if result["status"] == "ok":
            test_entry["expect"] = {"rows": result["ground_truth_rows"]}
            ok_count += 1
        else:
            failed_count += 1
            logger.warning(
                "event=scenario_failed command=execute_spec run_id=%s scenario=%s errors=%s",
                run_id, test_entry["name"], result.get("errors"),
            )

    # Write updated spec back with expect.rows populated
    with spec_path.open("w") as f:
        json.dump(spec_data, f, indent=2)

    output = {
        "schema_version": "1.0",
        "run_id": run_id,
        "spec_path": str(spec_path),
        "total": len(unit_tests),
        "ok": ok_count,
        "failed": failed_count,
        "results": results,
    }

    typer.echo(json.dumps(output, indent=2))
    logger.info(
        "event=cli_complete command=execute_spec run_id=%s total=%d ok=%d failed=%d",
        run_id, len(unit_tests), ok_count, failed_count,
    )
    if ok_count == 0:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
