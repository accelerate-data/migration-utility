"""CLI entrypoint and compatibility barrel for sandbox test harness helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer
from shared.cli_utils import emit
from shared.env_config import resolve_project_root
from shared.loader import CatalogFileMissingError, CatalogLoadError
from shared.loader_io import clear_manifest_sandbox, write_manifest_sandbox
from shared.runtime_config import get_extracted_schemas
from shared.test_harness_support.catalog_write import run_write_test_gen
from shared.test_harness_support.execution import run_compare_sql, run_execute_spec
from shared.test_harness_support.manifest import (
    _create_backend,
    _error_exit,
    _load_manifest,
    _resolve_sandbox_db,
)
from shared.test_harness_support.review import run_validate_review

logger = logging.getLogger(__name__)
app = typer.Typer(name="test-harness", no_args_is_help=True, add_completion=False, pretty_exceptions_enable=False)


@app.command()
def sandbox_up(
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Create a sandbox database and clone schema from the source."""
    logger.info("event=cli_invoked command=sandbox_up")
    root = resolve_project_root(Path(project_root))
    manifest = _load_manifest(root)
    backend = _create_backend(manifest, root)

    schemas = get_extracted_schemas(manifest)
    if not schemas:
        _error_exit(
            "NO_SCHEMAS",
            "extraction.schemas is missing or empty in manifest.json — nothing to clone",
        )

    try:
        result = backend.sandbox_up(
            schemas=schemas,
        )
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_UP_INVALID_INPUT", str(exc), exc)
    if result.status != "error":
        write_manifest_sandbox(root, result.sandbox_database)
    emit(result)
    logger.info("event=cli_complete command=sandbox_up sandbox_db=%s status=%s", result.sandbox_database, result.status)
    if result.status == "error":
        raise typer.Exit(code=1)


@app.command()
def sandbox_down(
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Drop a sandbox database."""
    logger.info("event=cli_invoked command=sandbox_down")
    root = resolve_project_root(Path(project_root))
    sandbox_db, manifest = _resolve_sandbox_db(root)
    logger.info("event=cli_resolved command=sandbox_down sandbox_db=%s", sandbox_db)
    backend = _create_backend(manifest, root)

    try:
        result = backend.sandbox_down(sandbox_db=sandbox_db)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_DOWN_INVALID_INPUT", str(exc), exc)
    if result.status != "error":
        clear_manifest_sandbox(root)
    emit(result)
    logger.info("event=cli_complete command=sandbox_down sandbox_db=%s status=%s", sandbox_db, result.status)
    if result.status == "error":
        raise typer.Exit(code=1)


@app.command()
def sandbox_status(
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Check whether a sandbox database exists."""
    logger.info("event=cli_invoked command=sandbox_status")
    root = resolve_project_root(Path(project_root))
    sandbox_db, manifest = _resolve_sandbox_db(root)
    logger.info("event=cli_resolved command=sandbox_status sandbox_db=%s", sandbox_db)
    backend = _create_backend(manifest, root)

    try:
        result = backend.sandbox_status(sandbox_db=sandbox_db)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_STATUS_INVALID_INPUT", str(exc), exc)
    emit(result)
    logger.info("event=cli_complete command=sandbox_status sandbox_db=%s status=%s", sandbox_db, result.status)
    if result.status == "error":
        raise typer.Exit(code=1)
    if not result.exists:
        raise typer.Exit(code=1)


@app.command()
def execute(
    scenario: str = typer.Option(..., help="Path to scenario JSON file"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Execute a test scenario in the sandbox and capture ground truth."""
    logger.info("event=cli_invoked command=execute")
    root = resolve_project_root(Path(project_root))
    sandbox_db, manifest = _resolve_sandbox_db(root)
    logger.info("event=cli_resolved command=execute sandbox_db=%s scenario=%s", sandbox_db, scenario)
    backend = _create_backend(manifest, root)

    scenario_path = Path(scenario)
    try:
        with scenario_path.open() as f:
            scenario_data = json.load(f)
    except FileNotFoundError:
        _error_exit("SCENARIO_NOT_FOUND", f"Scenario file not found: {scenario}")
    except json.JSONDecodeError as exc:
        _error_exit("SCENARIO_INVALID_JSON", f"Scenario file is not valid JSON: {exc}", exc)

    try:
        result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario_data)
    except NotImplementedError as exc:
        _error_exit("EXECUTE_UNSUPPORTED", str(exc), exc)
    except (ValueError, KeyError) as exc:
        _error_exit("EXECUTE_INVALID_INPUT", str(exc), exc)
    emit(result)
    logger.info("event=cli_complete command=execute sandbox_db=%s status=%s", sandbox_db, result.status)
    if result.status == "error":
        raise typer.Exit(code=1)


@app.command()
def execute_spec(
    spec: str = typer.Option(..., help="Path to test spec JSON file"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Bulk-execute all scenarios in a test spec and write expect.rows back."""
    logger.info("event=cli_invoked command=execute_spec")
    root = resolve_project_root(Path(project_root))
    sandbox_db, manifest = _resolve_sandbox_db(root)
    logger.info("event=cli_resolved command=execute_spec sandbox_db=%s spec=%s", sandbox_db, spec)
    backend = _create_backend(manifest, root)

    spec_path = Path(spec)
    try:
        output = run_execute_spec(backend, sandbox_db, spec_path)
    except ValueError as exc:
        code, _, message = str(exc).partition(": ")
        _error_exit(code or "EXECUTE_SPEC_ERROR", message or str(exc), exc)

    emit(output)
    logger.info(
        "event=cli_complete command=execute_spec sandbox_db=%s total=%d ok=%d failed=%d",
        sandbox_db, output.total, output.ok, output.failed,
    )
    if output.failed > 0:
        raise typer.Exit(code=1)


@app.command()
def compare_sql(
    sql_a_file: str = typer.Option(..., help="Path to file containing extracted core SELECT (sql A)"),
    sql_b_file: str = typer.Option(..., help="Path to file containing refactored CTE SELECT (sql B)"),
    spec: str = typer.Option(..., help="Path to test spec JSON file"),
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Compare two SQL SELECT statements for equivalence per test scenario.

    For each scenario in the spec, seeds fixtures, runs both SELECTs, and
    computes symmetric diff — all within a single rolled-back transaction.
    Returns aggregated pass/fail JSON.
    """
    logger.info("event=cli_invoked command=compare_sql")
    root = resolve_project_root(Path(project_root))
    sandbox_db, manifest = _resolve_sandbox_db(root)
    backend = _create_backend(manifest, root)

    try:
        output = run_compare_sql(
            backend,
            sandbox_db,
            Path(sql_a_file),
            Path(sql_b_file),
            Path(spec),
        )
    except ValueError as exc:
        code, _, message = str(exc).partition(": ")
        _error_exit(code or "COMPARE_SQL_ERROR", message or str(exc), exc)

    typer.echo(json.dumps(output, indent=2))
    logger.info(
        "event=cli_complete command=compare_sql sandbox_db=%s total=%d passed=%d failed=%d",
        sandbox_db, output["total"], output["passed"], output["failed"],
    )
    if output["failed"] > 0:
        raise typer.Exit(code=1)


@app.command("write")
def write_cmd(
    table: str = typer.Option(..., help="Fully-qualified table/view name"),
    branches: int = typer.Option(..., help="Number of test branches"),
    unit_tests: int = typer.Option(..., "--unit-tests", help="Number of unit tests"),
    coverage: str = typer.Option(..., help="Coverage level: complete, partial, none"),
    warnings_json: Optional[str] = typer.Option(None, "--warnings", help="JSON array of warning diagnostics"),
    errors_json: Optional[str] = typer.Option(None, "--errors", help="JSON array of error diagnostics"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Write test-gen summary to catalog with CLI-determined status."""
    root = resolve_project_root(project_root)

    parsed_warnings: list[dict[str, Any]] | None = None
    parsed_errors: list[dict[str, Any]] | None = None
    if warnings_json:
        try:
            parsed_warnings = json.loads(warnings_json)
        except json.JSONDecodeError as exc:
            logger.error("event=write_failed operation=parse_warnings table=%s error=%s", table, exc)
            raise typer.Exit(code=1) from exc
    if errors_json:
        try:
            parsed_errors = json.loads(errors_json)
        except json.JSONDecodeError as exc:
            logger.error("event=write_failed operation=parse_errors table=%s error=%s", table, exc)
            raise typer.Exit(code=1) from exc

    try:
        result = run_write_test_gen(
            project_root=root,
            table_fqn=table,
            branches=branches,
            unit_tests=unit_tests,
            coverage=coverage,
            warnings=parsed_warnings,
            errors=parsed_errors,
        )
    except ValueError as exc:
        logger.error("event=write_failed operation=validation table=%s error=%s", table, exc)
        emit({"status": "error", "error": str(exc)})
        raise typer.Exit(code=1) from exc
    except (CatalogFileMissingError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=1) from exc
    except OSError as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command("validate-review")
def validate_review_cmd(
    review_file: Path = typer.Option(..., "--review-file", help="Path to review JSON file"),
) -> None:
    """Validate a test review result via Pydantic TestReviewOutput model."""
    try:
        result = run_validate_review(review_file)
    except ValueError as exc:
        logger.error("event=validate_review_failed file=%s error=%s", review_file, exc)
        emit({"valid": False, "error": str(exc)})
        raise typer.Exit(code=1) from exc
    emit(result)


if __name__ == "__main__":
    app()
