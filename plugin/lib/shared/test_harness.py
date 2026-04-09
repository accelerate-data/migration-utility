"""Test harness CLI — sandbox lifecycle and scenario execution.

Reads manifest.json to determine technology, then routes to a
technology-specific backend (SQL Server via pyodbc first).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, NoReturn, Optional

import typer
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from shared.catalog import (
    load_and_merge_catalog,
    write_json as _write_catalog_json,
)
from shared.catalog_models import TestGenSection
from shared.cli_utils import emit
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.loader import CatalogFileMissingError, CatalogLoadError
from shared.loader_io import clear_manifest_sandbox, read_manifest, write_manifest_sandbox
from shared.name_resolver import normalize
from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend

logger = logging.getLogger(__name__)
app = typer.Typer(name="test-harness", no_args_is_help=True, add_completion=False, pretty_exceptions_enable=False)

_SCHEMA_DIR = Path(__file__).with_name("schemas")


# ── Schema validation helpers ────────────────────────────────────────────────


def _load_schema_with_store(schema_name: str) -> tuple[dict[str, Any], Registry]:
    """Load a schema file and a registry for local schema references."""
    registry = Registry()
    loaded: dict[str, Any] = {}
    for schema_path in _SCHEMA_DIR.glob("*.json"):
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        loaded[schema_path.name] = schema
        resource = Resource.from_contents(schema)
        registry = registry.with_resource(schema_path.name, resource)
        registry = registry.with_resource(schema_path.resolve().as_uri(), resource)
    return loaded[schema_name], registry


def _format_validation_errors(errors: list[Any]) -> str:
    """Render jsonschema validation errors in compact, actionable form."""
    parts: list[str] = []
    for error in errors:
        path = "/" + "/".join(str(segment) for segment in error.absolute_path)
        parts.append(f"{path or '/'}: {error.message}")
    return "; ".join(parts)


def _validate_test_spec(spec_data: dict[str, Any]) -> None:
    """Validate a test spec against test_spec.json and raise ValueError with field-level errors."""
    schema, registry = _load_schema_with_store("test_spec.json")
    validator = Draft202012Validator(schema, registry=registry)
    errors = sorted(validator.iter_errors(spec_data), key=lambda err: list(err.absolute_path))
    if errors:
        raise ValueError(
            f"Test spec schema validation failed: {_format_validation_errors(errors)}"
        )


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


def _resolve_sandbox_db(project_root: Path) -> tuple[str, dict[str, Any]]:
    """Read sandbox.database from manifest, or fail with SANDBOX_NOT_CONFIGURED.

    Returns ``(sandbox_db, manifest)`` to avoid re-reading manifest.json.
    """
    manifest = read_manifest(project_root)
    sandbox = manifest.get("sandbox")
    if not sandbox or not sandbox.get("database"):
        _error_exit(
            "SANDBOX_NOT_CONFIGURED",
            "No sandbox configured in manifest.json. Run /setup-sandbox first.",
        )
    return sandbox["database"], manifest


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
    project_root: str = typer.Option(".", help="Project root directory"),
) -> None:
    """Create a sandbox database and clone schema from the source."""
    logger.info("event=cli_invoked command=sandbox_up")
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
            schemas=schemas,
        )
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_UP_INVALID_INPUT", str(exc), exc)
    if result.get("status") != "error":
        write_manifest_sandbox(root, result["sandbox_database"])
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_up sandbox_db=%s status=%s", result.get("sandbox_database"), result.get("status"))
    if result.get("status") == "error":
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
    backend = _create_backend(manifest)

    try:
        result = backend.sandbox_down(sandbox_db=sandbox_db)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_DOWN_INVALID_INPUT", str(exc), exc)
    if result.get("status") != "error":
        clear_manifest_sandbox(root)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_down sandbox_db=%s status=%s", sandbox_db, result.get("status"))
    if result.get("status") == "error":
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
    backend = _create_backend(manifest)

    try:
        result = backend.sandbox_status(sandbox_db=sandbox_db)
    except (ValueError, KeyError) as exc:
        _error_exit("SANDBOX_STATUS_INVALID_INPUT", str(exc), exc)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=sandbox_status sandbox_db=%s status=%s", sandbox_db, result.get("status"))
    if result.get("status") == "error":
        raise typer.Exit(code=1)
    if not result.get("exists"):
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
        result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario_data)
    except (ValueError, KeyError) as exc:
        _error_exit("EXECUTE_INVALID_INPUT", str(exc), exc)
    typer.echo(json.dumps(result, indent=2))
    logger.info("event=cli_complete command=execute sandbox_db=%s status=%s", sandbox_db, result.get("status"))
    if result.get("status") == "error":
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
        try:
            if "procedure" in test_entry:
                # Procedure-based test: run stored procedure, read target table
                scenario = {
                    "name": test_entry["name"],
                    "target_table": test_entry["target_table"],
                    "procedure": test_entry["procedure"],
                    "given": test_entry["given"],
                }
                result = backend.execute_scenario(sandbox_db=sandbox_db, scenario=scenario)
            else:
                # View-based test: run a SELECT directly
                result = backend.execute_select(
                    sandbox_db=sandbox_db,
                    sql=test_entry["sql"],
                    fixtures=test_entry["given"],
                )
                result["scenario_name"] = test_entry["name"]
        except (ValueError, KeyError) as exc:
            result = {
                "scenario_name": test_entry.get("name", "unknown"),
                "status": "error",
                "ground_truth_rows": [],
                "row_count": 0,
                "errors": [{"code": "EXECUTE_INVALID_INPUT", "message": str(exc)}],
            }

        results.append({
            "scenario_name": result.get("scenario_name", test_entry.get("name", "unknown")),
            "status": result["status"],
            "row_count": result.get("row_count", 0),
            "errors": result.get("errors", []),
        })

        if result["status"] == "ok":
            test_entry["expect"] = {"rows": result["ground_truth_rows"]}
            ok_count += 1
        else:
            test_entry.pop("expect", None)  # clear stale ground truth
            failed_count += 1
            logger.warning(
                "event=scenario_failed command=execute_spec sandbox_db=%s scenario=%s errors=%s",
                sandbox_db, test_entry["name"], result.get("errors"),
            )

    # Write updated spec back with expect.rows populated
    with spec_path.open("w") as f:
        json.dump(spec_data, f, indent=2)

    output = {
        "schema_version": "1.0",
        "sandbox_database": sandbox_db,
        "spec_path": str(spec_path),
        "total": len(unit_tests),
        "ok": ok_count,
        "failed": failed_count,
        "results": results,
    }

    typer.echo(json.dumps(output, indent=2))
    logger.info(
        "event=cli_complete command=execute_spec sandbox_db=%s total=%d ok=%d failed=%d",
        sandbox_db, len(unit_tests), ok_count, failed_count,
    )
    if ok_count == 0:
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
    backend = _create_backend(manifest)

    # Load both SQL files
    for label, path_str in [("A", sql_a_file), ("B", sql_b_file)]:
        if not Path(path_str).exists():
            _error_exit("SQL_FILE_NOT_FOUND", f"SQL file {label} not found: {path_str}")
    try:
        sql_a = Path(sql_a_file).read_text(encoding="utf-8")
        sql_b = Path(sql_b_file).read_text(encoding="utf-8")
    except OSError as exc:
        _error_exit("SQL_FILE_READ_ERROR", f"Cannot read SQL file: {exc}", exc)

    # Load test spec
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
    passed_count = 0
    failed_count = 0

    for test_entry in unit_tests:
        fixtures = test_entry.get("given", [])
        scenario_name = test_entry.get("name", "unnamed")

        try:
            result = backend.compare_two_sql(
                sandbox_db=sandbox_db,
                sql_a=sql_a,
                sql_b=sql_b,
                fixtures=fixtures,
            )
        except (ValueError, KeyError) as exc:
            result = {
                "status": "error",
                "equivalent": False,
                "a_count": 0,
                "b_count": 0,
                "a_minus_b": [],
                "b_minus_a": [],
                "errors": [{"code": "COMPARE_INVALID_INPUT", "message": str(exc)}],
            }

        result["scenario_name"] = scenario_name
        results.append(result)

        if result.get("equivalent"):
            passed_count += 1
        else:
            failed_count += 1
            logger.warning(
                "event=compare_scenario_failed command=compare_sql sandbox_db=%s "
                "scenario=%s errors=%s",
                sandbox_db, scenario_name, result.get("errors"),
            )

    output = {
        "schema_version": "1.0",
        "sandbox_database": sandbox_db,
        "total": len(unit_tests),
        "passed": passed_count,
        "failed": failed_count,
        "results": results,
    }

    typer.echo(json.dumps(output, indent=2))
    logger.info(
        "event=cli_complete command=compare_sql sandbox_db=%s total=%d passed=%d failed=%d",
        sandbox_db, len(unit_tests), passed_count, failed_count,
    )
    if passed_count == 0:
        raise typer.Exit(code=1)


# ── Write test-gen summary ────────────────────────────────────────────────────


def run_write_test_gen(
    project_root: Path,
    table_fqn: str,
    branches: int,
    unit_tests: int,
    coverage: str,
    warnings: list[dict[str, Any]] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validate test-gen output and write test_gen section to catalog.

    Reads the test-spec file from disk, validates it against test_spec.json,
    and writes the test_gen section to the table or view catalog file.
    Raises ValueError with field-level errors if the spec is invalid.
    """
    norm = normalize(table_fqn)

    # Load and validate the test-spec file
    spec_path = project_root / "test-specs" / f"{norm}.json"
    if not spec_path.exists():
        raise ValueError(
            f"Test spec file not found: {spec_path}. "
            f"Write test-specs/{norm}.json before calling test-harness write."
        )

    try:
        spec_data = json.loads(spec_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Test spec file is not valid JSON: {spec_path}. Parse error: {exc}"
        ) from exc

    # Validate against test_spec.json schema — raises ValueError with
    # field-level errors the caller can use to fix the spec and retry.
    _validate_test_spec(spec_data)

    # Determine status
    status = "ok" if branches > 0 else "error"

    # Build and validate test_gen section via Pydantic
    test_gen: dict[str, Any] = {
        "status": status,
        "test_spec_path": f"test-specs/{norm}.json",
        "branches": branches,
        "unit_tests": unit_tests,
        "coverage": coverage,
        "warnings": warnings or [],
        "errors": errors or [],
    }
    TestGenSection.model_validate(test_gen)

    result = load_and_merge_catalog(project_root, norm, "test_gen", test_gen)
    logger.info("event=write_test_gen_complete table=%s status=%s", norm, status)
    return result


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


# ── Validate review output ───────────────────────────────────────────────────


def run_validate_review(review_file: Path) -> dict[str, Any]:
    """Validate a test review JSON file against test_review_output.json schema.

    Returns {"valid": true} on success.
    Raises ValueError with field-level errors on validation failure.
    """
    if not review_file.exists():
        raise ValueError(f"Review file not found: {review_file}")

    try:
        review_data = json.loads(review_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Review file is not valid JSON: {review_file}. Parse error: {exc}") from exc

    schema, registry = _load_schema_with_store("test_review_output.json")
    validator = Draft202012Validator(schema, registry=registry)
    errors = sorted(validator.iter_errors(review_data), key=lambda err: list(err.absolute_path))
    if errors:
        raise ValueError(
            f"Review schema validation failed: {_format_validation_errors(errors)}"
        )

    return {"valid": True}


@app.command("validate-review")
def validate_review_cmd(
    review_file: Path = typer.Option(..., "--review-file", help="Path to review JSON file"),
) -> None:
    """Validate a test review result against test_review_output.json schema."""
    try:
        result = run_validate_review(review_file)
    except ValueError as exc:
        logger.error("event=validate_review_failed file=%s error=%s", review_file, exc)
        emit({"valid": False, "error": str(exc)})
        raise typer.Exit(code=1) from exc
    emit(result)


if __name__ == "__main__":
    app()
