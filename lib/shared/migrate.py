"""CLI entrypoint and compatibility barrel for migrate helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import typer

from shared.cli_utils import emit
from shared.env_config import resolve_dbt_project_path, resolve_project_root
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    ProfileMissingError,
)
from shared.migrate_support.artifacts import (
    run_render_unit_tests,
    run_write,
    run_write_generate,
)
from shared.migrate_support.context import _load_refactored_sql, run_context  # noqa: F401
from shared.migrate_support.derivation import derive_materialization, derive_schema_tests  # noqa: F401

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command()
def context(
    table: str = typer.Option(..., help="Fully-qualified target table name (schema.table)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
) -> None:
    """Assemble migration context from catalog + DDL."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_context(project_root, table, writer)
    except (CatalogFileMissingError, ProfileMissingError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=1) from exc
    except (ValueError, FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def write(
    table: str = typer.Option(..., help="Fully-qualified target table name (schema.table)"),
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    dbt_project_path: Optional[Path] = typer.Option(None, "--dbt-project-path", help="Path to dbt project (default: $DBT_PROJECT_PATH or ./dbt)"),
    model_sql: str = typer.Option("", help="Generated dbt model SQL (inline string)"),
    schema_yml: str = typer.Option("", help="Generated schema YAML (inline string)"),
    model_sql_file: Optional[Path] = typer.Option(None, "--model-sql-file", help="Path to file containing generated dbt model SQL"),
    schema_yml_file: Optional[Path] = typer.Option(None, "--schema-yml-file", help="Path to file containing generated schema YAML"),
) -> None:
    """Write generated dbt model SQL + schema YAML to dbt project."""
    if model_sql_file:
        model_sql = model_sql_file.read_text(encoding="utf-8")
    if schema_yml_file:
        schema_yml = schema_yml_file.read_text(encoding="utf-8")
    if not model_sql:
        logger.error("event=write_failed table=%s error=no model SQL provided (use --model-sql or --model-sql-file)", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    if dbt_project_path is None:
        dbt_project_path = resolve_dbt_project_path(project_root)
    try:
        result = run_write(table, project_root, dbt_project_path, model_sql, schema_yml)
    except ValueError as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


# ── Write generate summary ────────────────────────────────────────────────────


@app.command("write-catalog")
def write_catalog_cmd(
    table: str = typer.Option(..., help="Fully-qualified table/view name"),
    model_path: str = typer.Option(..., "--model-path", help="Relative path to dbt model SQL file"),
    compiled: bool = typer.Option(..., help="Whether dbt compile succeeded"),
    tests_passed: bool = typer.Option(..., "--tests-passed/--no-tests-passed", help="Whether dbt test passed"),
    test_count: int = typer.Option(0, "--test-count", help="Number of dbt tests executed"),
    schema_yml_flag: bool = typer.Option(False, "--schema-yml", help="Whether schema YAML entry exists"),
    warnings_json: Optional[str] = typer.Option(None, "--warnings", help="JSON array of warning diagnostics"),
    errors_json: Optional[str] = typer.Option(None, "--errors", help="JSON array of error diagnostics"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Write model generation summary to catalog with CLI-determined status."""
    root = resolve_project_root(project_root)

    parsed_warnings: list[dict[str, Any]] | None = None
    parsed_errors: list[dict[str, Any]] | None = None
    if warnings_json:
        try:
            parsed_warnings = json.loads(warnings_json)
        except json.JSONDecodeError as exc:
            logger.error("event=write_catalog_failed operation=parse_warnings table=%s error=%s", table, exc)
            raise typer.Exit(code=1) from exc
    if errors_json:
        try:
            parsed_errors = json.loads(errors_json)
        except json.JSONDecodeError as exc:
            logger.error("event=write_catalog_failed operation=parse_errors table=%s error=%s", table, exc)
            raise typer.Exit(code=1) from exc

    try:
        result = run_write_generate(
            project_root=root,
            table_fqn=table,
            model_path=model_path,
            compiled=compiled,
            tests_passed=tests_passed,
            test_count=test_count,
            schema_yml=schema_yml_flag,
            warnings=parsed_warnings,
            errors=parsed_errors,
        )
    except (CatalogFileMissingError, CatalogLoadError) as exc:
        logger.error("event=write_catalog_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=1) from exc
    except OSError as exc:
        logger.error("event=write_catalog_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command("render-unit-tests")
def render_unit_tests_cmd(
    table: str = typer.Option(..., help="Fully-qualified table/view name"),
    model_name: str = typer.Option(..., "--model-name", help="dbt model name"),
    spec: Path = typer.Option(..., help="Path to test-specs/<item_id>.json"),
    schema_yml: Path = typer.Option(..., "--schema-yml", help="Path to target schema YAML file"),
    project_root: Optional[Path] = typer.Option(None, "--project-root"),
) -> None:
    """Translate test-spec scenarios into dbt unit tests in schema YAML."""
    root = resolve_project_root(project_root)
    try:
        result = run_render_unit_tests(root, table, model_name, spec, schema_yml)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("event=render_unit_tests_failed table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
