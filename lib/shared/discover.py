"""CLI entrypoint and compatibility barrel for discover helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer

from shared.catalog import resolve_catalog_path
from shared.catalog_writer import (
    run_write_scoping,
    run_write_source,
    run_write_statements,
    run_write_table_slice,
    run_write_view_scoping,
)
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    ObjectNotFoundError,
)
from shared.cli_utils import emit
from shared.discover_support.browse import ObjectType, run_list, run_refs, run_show
from shared.env_config import resolve_project_root
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def _is_view_catalog_path(cat_path: Path) -> bool:
    """Return True when a catalog path points at catalog/views/."""
    return cat_path.parent.name == "views"


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command(name="list")
def list_objects(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    type: ObjectType = typer.Option(..., help="Object type to list"),
) -> None:
    """List all objects of a given type in a DDL directory."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_list(project_root, type)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def show(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
) -> None:
    """Show details for a single named DDL object."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_show(project_root, name)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def refs(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
) -> None:
    """Find all procedures/views that reference a given object."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_refs(project_root, name)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command(name="write-statements")
def write_statements(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully-qualified procedure name (schema.Name)"),
    statements: str = typer.Option("", help="JSON array of resolved statement objects"),
    statements_file: Optional[Path] = typer.Option(None, "--statements-file", help="Path to file containing statements JSON"),
) -> None:
    """Persist resolved statements into a procedure catalog file."""
    if statements_file:
        statements = statements_file.read_text(encoding="utf-8")
    if not statements:
        logger.error("event=command_failed error=no statements provided (use --statements or --statements-file)")
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        stmts = json.loads(statements)
    except json.JSONDecodeError as exc:
        logger.error("event=command_failed error=invalid_json detail=%s", exc)
        raise typer.Exit(code=2) from exc
    try:
        result = run_write_statements(project_root, name, stmts)
    except (ObjectNotFoundError, FileNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    except ValueError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    emit(result)


@app.command(name="write-scoping")
def write_scoping(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully qualified table name"),
    scoping: str = typer.Option("", help="Scoping JSON"),
    scoping_file: Optional[Path] = typer.Option(None, "--scoping-file", help="Path to file containing scoping JSON"),
) -> None:
    """Persist scoping results to a table catalog file."""
    if scoping_file:
        scoping = scoping_file.read_text(encoding="utf-8")
    if not scoping:
        logger.error("event=command_failed error=no scoping provided (use --scoping or --scoping-file)")
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        scoping_data = json.loads(scoping)
    except json.JSONDecodeError as exc:
        logger.error("event=command_failed error=invalid_json detail=%s", exc)
        raise typer.Exit(code=2) from exc
    try:
        # Auto-detect: route to view or table scoping based on catalog presence
        cat_path = resolve_catalog_path(project_root, normalize(name))
        if _is_view_catalog_path(cat_path):
            result = run_write_view_scoping(project_root, name, scoping_data)
        else:
            result = run_write_scoping(project_root, name, scoping_data)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    except ValueError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    emit(result)


@app.command(name="write-source")
def write_source(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    name: str = typer.Option(..., help="Fully qualified table name"),
    value: bool = typer.Option(True, "--value/--no-value", help="Set (--value) or clear (--no-value) the is_source flag"),
) -> None:
    """Set or clear the is_source flag on a table catalog file.

    Marks the table as a dbt source (is_source: true) or resets it to false.
    Guard: table catalog must exist and scoping must be present.
    """
    project_root = resolve_project_root(project_root)
    try:
        result = run_write_source(project_root, name, value)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    except ValueError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    emit(result)


@app.command(name="write-slice")
def write_slice(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    proc: str = typer.Option(..., "--proc", help="Fully qualified procedure FQN"),
    table: str = typer.Option(..., "--table", help="Fully qualified table FQN"),
    slice: Optional[str] = typer.Option(None, "--slice", help="Inline DDL text for the table slice (mutually exclusive with --slice-file)"),
    slice_file: Optional[Path] = typer.Option(None, "--slice-file", help="Path to file containing the DDL slice (mutually exclusive with --slice)"),
) -> None:
    """Write a per-table DDL slice into a procedure catalog file."""
    if slice_file is not None and slice is not None:
        logger.error("event=command_failed error=--slice and --slice-file are mutually exclusive")
        raise typer.Exit(code=1)
    if slice_file:
        slice = slice_file.read_text(encoding="utf-8")
    if not slice:
        logger.error("event=command_failed error=no slice provided (use --slice or --slice-file)")
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        result = run_write_table_slice(project_root, proc, table, slice)
    except (CatalogFileMissingError, ObjectNotFoundError) as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=1) from exc
    except CatalogLoadError as exc:
        logger.error("event=command_failed error=%s", exc)
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
