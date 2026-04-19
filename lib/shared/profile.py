"""profile.py -- Profiling context assembly and catalog write-back.

Standalone CLI with two subcommands:

    context  Assemble all deterministic context needed for LLM profiling.
    write    Validate and merge a profile section into a table catalog file.

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain/validation failure
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer
from pydantic import ValidationError

from shared.catalog import (
    load_and_merge_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    read_selected_writer,
)
from shared.catalog_models import ReferencesBucket, TableCatalog, TableProfileSection, ViewProfileSection
from shared.cli_utils import emit
from shared.context_helpers import (
    project_sql_dialect,
    references_from_selected_sql,
    resolve_selected_writer_ddl_slice,
    target_visible_columns,
)
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    load_ddl,
)
from shared.name_resolver import normalize
from shared.output_models.discover import SqlElement
from shared.output_models.profile import (
    CatalogSignals,
    EnrichedInScopeRef,
    EnrichedScopedRefList,
    OutOfScopeRef,
    ProfileColumnDef,
    ProfileContext,
    RelatedProcedure,
    ViewColumnDef,
    ViewProfileContext,
    ViewReferencedBy,
    ViewReferences,
)
from shared.profile_support import (
    build_seed_profile,
    derive_table_profile_status,
    derive_view_profile_status,
    run_context,
    run_view_context,
    run_write,
)

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)

__all__ = [
    "CatalogSignals",
    "EnrichedInScopeRef",
    "EnrichedScopedRefList",
    "OutOfScopeRef",
    "ProfileColumnDef",
    "ProfileContext",
    "ReferencesBucket",
    "RelatedProcedure",
    "SqlElement",
    "TableCatalog",
    "TableProfileSection",
    "ViewColumnDef",
    "ViewProfileContext",
    "ViewReferencedBy",
    "ViewReferences",
    "ViewProfileSection",
    "app",
    "build_seed_profile",
    "context",
    "derive_table_profile_status",
    "derive_view_profile_status",
    "load_and_merge_catalog",
    "load_ddl",
    "load_proc_catalog",
    "load_table_catalog",
    "load_view_catalog",
    "project_sql_dialect",
    "read_selected_writer",
    "references_from_selected_sql",
    "resolve_catalog_dir",
    "resolve_selected_writer_ddl_slice",
    "run_context",
    "run_view_context",
    "run_write",
    "target_visible_columns",
    "view_context",
    "write",
]


@app.command()
def context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
) -> None:
    """Assemble profiling context for a table + writer pair."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_context(project_root, table, writer)
    except CatalogFileMissingError as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=1) from exc
    except (ValueError, FileNotFoundError, DdlParseError, CatalogNotFoundError, CatalogLoadError) as exc:
        logger.error("event=context_failed table=%s writer=%s error=%s", table, writer, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command(name="view-context")
def view_context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    view: str = typer.Option(..., "--view", help="Fully-qualified view name (schema.Name)"),
) -> None:
    """Assemble view profiling context for LLM classification."""
    project_root = resolve_project_root(project_root)
    try:
        result = run_view_context(project_root, view)
    except CatalogFileMissingError as exc:
        logger.error("event=view_context_failed view=%s error=%s", view, exc)
        raise typer.Exit(code=1) from exc
    except ValueError as exc:
        logger.error("event=view_context_failed view=%s error=%s", view, exc)
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, CatalogLoadError) as exc:
        logger.error("event=view_context_failed view=%s error=%s", view, exc)
        raise typer.Exit(code=2) from exc
    emit(result)


@app.command()
def write(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    profile: str = typer.Option("", help="Profile JSON string"),
    profile_file: Optional[Path] = typer.Option(None, "--profile-file", help="Path to file containing profile JSON"),
) -> None:
    """Validate and merge a profile section into a table catalog file."""
    if profile_file:
        profile = profile_file.read_text(encoding="utf-8")
    if not profile:
        logger.error("event=write_failed table=%s error=no profile provided (use --profile or --profile-file)", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)
    try:
        profile_data = json.loads(profile)
    except json.JSONDecodeError as exc:
        logger.error("event=write_failed operation=parse_json table=%s error=%s", table, exc)
        raise typer.Exit(code=2) from exc

    try:
        result = run_write(project_root, table, profile_data)
    except (ValueError, ValidationError, CatalogFileMissingError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit({"ok": False, "error": str(exc), "table": normalize(table)})
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
