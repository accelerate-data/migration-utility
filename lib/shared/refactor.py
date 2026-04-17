"""CLI entrypoint and compatibility barrel for refactor helpers."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

import typer

from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
)
from shared.cli_utils import emit
from shared.env_config import resolve_catalog_dir, resolve_project_root
from shared.name_resolver import normalize
from shared.output_models.refactor import RefactorWriteOutput
from shared.refactor_support.context import run_context
from shared.refactor_support.writeback import run_write as _run_write_impl

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


def run_write(
    project_root: Path,
    table_fqn: str,
    extracted_sql: str,
    refactored_sql: str,
    semantic_review: dict[str, Any] | None = None,
    compare_sql_result: dict[str, Any] | None = None,
    compare_required: bool = True,
) -> RefactorWriteOutput:
    """Compatibility wrapper for refactor writes."""
    return _run_write_impl(
        project_root=project_root,
        table_fqn=table_fqn,
        extracted_sql=extracted_sql,
        refactored_sql=refactored_sql,
        semantic_review=semantic_review,
        compare_sql_result=compare_sql_result,
        compare_required=compare_required,
        catalog_dir=resolve_catalog_dir(project_root),
    )


# ── CLI commands ─────────────────────────────────────────────────────────────


@app.command()
def context(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    writer: Optional[str] = typer.Option(None, help="Fully-qualified writer procedure name (reads from catalog scoping section if omitted)"),
) -> None:
    """Assemble refactoring context for a table + writer pair."""
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


@app.command()
def write(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Path to project root directory (defaults to current working directory)"),
    table: str = typer.Option(..., help="Fully-qualified table name (schema.Name)"),
    extracted_sql: str = typer.Option("", help="Extracted core SQL string"),
    extracted_sql_file: Optional[Path] = typer.Option(None, "--extracted-sql-file", help="Path to file containing extracted core SQL"),
    refactored_sql: str = typer.Option("", help="Refactored SQL string"),
    refactored_sql_file: Optional[Path] = typer.Option(None, "--refactored-sql-file", help="Path to file containing refactored SQL"),
    semantic_review_file: Optional[Path] = typer.Option(None, "--semantic-review-file", help="Path to JSON file containing structured semantic review evidence"),
    compare_sql_file: Optional[Path] = typer.Option(None, "--compare-sql-file", help="Path to JSON file containing compare-sql output"),
    compare_required: bool = typer.Option(True, "--compare-required/--no-compare-required", help="Require executable compare-sql proof for status=ok"),
) -> None:
    """Validate and merge a refactor section into the writer procedure's catalog."""
    if extracted_sql_file:
        extracted_sql = extracted_sql_file.read_text(encoding="utf-8")
    if refactored_sql_file:
        refactored_sql = refactored_sql_file.read_text(encoding="utf-8")
    semantic_review = None
    compare_sql_result = None
    if semantic_review_file:
        semantic_review = json.loads(semantic_review_file.read_text(encoding="utf-8"))
    if compare_sql_file:
        compare_sql_result = json.loads(compare_sql_file.read_text(encoding="utf-8"))
    if not extracted_sql and not refactored_sql:
        logger.error("event=write_failed table=%s error=no SQL provided", table)
        raise typer.Exit(code=1)
    project_root = resolve_project_root(project_root)

    try:
        result = run_write(
            project_root=project_root,
            table_fqn=table,
            extracted_sql=extracted_sql,
            refactored_sql=refactored_sql,
            semantic_review=semantic_review,
            compare_sql_result=compare_sql_result,
            compare_required=compare_required,
        )
    except (ValueError, CatalogFileMissingError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit(RefactorWriteOutput(ok=False, error=str(exc), table=normalize(table)))
        raise typer.Exit(code=1) from exc
    except (FileNotFoundError, OSError, CatalogLoadError) as exc:
        logger.error("event=write_failed table=%s error=%s", table, exc)
        emit(RefactorWriteOutput(ok=False, error=str(exc), table=normalize(table)))
        raise typer.Exit(code=2) from exc
    emit(result)


if __name__ == "__main__":
    app()
