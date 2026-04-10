"""catalog_writer.py — Pydantic-validated catalog write-back operations.

Provides the run_write_* functions that persist agent/skill outputs
(statements, scoping, source flags, table slices) into catalog JSON
files with Pydantic model validation.

Split from discover.py for module focus.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.catalog import (
    load_and_merge_catalog,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    write_proc_statements,
    write_proc_table_slice,
)
from shared.loader import CatalogFileMissingError
from shared.catalog_models import StatementEntry, TableScopingSection, ViewScopingSection
from shared.name_resolver import normalize
from shared.output_models.writeback import WriteSliceOutput, WriteSourceOutput

logger = logging.getLogger(__name__)


# ── Write-back operations ────────────────────────────────────────────────────


def run_write_statements(
    project_root: Path, name: str, statements: list[dict[str, Any]],
) -> dict[str, Any]:
    """Persist resolved statements into a procedure catalog file.

    All statements must have ``action`` set to ``migrate`` or ``skip`` —
    unresolved ``needs_llm`` actions are rejected.

    Returns a dict with ``written`` (path) and ``statement_count``.
    """
    for stmt in statements:
        action = stmt.get("action")
        if action not in ("migrate", "skip"):
            raise ValueError(
                f"Unresolved statement action {action!r} for {stmt.get('id', '?')} — "
                "all actions must be 'migrate' or 'skip' before writing."
            )
    for stmt in statements:
        StatementEntry.model_validate(stmt)
    path = write_proc_statements(project_root, name, statements)
    return {"written": str(path), "statement_count": len(statements)}


def run_write_scoping(
    project_root: Path,
    table_fqn: str,
    scoping: dict[str, Any],
) -> dict[str, Any]:
    """Validate and merge scoping results into a table catalog file."""
    if "status" in scoping:
        raise ValueError("status must not be passed — determined by CLI")

    table_norm = normalize(table_fqn)

    # Load existing catalog
    cat_model = load_table_catalog(project_root, table_norm)
    if cat_model is None:
        raise CatalogFileMissingError("table", table_norm)

    # Determine status from content
    selected_writer = scoping.get("selected_writer")
    has_errors = any(
        entry.get("severity") == "error"
        for entry in scoping.get("errors", [])
        if isinstance(entry, dict)
    )
    if has_errors:
        status = "error"
    elif selected_writer:
        proc_cat = load_proc_catalog(project_root, selected_writer)
        if proc_cat is not None:
            status = "resolved"
        else:
            status = "error"
    elif scoping.get("candidates"):
        status = "ambiguous_multi_writer"
    else:
        status = "no_writer_found"

    scoping["status"] = status
    TableScopingSection.model_validate(scoping)

    result = load_and_merge_catalog(project_root, table_norm, "scoping", scoping)
    return {"written": result["catalog_path"], "status": "ok"}


def run_write_view_scoping(
    project_root: Path,
    view_fqn: str,
    scoping: dict[str, Any],
) -> dict[str, Any]:
    """Validate and merge scoping results into a view catalog file."""
    if "status" in scoping:
        raise ValueError("status must not be passed — determined by CLI")

    view_norm = normalize(view_fqn)

    cat_model = load_view_catalog(project_root, view_norm)
    if cat_model is None:
        raise CatalogFileMissingError("view", view_norm)

    # Determine status from content
    has_sql_elements = scoping.get("sql_elements") is not None
    has_parse_errors = any(
        entry.get("code") == "DDL_PARSE_ERROR"
        for entry in scoping.get("errors", [])
        if isinstance(entry, dict)
    )
    if has_sql_elements:
        status = "analyzed"
    elif has_parse_errors:
        status = "error"
    else:
        status = "error"

    scoping["status"] = status
    ViewScopingSection.model_validate(scoping)

    result = load_and_merge_catalog(project_root, view_norm, "scoping", scoping)
    return {"written": result["catalog_path"], "status": "ok"}


def run_write_source(
    project_root: Path,
    table_fqn: str,
    value: bool,
) -> WriteSourceOutput:
    """Set or clear the is_source flag on a table catalog file."""
    table_norm = normalize(table_fqn)
    cat_model = load_table_catalog(project_root, table_norm)
    if cat_model is None:
        raise CatalogFileMissingError("table", table_norm)

    if cat_model.scoping is None:
        raise ValueError(
            f"Table {table_norm!r} has not been analyzed yet. "
            "Run /analyzing-table first."
        )

    result = load_and_merge_catalog(project_root, table_norm, "is_source", value)

    logger.info(
        "event=write_source_complete component=catalog_writer operation=run_write_source "
        "table=%s is_source=%s status=success",
        table_norm,
        value,
    )

    return WriteSourceOutput(written=result["catalog_path"], is_source=value, status="ok")


def run_write_table_slice(
    project_root: Path, proc_fqn: str, table_fqn: str, ddl_slice: str
) -> WriteSliceOutput:
    """Write a per-table DDL slice into the proc catalog."""
    path = write_proc_table_slice(project_root, proc_fqn, table_fqn, ddl_slice)
    logger.info(
        "event=write_table_slice proc=%s table=%s status=success",
        normalize(proc_fqn),
        normalize(table_fqn),
    )
    return WriteSliceOutput(written=str(path), status="ok")
