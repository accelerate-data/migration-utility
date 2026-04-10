"""catalog_writer.py — Schema-validated catalog write-back operations.

Provides the run_write_* functions that persist agent/skill outputs
(statements, scoping, source flags, table slices) into catalog JSON
files with JSON Schema validation.

Split from discover.py for module focus.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from referencing import Registry, Resource

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

logger = logging.getLogger(__name__)

_SCHEMA_DIR = Path(__file__).with_name("schemas")


# ── Schema validation helpers ────────────────────────────────────────────────


def _load_schema_with_store(schema_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
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
    """Render jsonschema validation errors in compact, retry-friendly form."""
    parts: list[str] = []
    for error in errors:
        path = "/" + "/".join(str(segment) for segment in error.absolute_path)
        parts.append(f"{path or '/'}: {error.message}")
    return "; ".join(parts)


def _validate_schema_fragment(data: Any, schema_name: str, fragment_path: str) -> None:
    """Validate data against a schema fragment and raise a field-level ValueError."""
    schema, registry = _load_schema_with_store(schema_name)
    wrapper_schema = {
        "$schema": schema.get("$schema", "https://json-schema.org/draft/2020-12/schema"),
        "$ref": f"{schema_name}#/{fragment_path}",
    }
    validator = Draft202012Validator(wrapper_schema, registry=registry)
    errors = sorted(validator.iter_errors(data), key=lambda err: list(err.absolute_path))
    if errors:
        raise ValueError(
            f"Schema validation failed for {schema_name}#/{fragment_path}: "
            f"{_format_validation_errors(errors)}"
        )


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
    _validate_schema_fragment(statements, "procedure_catalog.json", "properties/statements")
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
    _validate_schema_fragment(scoping, "table_catalog.json", "properties/scoping")

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
    _validate_schema_fragment(scoping, "view_catalog.json", "properties/scoping")

    result = load_and_merge_catalog(project_root, view_norm, "scoping", scoping)
    return {"written": result["catalog_path"], "status": "ok"}


def run_write_source(
    project_root: Path,
    table_fqn: str,
    value: bool,
) -> dict[str, Any]:
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

    return {"written": result["catalog_path"], "is_source": value, "status": "ok"}


def run_write_table_slice(
    project_root: Path, proc_fqn: str, table_fqn: str, ddl_slice: str
) -> dict[str, Any]:
    """Write a per-table DDL slice into the proc catalog."""
    path = write_proc_table_slice(project_root, proc_fqn, table_fqn, ddl_slice)
    logger.info(
        "event=write_table_slice proc=%s table=%s status=success",
        normalize(proc_fqn),
        normalize(table_fqn),
    )
    return {"written": str(path), "status": "ok"}
