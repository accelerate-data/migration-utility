from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog_support.merge import write_json
from shared.catalog_support.paths import _object_path
from shared.dmf_processing import empty_scoped
from shared.loader_data import CatalogFileMissingError, CatalogLoadError
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)


def write_table_catalog(
    project_root: Path,
    table_fqn: str,
    signals: dict[str, Any],
    referenced_by: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    *,
    ddl_hash: str | None = None,
) -> Path:
    """Write a table catalog file.  Returns the written path."""
    fqn = normalize(table_fqn)
    schema, name = fqn_parts(fqn)
    defaults: dict[str, Any] = {
        "columns": [],
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "change_capture": None,
        "sensitivity_classifications": [],
        "excluded": False,
        "is_source": False,
    }
    data: dict[str, Any] = {"schema": schema, "name": name, **defaults, **signals}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if referenced_by is not None:
        data["referenced_by"] = referenced_by
    else:
        data.setdefault(
            "referenced_by",
            {
                "procedures": empty_scoped(),
                "views": empty_scoped(),
                "functions": empty_scoped(),
            },
        )
    p = _object_path(project_root, "tables", fqn)
    write_json(p, data)
    return p


def write_proc_statements(
    project_root: Path,
    proc_fqn: str,
    statements: list[dict[str, Any]],
) -> Path:
    """Persist resolved statements into an existing procedure catalog file.

    Reads the current catalog file, sets ``statements``, and writes it back.
    Raises ``FileNotFoundError`` if the catalog file does not exist.
    """
    norm = normalize(proc_fqn)
    p = _object_path(project_root, "procedures", norm)
    if not p.exists():
        raise FileNotFoundError(f"Procedure catalog not found: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(p), exc) from exc
    data["statements"] = statements
    llm_recovered = any(stmt.get("source") == "llm" for stmt in statements if isinstance(stmt, dict))
    if llm_recovered:
        existing_errors = data.get("errors")
        if isinstance(existing_errors, list):
            filtered_errors = [
                err
                for err in existing_errors
                if not (isinstance(err, dict) and err.get("code") == "PARSE_ERROR")
            ]
            if len(filtered_errors) != len(existing_errors):
                logger.info(
                    "event=proc_parse_error_cleared component=catalog operation=write_proc_statements proc=%s status=success",
                    norm,
                )
                data["errors"] = filtered_errors
    write_json(p, data)
    return p


def write_proc_table_slice(
    project_root: Path, proc_fqn: str, table_fqn: str, ddl_slice: str
) -> Path:
    """Upsert a per-table DDL slice into the proc catalog table_slices section."""
    norm_proc = normalize(proc_fqn)
    norm_table = normalize(table_fqn)
    cat_path = _object_path(project_root, "procedures", norm_proc)
    if not cat_path.exists():
        raise CatalogFileMissingError("procedure", norm_proc)
    try:
        data = json.loads(cat_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(cat_path), exc) from exc
    data.setdefault("table_slices", {})[norm_table] = ddl_slice
    write_json(cat_path, data)
    return cat_path


def _write_catalog_json(
    project_root: Path, object_type: str, norm_fqn: str, data: dict[str, Any]
) -> Path:
    p = _object_path(project_root, object_type, norm_fqn)
    write_json(p, data)
    return p


def write_proc_catalog(
    project_root: Path,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    needs_llm: bool = False,
    needs_enrich: bool = False,
    mode: str | None = None,
    routing_reasons: list[str] | None = None,
    params: list[dict[str, Any]] | None = None,
    ddl_hash: str | None = None,
    dmf_errors: list[str] | None = None,
    segmenter_error: str | None = None,
) -> Path:
    """Write a procedure catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if params is not None:
        data["params"] = params
    if needs_llm:
        data["needs_llm"] = True
    if needs_enrich:
        data["needs_enrich"] = True
    if mode is not None:
        data["mode"] = mode
    if routing_reasons is not None:
        data["routing_reasons"] = routing_reasons
    if dmf_errors:
        data["dmf_errors"] = dmf_errors
    if segmenter_error is not None:
        data["segmenter_error"] = segmenter_error
    return _write_catalog_json(project_root, "procedures", norm, data)


def write_view_catalog(
    project_root: Path,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    sql: str | None = None,
    columns: list[dict[str, Any]] | None = None,
    is_materialized_view: bool = False,
    long_truncation: bool = False,
    ddl_hash: str | None = None,
    dmf_errors: list[str] | None = None,
    segmenter_error: str | None = None,
) -> Path:
    """Write a view catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references, "excluded": False}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if is_materialized_view:
        data["is_materialized_view"] = True
    if sql is not None:
        data["sql"] = sql
    if columns is not None:
        data["columns"] = columns
    if dmf_errors:
        data["dmf_errors"] = dmf_errors
    if segmenter_error is not None:
        data["segmenter_error"] = segmenter_error
    if long_truncation:
        data["long_truncation"] = True
    return _write_catalog_json(project_root, "views", norm, data)


def write_function_catalog(
    project_root: Path,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    subtype: str | None = None,
    ddl_hash: str | None = None,
    dmf_errors: list[str] | None = None,
    segmenter_error: str | None = None,
) -> Path:
    """Write a function catalog file.  Returns the written path."""
    norm = normalize(fqn)
    schema, name = fqn_parts(norm)
    data: dict[str, Any] = {"schema": schema, "name": name, "references": references}
    if ddl_hash is not None:
        data["ddl_hash"] = ddl_hash
    if subtype is not None:
        data["subtype"] = subtype
    if dmf_errors:
        data["dmf_errors"] = dmf_errors
    if segmenter_error is not None:
        data["segmenter_error"] = segmenter_error
    return _write_catalog_json(project_root, "functions", norm, data)
