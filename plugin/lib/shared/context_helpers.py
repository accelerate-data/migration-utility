"""context_helpers.py — Shared catalog context loaders.

Used by migrate.py, refactor.py, and dry_run_content.py to load catalog
sections needed for context assembly.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from shared.catalog import load_proc_catalog, load_table_catalog, load_view_catalog
from shared.loader import (
    CatalogFileMissingError,
    ProfileMissingError,
    load_directory,
)
from shared.loader_io import read_manifest
from shared.name_resolver import normalize


def load_table_profile(project_root: Path, table_fqn: str) -> dict[str, Any]:
    """Load the profile section from a table catalog file."""
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        raise CatalogFileMissingError("table", table_fqn)
    if cat.profile is None:
        raise ProfileMissingError(table_fqn)
    return cat.profile


def load_proc_statements(project_root: Path, writer_fqn: str) -> list[dict[str, Any]]:
    """Load resolved statements from a procedure catalog file."""
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        raise CatalogFileMissingError("procedure", writer_fqn)
    return cat.statements


def load_proc_body(project_root: Path, writer_fqn: str) -> str:
    """Load the raw DDL body of a procedure from the DDL directory."""
    catalog = load_directory(project_root)
    entry = catalog.get_procedure(writer_fqn)
    if entry is None:
        raise CatalogFileMissingError("procedure DDL", writer_fqn)
    return entry.raw_ddl


def load_table_columns(project_root: Path, table_fqn: str) -> list[dict[str, Any]]:
    """Load column list from the table catalog file."""
    cat = load_table_catalog(project_root, table_fqn)
    if cat and cat.columns:
        return cat.columns
    return []


def load_source_columns(project_root: Path, source_fqn: str) -> list[dict[str, Any]]:
    """Load column metadata for a source table or view.

    Checks catalog/tables/<fqn>.json first; falls back to catalog/views/<fqn>.json.
    Returns an empty list when neither catalog file exists.
    """
    cat = load_table_catalog(project_root, source_fqn)
    if cat:
        if cat.columns:
            return cat.columns
        logger.warning("event=source_columns_empty source=%s catalog=tables", source_fqn)
    vcat = load_view_catalog(project_root, source_fqn)
    if vcat:
        if vcat.columns:
            return vcat.columns
        logger.warning("event=source_columns_empty source=%s catalog=views", source_fqn)
    return []


def collect_source_tables(project_root: Path, writer_fqn: str) -> list[str]:
    """Collect source tables and views from the writer procedure's references.

    Returns a flat list of normalized FQNs for both tables and views that the
    writer reads from (is_selected=True, is_updated=False).  Views are included
    transparently so downstream consumers (skills, commands) don't need to
    distinguish between table and view sources.
    """
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        return []
    refs = cat.references
    if refs is None:
        return []
    sources: list[str] = []
    for scoped in (refs.tables, refs.views):
        for entry in scoped.in_scope:
            if entry.is_selected and not entry.is_updated:
                sources.append(normalize(f"{entry.object_schema}.{entry.name}"))
    return sorted(set(sources))


def collect_view_source_tables(project_root: Path, view_fqn: str) -> list[str]:
    """Collect source table FQNs from a view's references.

    Unlike ``collect_source_tables`` for procedures, view references do not
    carry ``is_selected``/``is_updated`` flags from the DMF, so all in-scope
    table and view refs are collected unconditionally.
    """
    cat = load_view_catalog(project_root, view_fqn)
    if cat is None:
        return []
    refs = cat.references
    if refs is None:
        return []
    sources: list[str] = []
    for scoped in (refs.tables, refs.views):
        for entry in scoped.in_scope:
            sources.append(normalize(f"{entry.object_schema}.{entry.name}"))
    return sorted(set(sources))


def load_object_columns(project_root: Path, fqn: str) -> list[dict[str, Any]]:
    """Load column list for a table or view, trying tables first.

    This provides transparent catalog access — callers don't need to know
    whether the FQN refers to a table or a view.
    """
    cat = load_table_catalog(project_root, fqn)
    if cat and cat.columns:
        return cat.columns
    vcat = load_view_catalog(project_root, fqn)
    if vcat and vcat.columns:
        return vcat.columns
    return []


def sandbox_metadata(project_root: Path) -> dict[str, Any] | None:
    """Read sandbox metadata from manifest."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = read_manifest(project_root)
    except (ValueError, OSError):
        return None
    return manifest.get("sandbox")


def load_view_sql(project_root: Path, view_fqn: str) -> str | None:
    """Load the SQL body of a view from the view catalog file.

    Returns the view's SQL string, or None if the catalog file doesn't exist
    or has no 'sql' key.
    """
    cat = load_view_catalog(project_root, view_fqn)
    if cat is None:
        return None
    sql = (cat.model_extra or {}).get("sql")
    if not sql:
        logger.warning("event=view_sql_empty view=%s", view_fqn)
    return sql


def load_test_spec(project_root: Path, table_fqn: str) -> dict[str, Any] | None:
    """Load a test spec file if it exists."""
    import logging as _logging
    _log = _logging.getLogger(__name__)
    norm = normalize(table_fqn)
    spec_path = project_root / "test-specs" / f"{norm}.json"
    if not spec_path.exists():
        return None
    try:
        return json.loads(spec_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(
            "event=load_test_spec_failed operation=load table=%s path=%s error=%s",
            norm, spec_path, exc,
        )
        return None
