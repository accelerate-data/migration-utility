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
    profile = cat.get("profile")
    if profile is None:
        raise ProfileMissingError(table_fqn)
    return profile


def load_proc_statements(project_root: Path, writer_fqn: str) -> list[dict[str, Any]]:
    """Load resolved statements from a procedure catalog file."""
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        raise CatalogFileMissingError("procedure", writer_fqn)
    statements = cat.get("statements")
    if statements is None:
        raise ValueError(f"Procedure catalog for {writer_fqn} has no 'statements' section")
    return statements


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
    if cat and cat.get("columns"):
        return cat["columns"]
    return []


def load_source_columns(project_root: Path, source_fqn: str) -> list[dict[str, Any]]:
    """Load column metadata for a source table or view.

    Checks catalog/tables/<fqn>.json first; falls back to catalog/views/<fqn>.json.
    Returns an empty list when neither catalog file exists.
    """
    cat = load_table_catalog(project_root, source_fqn)
    if cat:
        if cat.get("columns"):
            return cat["columns"]
        logger.warning("event=source_columns_empty source=%s catalog=tables", source_fqn)
    cat = load_view_catalog(project_root, source_fqn)
    if cat:
        if cat.get("columns"):
            return cat["columns"]
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
    refs = cat.get("references", {})
    sources: list[str] = []
    for section in ("tables", "views"):
        for entry in refs.get(section, {}).get("in_scope", []):
            if entry.get("is_selected") and not entry.get("is_updated"):
                sources.append(normalize(f"{entry['schema']}.{entry['name']}"))
    return sorted(set(sources))


def load_object_columns(project_root: Path, fqn: str) -> list[dict[str, Any]]:
    """Load column list for a table or view, trying tables first.

    This provides transparent catalog access — callers don't need to know
    whether the FQN refers to a table or a view.
    """
    cat = load_table_catalog(project_root, fqn)
    if cat and cat.get("columns"):
        return cat["columns"]
    cat = load_view_catalog(project_root, fqn)
    if cat and cat.get("columns"):
        return cat["columns"]
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
    sql = cat.get("sql")
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
