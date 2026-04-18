"""context_helpers.py — Shared catalog context loaders.

Used by migrate.py, refactor.py, and dry_run_content.py to load catalog
sections needed for context assembly.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import sqlglot

logger = logging.getLogger(__name__)

from shared.catalog import load_proc_catalog, load_table_catalog, load_view_catalog
from shared.catalog_models import RefEntry, ReferencesBucket, ScopedRefList
from shared.loader import (
    CatalogFileMissingError,
    ProfileMissingError,
    collect_refs_from_statements,
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
        return target_visible_columns(cat.columns)
    return []


def resolve_selected_writer_ddl_slice(proc_cat: Any, table_fqn: str, writer_fqn: str) -> str | None:
    """Return the target-specific slice, or fail if a sliced writer lacks it."""
    table_norm = normalize(table_fqn)
    writer_norm = normalize(writer_fqn)
    table_slices = proc_cat.table_slices or {}
    if not table_slices:
        return None
    selected_slice = table_slices.get(table_norm)
    if selected_slice:
        return selected_slice
    raise ValueError(
        f"Writer {writer_norm} has table_slices, but no slice exists for target {table_norm}. "
        "Re-run slice extraction or write the table slice before continuing."
    )


def project_sql_dialect(project_root: Path) -> str:
    """Return the SQL dialect configured for a migration project."""
    return str(read_manifest(project_root).get("dialect", "tsql"))


def collect_source_tables_from_sql(sql: str, dialect: str = "tsql") -> list[str]:
    """Collect source tables from selected SQL instead of full writer metadata."""
    statements = sqlglot.parse(sql, dialect=dialect, error_level=sqlglot.ErrorLevel.IGNORE)
    refs = collect_refs_from_statements([stmt for stmt in statements if stmt is not None], dialect=dialect)
    return sorted(set(refs.reads_from))


def references_from_selected_sql(sql: str, dialect: str = "tsql") -> ReferencesBucket:
    """Build writer reference evidence from selected SQL only."""
    table_refs: list[RefEntry] = []
    for fqn in collect_source_tables_from_sql(sql, dialect=dialect):
        schema, _, name = fqn.rpartition(".")
        table_refs.append(
            RefEntry(schema=schema, name=name, is_selected=True, is_updated=False)
        )
    return ReferencesBucket(tables=ScopedRefList(in_scope=table_refs, out_of_scope=[]))


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
        return target_visible_columns(cat.columns)
    vcat = load_view_catalog(project_root, fqn)
    if vcat and vcat.columns:
        return target_visible_columns(vcat.columns)
    return []


def target_visible_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return column metadata suitable for target-facing contexts and prompts."""
    visible: list[dict[str, Any]] = []
    hidden = {"source_sql_type", "canonical_tsql_type"}
    for column in columns:
        if not isinstance(column, dict):
            visible.append(column)
            continue
        visible.append({key: value for key, value in column.items() if key not in hidden})
    return visible


def sandbox_metadata(project_root: Path) -> dict[str, Any] | None:
    """Read sandbox metadata from manifest."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = read_manifest(project_root)
    except (ValueError, OSError):
        return None
    runtime = manifest.get("runtime") or {}
    sandbox = runtime.get("sandbox")
    if not isinstance(sandbox, dict):
        return None
    return sandbox.get("connection")


def load_test_spec(project_root: Path, table_fqn: str) -> dict[str, Any] | None:
    """Load a test spec file if it exists."""
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
