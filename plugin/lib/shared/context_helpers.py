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

from shared.catalog import load_proc_catalog, load_table_catalog
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


def collect_source_tables(project_root: Path, writer_fqn: str) -> list[str]:
    """Collect source tables from the writer procedure's references."""
    cat = load_proc_catalog(project_root, writer_fqn)
    if cat is None:
        return []
    refs = cat.get("references", {})
    tables_in_scope = refs.get("tables", {}).get("in_scope", [])
    sources = []
    for t in tables_in_scope:
        if t.get("is_selected") and not t.get("is_updated"):
            sources.append(normalize(f"{t['schema']}.{t['name']}"))
    return sorted(set(sources))


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
