"""DDL loading orchestration for shared.loader_io."""

from __future__ import annotations

import logging
from pathlib import Path

from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogNotFoundError, DdlCatalog
from shared.loader_io_support.directory import load_directory
from shared.loader_io_support.indexing import load_catalog
from shared.loader_io_support.manifest import read_manifest

logger = logging.getLogger(__name__)


def load_ddl(project_root: Path) -> tuple[DdlCatalog, str]:
    """Load a DdlCatalog and dialect from a project root directory.

    Requires a ``catalog/`` directory (from setup-ddl) containing per-object
    JSON files. Raises ``CatalogNotFoundError`` if the directory is missing or
    empty.

    Two loading strategies:

    1. **Pre-built index** — if ``catalog.json`` exists (written by
       ``index_directory``), loads from that flat reference index.  This is
       faster but does not include sqlglot AST data.
    2. **Live parse** — parses ``.sql`` DDL files directly via sqlglot.

    The ``catalog/`` directory guard ensures we don't silently use a stale
    ``catalog.json`` from a different project state.
    """

    manifest = read_manifest(project_root)
    dialect = manifest.get("dialect", "tsql")
    catalog_dir = resolve_catalog_dir(project_root)
    if not catalog_dir.is_dir() or not any(catalog_dir.rglob("*.json")):
        logger.error("event=load_ddl operation=check_catalog project_root=%s reason=no_catalog_directory", project_root)
        raise CatalogNotFoundError(project_root)
    catalog_json = project_root / "catalog.json"
    if catalog_json.exists():
        return load_catalog(project_root), dialect
    return load_directory(project_root, dialect=dialect), dialect
