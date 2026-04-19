from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.catalog_models import FunctionCatalog, ProcedureCatalog, TableCatalog, ViewCatalog
from shared.catalog_support.paths import _object_path
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize


def _load_catalog_file(project_root: Path, object_type: str, fqn: str) -> dict[str, Any] | None:
    """Load a single catalog JSON file, or ``None`` if absent."""
    p = _object_path(project_root, object_type, normalize(fqn))
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CatalogLoadError(str(p), exc) from exc


def load_table_catalog(project_root: Path, table_fqn: str) -> TableCatalog | None:
    data = _load_catalog_file(project_root, "tables", table_fqn)
    return TableCatalog.model_validate(data) if data is not None else None


def load_proc_catalog(project_root: Path, proc_fqn: str) -> ProcedureCatalog | None:
    data = _load_catalog_file(project_root, "procedures", proc_fqn)
    return ProcedureCatalog.model_validate(data) if data is not None else None


def load_view_catalog(project_root: Path, view_fqn: str) -> ViewCatalog | None:
    data = _load_catalog_file(project_root, "views", view_fqn)
    return ViewCatalog.model_validate(data) if data is not None else None


def load_function_catalog(project_root: Path, func_fqn: str) -> FunctionCatalog | None:
    data = _load_catalog_file(project_root, "functions", func_fqn)
    return FunctionCatalog.model_validate(data) if data is not None else None


def read_selected_writer(project_root: Path, table_fqn: str) -> str | None:
    """Read selected_writer from the scoping section of a table catalog file.

    Returns None if the table catalog doesn't exist or has no scoping section
    or scoping.selected_writer is not set.
    """
    cat = load_table_catalog(project_root, table_fqn)
    if cat is None:
        return None
    if cat.scoping is None:
        return None
    return cat.scoping.selected_writer
