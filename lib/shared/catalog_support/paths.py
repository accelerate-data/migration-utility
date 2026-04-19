from __future__ import annotations

import json
from pathlib import Path

from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogFileMissingError
from shared.name_resolver import normalize


def _catalog_dir(project_root: Path) -> Path:
    return resolve_catalog_dir(project_root)


def _object_path(project_root: Path, object_type: str, fqn: str) -> Path:
    """Return the catalog JSON path for a given object.

    *object_type* is one of ``tables``, ``procedures``, ``views``,
    ``functions``.  *fqn* is a normalised ``schema.name`` string.
    """
    return _catalog_dir(project_root) / object_type / f"{fqn}.json"


def has_catalog(project_root: Path) -> bool:
    """Return True if a catalog directory exists with at least one file."""
    d = _catalog_dir(project_root)
    if not d.is_dir():
        return False
    return any(d.rglob("*.json"))


def resolve_catalog_path(project_root: Path, fqn: str) -> Path:
    """Resolve the catalog JSON path for a table or view FQN.

    Checks views first, then tables. Raises CatalogFileMissingError if neither exists.
    """
    catalog_dir = resolve_catalog_dir(project_root)
    view_path = catalog_dir / "views" / f"{fqn}.json"
    if view_path.exists():
        return view_path
    table_path = catalog_dir / "tables" / f"{fqn}.json"
    if table_path.exists():
        return table_path
    raise CatalogFileMissingError("table or view", fqn)


def detect_catalog_bucket(project_root: Path, fqn: str) -> str | None:
    """Return ``tables`` or ``views`` if a catalog file exists for the FQN."""
    norm = normalize(fqn)
    catalog_dir = resolve_catalog_dir(project_root)
    if (catalog_dir / "tables" / f"{norm}.json").exists():
        return "tables"
    if (catalog_dir / "views" / f"{norm}.json").exists():
        return "views"
    return None


def detect_object_type(project_root: Path, fqn: str) -> str | None:
    """Detect whether a normalized FQN refers to a table, view, or MV."""
    norm = normalize(fqn)
    bucket = detect_catalog_bucket(project_root, norm)
    if bucket == "tables":
        return "table"
    if bucket == "views":
        view_path = resolve_catalog_dir(project_root) / "views" / f"{norm}.json"
        try:
            data = json.loads(view_path.read_text(encoding="utf-8"))
            if data.get("is_materialized_view"):
                return "mv"
        except (json.JSONDecodeError, OSError, UnicodeDecodeError):
            pass
        return "view"
    return None
