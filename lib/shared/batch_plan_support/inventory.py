"""Catalog inventory enumeration for batch-plan construction."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from shared.catalog import load_table_catalog, load_view_catalog
from shared.env_config import resolve_catalog_dir
from shared.loader_data import CatalogLoadError


@dataclass
class _CatalogInventory:
    """Result of enumerating and classifying all catalog objects."""

    table_fqns: list[str] = field(default_factory=list)
    view_entries: list[tuple[str, str]] = field(default_factory=list)
    excluded_fqns: set[str] = field(default_factory=set)
    source_table_fqns: list[str] = field(default_factory=list)
    seed_table_fqns: list[str] = field(default_factory=list)
    source_pending_fqns: list[str] = field(default_factory=list)

    @property
    def all_objects(self) -> list[tuple[str, str]]:
        return [(fqn, "table") for fqn in self.table_fqns] + self.view_entries

    @property
    def obj_type_map(self) -> dict[str, str]:
        return {fqn: t for fqn, t in self.all_objects}


def _enumerate_catalog(project_root: Path) -> _CatalogInventory:
    """Classify all catalog objects into pipeline buckets."""
    catalog_dir = resolve_catalog_dir(project_root)
    inv = _CatalogInventory()

    table_dir = catalog_dir / "tables"
    if table_dir.is_dir():
        for p in sorted(table_dir.glob("*.json")):
            fqn = p.stem
            try:
                cat = load_table_catalog(project_root, fqn)
            except (json.JSONDecodeError, OSError, CatalogLoadError):
                cat = None
            if cat is not None and cat.excluded:
                inv.excluded_fqns.add(fqn)
            elif cat is not None and cat.is_seed:
                inv.seed_table_fqns.append(fqn)
            elif cat is not None and cat.is_source:
                inv.source_table_fqns.append(fqn)
            else:
                # Keep confirmed writerless tables in the main inventory so
                # pipeline_status can classify them as n_a instead of pending.
                inv.table_fqns.append(fqn)

    view_dir = catalog_dir / "views"
    if view_dir.is_dir():
        for p in sorted(view_dir.glob("*.json")):
            fqn = p.stem
            try:
                cat = load_view_catalog(project_root, fqn)
                obj_type = "mv" if cat is not None and cat.is_materialized_view else "view"
            except (json.JSONDecodeError, OSError, CatalogLoadError):
                cat = None
                obj_type = "view"
            if cat is not None and cat.excluded:
                inv.excluded_fqns.add(fqn)
            else:
                inv.view_entries.append((fqn, obj_type))

    return inv
