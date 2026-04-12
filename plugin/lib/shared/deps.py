"""deps.py — Transitive dependency traversal for catalog objects.

Provides BFS/DFS traversal of catalog reference graphs to compute the
full set of upstream dependencies for a given table or view.

Split from batch_plan.py for module focus.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from shared.catalog import load_proc_catalog, load_table_catalog, load_view_catalog
from shared.catalog_models import ReferencesBucket, RefEntry, ScopedRefList
from shared.loader_data import CatalogLoadError
from shared.name_resolver import model_name_from_table, normalize

logger = logging.getLogger(__name__)

_MAX_DEPTH = 20  # traversal depth limit for cycle prevention


# ── Low-level helpers ────────────────────────────────────────────────────────


def _iter_in_scope(refs: ReferencesBucket | None, kind: str) -> list[RefEntry]:
    """Return the in_scope list for a reference kind (tables/views/procedures)."""
    if refs is None:
        return []
    scoped: ScopedRefList | None = getattr(refs, kind, None)
    if scoped is None:
        return []
    return scoped.in_scope


def _ref_fqn(entry: RefEntry) -> str:
    """Build a normalized FQN from a catalog reference entry."""
    schema = entry.object_schema
    name = entry.name
    if not schema:
        logger.debug(
            "event=ref_fqn_no_schema component=deps name=%s "
            "detail=schema_missing_in_reference_entry",
            name,
        )
    raw = f"{schema}.{name}" if schema else name
    return normalize(raw)


def _locate_dbt_model(dbt_root: Path, model_name: str) -> Optional[Path]:
    """Find a dbt model .sql file anywhere under dbt/models/."""
    models_dir = dbt_root / "models"
    if not models_dir.is_dir():
        return None
    matches = list(models_dir.rglob(f"{model_name}.sql"))
    return matches[0] if matches else None


def _has_dbt_model(fqn: str, dbt_root: Path) -> bool:
    """Return True if a migrated dbt model exists for this FQN."""
    return _locate_dbt_model(dbt_root, model_name_from_table(fqn)) is not None


# ── Dependency traversal ─────────────────────────────────────────────────────


def _expand_view_refs(
    project_root: Path,
    view_fqn: str,
    visited: set[str],
    depth: int,
) -> set[str]:
    """Return all table/view FQNs that a view transitively references (in_scope only)."""
    if depth >= _MAX_DEPTH or view_fqn in visited:
        return set()
    visited.add(view_fqn)

    try:
        cat = load_view_catalog(project_root, view_fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return set()
    if cat is None:
        return set()

    refs = cat.references
    deps: set[str] = set()

    for entry in _iter_in_scope(refs, "tables"):
        fqn = _ref_fqn(entry)
        if fqn:
            deps.add(fqn)

    for entry in _iter_in_scope(refs, "views"):
        fqn = _ref_fqn(entry)
        if fqn:
            deps.add(fqn)
            deps.update(_expand_view_refs(project_root, fqn, visited, depth + 1))

    return deps


def _expand_proc_refs(
    project_root: Path,
    proc_fqn: str,
    visited_procs: set[str],
    visited_views: set[str],
    depth: int,
) -> set[str]:
    """Return all table/view FQNs that a proc transitively reads from (in_scope only).

    Traverses proc -> tables, proc -> views -> tables, and proc -> procs recursively.
    """
    if depth >= _MAX_DEPTH or proc_fqn in visited_procs:
        return set()
    visited_procs.add(proc_fqn)

    try:
        cat = load_proc_catalog(project_root, proc_fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return set()
    if cat is None:
        return set()

    refs = cat.references
    deps: set[str] = set()

    for entry in _iter_in_scope(refs, "tables"):
        fqn = _ref_fqn(entry)
        if fqn:
            deps.add(fqn)

    for entry in _iter_in_scope(refs, "views"):
        fqn = _ref_fqn(entry)
        if fqn:
            deps.add(fqn)
            deps.update(_expand_view_refs(project_root, fqn, visited_views, depth + 1))

    for entry in _iter_in_scope(refs, "procedures"):
        called = _ref_fqn(entry)
        if called:
            deps.update(
                _expand_proc_refs(
                    project_root, called, visited_procs, visited_views, depth + 1
                )
            )

    return deps


def collect_deps(
    project_root: Path,
    fqn: str,
    obj_type: str,
) -> set[str]:
    """Return the full transitive in-scope dependency set for an object.

    For tables: traverses the writer proc's references recursively.
    For views/MVs: traverses the view's references recursively.
    """
    if obj_type in ("view", "mv"):
        return _expand_view_refs(project_root, fqn, set(), 0)

    try:
        cat = load_table_catalog(project_root, fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return set()
    if cat is None:
        return set()

    writer = cat.scoping.selected_writer if cat.scoping else None
    if not writer:
        return set()

    deps = _expand_proc_refs(project_root, normalize(writer), set(), set(), 0)
    deps.discard(fqn)  # strip self-reference (MERGE/TRUNCATE+INSERT patterns read own target)
    return deps
