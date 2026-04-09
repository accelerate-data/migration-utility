"""batch_plan.py — Dependency-aware parallel batch scheduler for migration.

Reads all catalog files, builds a transitive dependency graph
(proc → tables, proc → views → underlying tables, proc → procs,
view → views transitively), and computes maximally-parallel execution
batches respecting the migration pipeline.

Every object node carries its current pipeline status and catalog
diagnostics so the LLM can interpret the output without re-reading
catalog files.

Exposed as: migrate-util batch-plan  (subcommand on dry_run.app)

Output schema: schemas/batch_plan_output.json

Exit codes:
    0  success
    1  domain failure (no catalog files, bad project root)
    2  IO or parse error
"""

from __future__ import annotations

import json
import logging
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from shared.catalog import load_proc_catalog, load_table_catalog, load_view_catalog
from shared.catalog_models import ReferencesBucket, RefEntry, ScopedRefList
from shared.env_config import resolve_catalog_dir, resolve_dbt_project_path
from shared.loader_data import CatalogLoadError
from shared.name_resolver import fqn_parts, normalize

logger = logging.getLogger(__name__)

_MAX_DEPTH = 20  # traversal depth limit for cycle prevention

# Maps diagnostic codes to the pipeline stage most impacted by that diagnostic.
# Used to pre-compute diagnostic_stage_flags per object node so the display layer
# does not need to reason about code→stage mappings.
_DIAG_STAGE_MAP: dict[str, str] = {
    "PARSE_ERROR": "refactor",
    "DDL_PARSE_ERROR": "refactor",
    "MULTI_TABLE_WRITE": "scope",
    "REMOTE_EXEC_UNSUPPORTED": "scope",
}

# Severity rank for worst-severity promotion. Higher rank wins.
_SEV_RANK: dict[str, int] = {"warning": 0, "error": 1}


def _compute_diagnostic_stage_flags(diagnostics: list[dict[str, Any]]) -> dict[str, str]:
    """Map a node's diagnostics to their most-impacted pipeline stage.

    Returns a dict of {stage: worst_severity} for stages with at least one
    relevant diagnostic, e.g. {"refactor": "error"} or {"scope": "warning"}.
    The highest-ranked severity for each stage wins (error > warning).
    Unknown severity values are treated as lower than warning and do not
    overwrite a known severity.
    """
    flags: dict[str, str] = {}
    for d in diagnostics:
        stage = _DIAG_STAGE_MAP.get(d.get("code", ""))
        if not stage:
            continue
        sev = d.get("severity", "warning")
        if _SEV_RANK.get(sev, -1) > _SEV_RANK.get(flags.get(stage, ""), -1):
            flags[stage] = sev
    return flags


# ── Low-level helpers ─────────────────────────────────────────────────────────


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
            "event=ref_fqn_no_schema component=batch_plan name=%s "
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


def _model_name_for(fqn: str) -> str:
    """Derive the expected dbt staging model name for a FQN (tables and views)."""
    _, name = fqn_parts(fqn)
    return f"stg_{name}"


def _has_dbt_model(fqn: str, dbt_root: Path) -> bool:
    """Return True if a migrated dbt model exists for this FQN."""
    return _locate_dbt_model(dbt_root, _model_name_for(fqn)) is not None


# ── Pipeline status ───────────────────────────────────────────────────────────


def object_pipeline_status(
    project_root: Path,
    fqn: str,
    obj_type: str,
    dbt_root: Path,
) -> str:
    """Determine the first incomplete pipeline stage for an object.

    Uses direct status field reads from catalog files rather than inference.

    Returns one of:
        scope_needed    — not yet scoped / view not yet analyzed
        profile_needed  — scoped but not profiled
        test_gen_needed — profiled but test-gen not complete
        refactor_needed — test-gen complete but writer not refactored
        migrate_needed  — refactored but no dbt model / generate not ok
        complete        — dbt model exists
        n_a             — writerless table (writer-dependent stages N/A)
    """
    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            return "scope_needed"
        if cat is None:
            return "scope_needed"
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status != "analyzed":
            return "scope_needed"
        profile_status = cat.profile.status if cat.profile else None
        if profile_status not in ("ok", "partial"):
            return "profile_needed"
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return "test_gen_needed"
        refactor_status = cat.refactor.status if cat.refactor else None
        if refactor_status != "ok":
            return "refactor_needed"
        generate_status = cat.generate.status if cat.generate else None
        if generate_status != "ok":
            return "migrate_needed"
        return "complete"

    # TABLE
    try:
        cat = load_table_catalog(project_root, fqn)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return "scope_needed"
    if cat is None:
        return "scope_needed"

    scoping_status = cat.scoping.status if cat.scoping else None
    if scoping_status == "no_writer_found":
        return "n_a"
    if scoping_status != "resolved":
        return "scope_needed"

    profile_status = cat.profile.status if cat.profile else None
    if profile_status not in ("ok", "partial"):
        return "profile_needed"

    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return "test_gen_needed"

    writer = cat.scoping.selected_writer if cat.scoping else None
    if writer:
        writer_norm = normalize(writer)
        try:
            proc_cat = load_proc_catalog(project_root, writer_norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            proc_cat = None
        refactor_status = proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        if refactor_status != "ok":
            return "refactor_needed"

    generate_status = cat.generate.status if cat.generate else None
    if generate_status != "ok":
        return "migrate_needed"

    return "complete"


# ── Dependency traversal ──────────────────────────────────────────────────────


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

    Traverses proc → tables, proc → views → tables, and proc → procs recursively.
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


# ── Diagnostics ───────────────────────────────────────────────────────────────


def collect_object_diagnostics(
    project_root: Path,
    fqn: str,
    obj_type: str,
) -> list[dict[str, Any]]:
    """Collect all warnings and errors from a catalog object and its sub-sections."""
    diagnostics: list[dict[str, Any]] = []

    def _gather(source: Any) -> None:
        if not source:
            return
        for entry in getattr(source, "warnings", None) or []:
            if isinstance(entry, dict) and "severity" not in entry:
                entry = {**entry, "severity": "warning"}
            diagnostics.append(entry)
        for entry in getattr(source, "errors", None) or []:
            if isinstance(entry, dict) and "severity" not in entry:
                entry = {**entry, "severity": "error"}
            diagnostics.append(entry)

    try:
        if obj_type in ("view", "mv"):
            cat = load_view_catalog(project_root, fqn)
            if cat is None:
                return diagnostics
            _gather(cat)
            _gather(cat.scoping)
        else:
            cat = load_table_catalog(project_root, fqn)
            if cat is None:
                return diagnostics
            _gather(cat)
            _gather(cat.scoping)
            _gather(cat.profile)
            _gather(cat.refactor)
            writer = cat.scoping.selected_writer if cat.scoping else None
            if writer:
                try:
                    proc_cat = load_proc_catalog(project_root, normalize(writer))
                    if proc_cat is not None:
                        _gather(proc_cat)
                        _gather(proc_cat.refactor)
                except (json.JSONDecodeError, OSError, CatalogLoadError):
                    pass
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        pass

    return diagnostics


# ── Topological batch computation ─────────────────────────────────────────────


def _topological_batches(
    fqns: list[str],
    blocking: dict[str, set[str]],
) -> list[list[str]]:
    """Compute maximally-parallel execution batches using Kahn's algorithm.

    Objects with no blocking deps go in batch 0.
    Objects whose blocking deps are all in batch 0 go in batch 1, etc.

    Any objects remaining after the algorithm (cycle members) are omitted —
    callers detect them by comparing the output to the input list.
    """
    fqn_set = set(fqns)
    # Restrict blocking deps to only those within fqn_set
    restricted: dict[str, set[str]] = {
        fqn: blocking.get(fqn, set()) & fqn_set for fqn in fqns
    }
    in_degree = {fqn: len(restricted[fqn]) for fqn in fqns}

    # Reverse map: dep -> objects that depend on it
    dependents: dict[str, set[str]] = {fqn: set() for fqn in fqns}
    for fqn in fqns:
        for dep in restricted[fqn]:
            dependents[dep].add(fqn)

    ready: deque[str] = deque(fqn for fqn in fqns if in_degree[fqn] == 0)
    batches: list[list[str]] = []

    while ready:
        batch: list[str] = []
        next_ready: deque[str] = deque()
        while ready:
            fqn = ready.popleft()
            batch.append(fqn)
        batches.append(sorted(batch))
        for fqn in batch:
            for dependent in dependents.get(fqn, set()):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    next_ready.append(dependent)
        ready = next_ready

    return batches


# ── Catalog inventory ────────────────────────────────────────────────────────


@dataclass
class _CatalogInventory:
    """Result of enumerating and classifying all catalog objects."""

    table_fqns: list[str] = field(default_factory=list)
    view_entries: list[tuple[str, str]] = field(default_factory=list)
    excluded_fqns: set[str] = field(default_factory=set)
    source_table_fqns: list[str] = field(default_factory=list)
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
            elif cat is not None and cat.is_source:
                inv.source_table_fqns.append(fqn)
            elif cat is not None and cat.scoping and cat.scoping.status == "no_writer_found":
                inv.source_pending_fqns.append(fqn)
            else:
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


# ── Phase classification ────────────────────────────────────────────────────


def _classify_phases(
    all_objects: list[tuple[str, str]],
    statuses: dict[str, str],
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    """Sort objects into pipeline phases based on their status.

    Returns (scope_phase, profile_phase, migrate_candidates, completed, n_a).
    """
    scope: list[str] = []
    profile: list[str] = []
    migrate: list[str] = []
    completed: list[str] = []
    n_a: list[str] = []

    for fqn, _ in all_objects:
        s = statuses[fqn]
        if s == "scope_needed":
            scope.append(fqn)
        elif s == "profile_needed":
            profile.append(fqn)
        elif s in ("test_gen_needed", "refactor_needed", "migrate_needed"):
            migrate.append(fqn)
        elif s == "complete":
            completed.append(fqn)
        elif s == "n_a":
            n_a.append(fqn)

    return scope, profile, migrate, completed, n_a


# ── Blocking dependency computation ─────────────────────────────────────────


def _compute_blocking_deps(
    migrate_candidates: list[str],
    raw_deps: dict[str, set[str]],
    dbt_status: dict[str, bool],
    writerless_fqns: set[str],
) -> dict[str, set[str]]:
    """Compute blocking deps for each migration candidate.

    A dep is blocking if it has no dbt model AND is not "covered" by a
    complete intermediate node (one that already has a dbt model). Writerless
    (n_a) tables are source tables referenced via {{ source() }}, so they
    never block.
    """
    blocking: dict[str, set[str]] = {}
    for fqn in migrate_candidates:
        covered: set[str] = set()
        for dep in raw_deps.get(fqn, set()):
            if dbt_status.get(dep, False):
                covered |= raw_deps.get(dep, set())
        blocking[fqn] = {
            d for d in raw_deps.get(fqn, set())
            if not dbt_status.get(d, False) and d not in covered and d not in writerless_fqns
        }
    return blocking


# ── Output node builders ────────────────────────────────────────────────────


def _make_node(
    fqn: str,
    obj_type_map: dict[str, str],
    statuses: dict[str, str],
    dbt_status: dict[str, bool],
    raw_deps: dict[str, set[str]],
    blocking: dict[str, set[str]],
    obj_diagnostics: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Build an output node dict for a single catalog object."""
    diags = obj_diagnostics.get(fqn, [])
    return {
        "fqn": fqn,
        "type": obj_type_map[fqn],
        "pipeline_status": statuses[fqn],
        "has_dbt_model": dbt_status[fqn],
        "direct_deps": sorted(raw_deps.get(fqn, set())),
        "blocking_deps": sorted(blocking.get(fqn, set())),
        "diagnostics": diags,
        "diagnostic_stage_flags": _compute_diagnostic_stage_flags(diags),
    }


def _resolve_excluded_type(project_root: Path, fqn: str) -> str:
    """Determine object type for an excluded FQN."""
    catalog_dir = resolve_catalog_dir(project_root)
    if (catalog_dir / "tables" / f"{fqn}.json").exists():
        return "table"
    try:
        cat = load_view_catalog(project_root, fqn)
        return "mv" if cat is not None and cat.is_materialized_view else "view"
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        return "view"


def _build_plan_output(
    *,
    inv: _CatalogInventory,
    project_root: Path,
    scope_phase: list[dict[str, Any]] | None = None,
    profile_phase: list[dict[str, Any]] | None = None,
    migrate_batches: list[dict[str, Any]] | None = None,
    completed_objects: list[dict[str, Any]] | None = None,
    n_a_fqns: list[str] | None = None,
    obj_type_map: dict[str, str] | None = None,
    circular_refs: list[dict[str, Any]] | None = None,
    all_errors: list[dict[str, Any]] | None = None,
    all_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the final batch plan output dict.

    Single source of truth for the output schema — all return paths use this.
    """
    _obj_type_map = obj_type_map or {}
    return {
        "summary": {
            "total_objects": len(inv.all_objects),
            "tables": len(inv.table_fqns),
            "views": sum(1 for _, t in inv.view_entries if t == "view"),
            "mvs": sum(1 for _, t in inv.view_entries if t == "mv"),
            "writerless_tables": len(n_a_fqns) if n_a_fqns is not None else 0,
            "excluded_count": len(inv.excluded_fqns),
            "source_tables": len(inv.source_table_fqns),
            "source_pending": len(inv.source_pending_fqns),
        },
        "scope_phase": scope_phase or [],
        "profile_phase": profile_phase or [],
        "migrate_batches": migrate_batches or [],
        "completed_objects": completed_objects or [],
        "n_a_objects": [
            {"fqn": fqn, "type": _obj_type_map.get(fqn, "table"), "reason": "writerless"}
            for fqn in sorted(n_a_fqns or [])
        ],
        "excluded_objects": [
            {
                "fqn": fqn,
                "type": _resolve_excluded_type(project_root, fqn),
                "note": "excluded from pipeline",
            }
            for fqn in sorted(inv.excluded_fqns)
        ],
        "source_tables": [
            {"fqn": fqn, "type": "table", "reason": "is_source"}
            for fqn in sorted(inv.source_table_fqns)
        ],
        "source_pending": [
            {"fqn": fqn, "type": "table"}
            for fqn in sorted(inv.source_pending_fqns)
        ],
        "circular_refs": circular_refs or [],
        "catalog_diagnostics": {
            "total_errors": len(all_errors or []),
            "total_warnings": len(all_warnings or []),
            "errors": all_errors or [],
            "warnings": all_warnings or [],
        },
    }


# ── Main entry point ──────────────────────────────────────────────────────────


def build_batch_plan(project_root: Path, dbt_root: Path | None = None) -> dict[str, Any]:
    """Build a dependency-aware parallel batch plan for all catalog objects.

    Reads all table and view catalog files, computes transitive dependencies,
    and returns a structured plan grouping objects by pipeline phase and,
    within the migration phase, by dependency-ordered parallel batches.

    Returns a dict matching schemas/batch_plan_output.json.
    """
    if dbt_root is None:
        dbt_root = resolve_dbt_project_path(project_root)

    logger.info(
        "event=batch_plan_start component=batch_plan operation=build_batch_plan "
        "project_root=%s",
        project_root,
    )

    # ── Enumerate all catalog objects ─────────────────────────────────────
    inv = _enumerate_catalog(project_root)

    if not inv.all_objects:
        logger.info(
            "event=batch_plan_complete component=batch_plan operation=build_batch_plan "
            "status=%s total_objects=0 source_tables=%d source_pending=%d",
            "empty" if not inv.excluded_fqns and not inv.source_table_fqns and not inv.source_pending_fqns else "success",
            len(inv.source_table_fqns),
            len(inv.source_pending_fqns),
        )
        return _build_plan_output(inv=inv, project_root=project_root)

    obj_type_map = inv.obj_type_map
    all_fqns = set(obj_type_map)

    # ── Determine pipeline status and dbt model presence ──────────────────
    statuses: dict[str, str] = {}
    dbt_status: dict[str, bool] = {}
    for fqn, obj_type in inv.all_objects:
        statuses[fqn] = object_pipeline_status(project_root, fqn, obj_type, dbt_root)
        dbt_status[fqn] = _has_dbt_model(fqn, dbt_root)

    # ── Build transitive dependency graph ─────────────────────────────────
    raw_deps: dict[str, set[str]] = {}
    for fqn, obj_type in inv.all_objects:
        raw_deps[fqn] = collect_deps(project_root, fqn, obj_type) & all_fqns

    # ── Collect diagnostics ────────────────────────────────────────────────
    obj_diagnostics: dict[str, list[dict[str, Any]]] = {
        fqn: collect_object_diagnostics(project_root, fqn, obj_type)
        for fqn, obj_type in inv.all_objects
    }

    # ── Classify objects into pipeline phases ──────────────────────────────
    scope_phase, profile_phase, migrate_candidates, completed_objects, n_a_objects = (
        _classify_phases(inv.all_objects, statuses)
    )

    # ── Compute blocking deps ─────────────────────────────────────────────
    writerless_fqns = {fqn for fqn, _ in inv.all_objects if statuses[fqn] == "n_a"}
    blocking = _compute_blocking_deps(migrate_candidates, raw_deps, dbt_status, writerless_fqns)

    # For the topological sort restrict to blocking among migrate_candidates only.
    migrate_set = set(migrate_candidates)
    topo_blocking: dict[str, set[str]] = {
        fqn: blocking[fqn] & migrate_set for fqn in migrate_candidates
    }

    # ── Topological sort for migration batches ────────────────────────────
    migrate_batch_lists = _topological_batches(migrate_candidates, topo_blocking)
    scheduled = {fqn for batch in migrate_batch_lists for fqn in batch}
    cycle_members = sorted(fqn for fqn in migrate_candidates if fqn not in scheduled)

    # ── Build output ──────────────────────────────────────────────────────
    node_args = dict(
        obj_type_map=obj_type_map,
        statuses=statuses,
        dbt_status=dbt_status,
        raw_deps=raw_deps,
        blocking=blocking,
        obj_diagnostics=obj_diagnostics,
    )

    migrate_batches = [
        {"batch": i, "objects": [_make_node(fqn, **node_args) for fqn in batch]}
        for i, batch in enumerate(migrate_batch_lists)
    ]

    # Aggregate catalog diagnostics
    all_errors: list[dict[str, Any]] = []
    all_warnings: list[dict[str, Any]] = []
    for fqn in sorted(obj_diagnostics):
        for d in obj_diagnostics[fqn]:
            entry = {"fqn": fqn, "object_type": obj_type_map[fqn], **d}
            if d.get("severity") == "error":
                all_errors.append(entry)
            else:
                all_warnings.append(entry)

    logger.info(
        "event=batch_plan_complete component=batch_plan operation=build_batch_plan "
        "status=success total_objects=%d excluded=%d scope=%d profile=%d "
        "migrate_candidates=%d migrate_batches=%d source_tables=%d source_pending=%d "
        "errors=%d warnings=%d",
        len(inv.all_objects),
        len(inv.excluded_fqns),
        len(scope_phase),
        len(profile_phase),
        len(migrate_candidates),
        len(migrate_batch_lists),
        len(inv.source_table_fqns),
        len(inv.source_pending_fqns),
        len(all_errors),
        len(all_warnings),
    )

    return _build_plan_output(
        inv=inv,
        project_root=project_root,
        scope_phase=[_make_node(fqn, **node_args) for fqn in sorted(scope_phase)],
        profile_phase=[_make_node(fqn, **node_args) for fqn in sorted(profile_phase)],
        migrate_batches=migrate_batches,
        completed_objects=[_make_node(fqn, **node_args) for fqn in sorted(completed_objects)],
        n_a_fqns=n_a_objects,
        obj_type_map=obj_type_map,
        circular_refs=[
            {
                "fqn": fqn,
                "type": obj_type_map[fqn],
                "note": "excluded from scheduling — circular dependency detected",
            }
            for fqn in cycle_members
        ],
        all_errors=all_errors,
        all_warnings=all_warnings,
    )
