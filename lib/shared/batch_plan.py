"""batch_plan.py — Dependency-aware parallel batch scheduler for migration.

Reads all catalog files, builds a transitive dependency graph
(proc → tables, proc → views → underlying tables, proc → procs,
view → views transitively), and computes maximally-parallel execution
batches respecting the migration pipeline.

Every object node carries its current pipeline status and catalog
diagnostics so the LLM can interpret the output without re-reading
catalog files.

Exposed as: migrate-util batch-plan  (subcommand on dry_run.app)

Output contract: output_models.BatchPlanOutput

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
from shared.env_config import resolve_catalog_dir, resolve_dbt_project_path
from shared.loader_data import CatalogLoadError
from shared.output_models.dry_run import (
    BatchPlanOutput,
    BatchSummary,
    CatalogDiagnosticEntry,
    CatalogDiagnostics,
    CircularRef,
    DiagnosticEntry,
    ExcludedObject,
    MigrateBatch,
    NaObject,
    ObjectNode,
    SourcePending,
    SourceTable,
)

from shared.deps import _has_dbt_model, collect_deps
from shared.pipeline_status import (
    _compute_diagnostic_stage_flags,
    _compute_status_and_diagnostics,
)

logger = logging.getLogger(__name__)


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
) -> ObjectNode:
    """Build an output node for a single catalog object."""
    diags = obj_diagnostics.get(fqn, [])
    return ObjectNode(
        fqn=fqn,
        type=obj_type_map[fqn],
        pipeline_status=statuses[fqn],
        has_dbt_model=dbt_status[fqn],
        direct_deps=sorted(raw_deps.get(fqn, set())),
        blocking_deps=sorted(blocking.get(fqn, set())),
        diagnostics=[DiagnosticEntry(**d) for d in diags],
        diagnostic_stage_flags=_compute_diagnostic_stage_flags(diags),
    )


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
    scope_phase: list[ObjectNode] | None = None,
    profile_phase: list[ObjectNode] | None = None,
    migrate_batches: list[MigrateBatch] | None = None,
    completed_objects: list[ObjectNode] | None = None,
    n_a_fqns: list[str] | None = None,
    obj_type_map: dict[str, str] | None = None,
    circular_refs: list[CircularRef] | None = None,
    all_errors: list[CatalogDiagnosticEntry] | None = None,
    all_warnings: list[CatalogDiagnosticEntry] | None = None,
) -> BatchPlanOutput:
    """Build the final batch plan output.

    Single source of truth for the output schema — all return paths use this.
    """
    _obj_type_map = obj_type_map or {}
    return BatchPlanOutput(
        summary=BatchSummary(
            total_objects=len(inv.all_objects),
            tables=len(inv.table_fqns),
            views=sum(1 for _, t in inv.view_entries if t == "view"),
            mvs=sum(1 for _, t in inv.view_entries if t == "mv"),
            writerless_tables=len(n_a_fqns) if n_a_fqns is not None else 0,
            excluded_count=len(inv.excluded_fqns),
            source_tables=len(inv.source_table_fqns),
            source_pending=len(inv.source_pending_fqns),
        ),
        scope_phase=scope_phase or [],
        profile_phase=profile_phase or [],
        migrate_batches=migrate_batches or [],
        completed_objects=completed_objects or [],
        n_a_objects=[
            NaObject(fqn=fqn, type=_obj_type_map.get(fqn, "table"), reason="writerless")
            for fqn in sorted(n_a_fqns or [])
        ],
        excluded_objects=[
            ExcludedObject(
                fqn=fqn,
                type=_resolve_excluded_type(project_root, fqn),
                note="excluded from pipeline",
            )
            for fqn in sorted(inv.excluded_fqns)
        ],
        source_tables=[
            SourceTable(fqn=fqn, type="table", reason="is_source")
            for fqn in sorted(inv.source_table_fqns)
        ],
        source_pending=[
            SourcePending(fqn=fqn, type="table")
            for fqn in sorted(inv.source_pending_fqns)
        ],
        circular_refs=circular_refs or [],
        catalog_diagnostics=CatalogDiagnostics(
            total_errors=len(all_errors or []),
            total_warnings=len(all_warnings or []),
            errors=all_errors or [],
            warnings=all_warnings or [],
        ),
    )


# ── Main entry point ──────────────────────────────────────────────────────────


def build_batch_plan(project_root: Path, dbt_root: Path | None = None) -> BatchPlanOutput:
    """Build a dependency-aware parallel batch plan for all catalog objects.

    Reads all table and view catalog files, computes transitive dependencies,
    and returns a structured plan grouping objects by pipeline phase and,
    within the migration phase, by dependency-ordered parallel batches.
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

    # ── Determine pipeline status, dbt model presence, and diagnostics ────
    # Uses _compute_status_and_diagnostics to load each catalog file once
    # instead of separately via object_pipeline_status + collect_object_diagnostics.
    statuses: dict[str, str] = {}
    dbt_status: dict[str, bool] = {}
    obj_diagnostics: dict[str, list[dict[str, Any]]] = {}
    for fqn, obj_type in inv.all_objects:
        status, diags = _compute_status_and_diagnostics(project_root, fqn, obj_type, dbt_root)
        statuses[fqn] = status
        dbt_status[fqn] = _has_dbt_model(fqn, dbt_root)
        obj_diagnostics[fqn] = diags

    # ── Build transitive dependency graph ─────────────────────────────────
    raw_deps: dict[str, set[str]] = {}
    for fqn, obj_type in inv.all_objects:
        raw_deps[fqn] = collect_deps(project_root, fqn, obj_type) & all_fqns

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
        MigrateBatch(batch=i, objects=[_make_node(fqn, **node_args) for fqn in batch])
        for i, batch in enumerate(migrate_batch_lists)
    ]

    # Aggregate catalog diagnostics
    all_errors: list[CatalogDiagnosticEntry] = []
    all_warnings: list[CatalogDiagnosticEntry] = []
    for fqn in sorted(obj_diagnostics):
        for d in obj_diagnostics[fqn]:
            entry = CatalogDiagnosticEntry(fqn=fqn, object_type=obj_type_map[fqn], **d)
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
            CircularRef(
                fqn=fqn,
                type=obj_type_map[fqn],
                note="excluded from scheduling — circular dependency detected",
            )
            for fqn in cycle_members
        ],
        all_errors=all_errors,
        all_warnings=all_warnings,
    )
