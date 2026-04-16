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
    SeedTable,
    SourcePending,
    SourceTable,
    StatusDiagnosticRow,
    StatusNextAction,
    StatusPipelineRow,
    StatusSummaryDashboard,
)

from shared.deps import _has_dbt_model, collect_deps
from shared.diagnostic_reviews import partition_reviewed_warnings
from shared.runtime_config import get_runtime_role
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
    reviewed_warnings_hidden: int = 0,
    status_summary: StatusSummaryDashboard | None = None,
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
            seed_tables=len(inv.seed_table_fqns),
        ),
        status_summary=status_summary or StatusSummaryDashboard(),
        scope_phase=scope_phase or [],
        profile_phase=profile_phase or [],
        migrate_batches=migrate_batches or [],
        completed_objects=completed_objects or [],
        seed_tables=[
            SeedTable(fqn=fqn, type="table", reason="is_seed")
            for fqn in sorted(inv.seed_table_fqns)
        ],
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
            reviewed_warnings_hidden=reviewed_warnings_hidden,
            errors=all_errors or [],
            warnings=all_warnings or [],
        ),
    )


def _runtime_role_is_configured(manifest: dict[str, Any], role: str) -> bool:
    try:
        runtime_role = get_runtime_role(manifest, role)
    except Exception:
        return False
    if runtime_role is None:
        return False
    connection = runtime_role.connection.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    return bool(connection)


def _test_gen_setup_block(project_root: Path) -> str | None:
    manifest_path = project_root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return "TARGET_NOT_CONFIGURED"

    if not _runtime_role_is_configured(manifest, "target"):
        return "TARGET_NOT_CONFIGURED"
    if not _runtime_role_is_configured(manifest, "sandbox"):
        return "SANDBOX_NOT_CONFIGURED"
    return None


def _pipeline_cells(
    *,
    pipeline_status: str,
    diagnostic_stage_flags: dict[str, str],
    test_gen_setup_block: str | None,
) -> dict[str, str]:
    if pipeline_status == "scope_needed":
        cells = {
            "scope": "pending",
            "profile": "blocked",
            "test_gen": "blocked",
            "refactor": "blocked",
            "migrate": "blocked",
        }
    elif pipeline_status == "profile_needed":
        cells = {
            "scope": "ok",
            "profile": "pending",
            "test_gen": "blocked",
            "refactor": "blocked",
            "migrate": "blocked",
        }
    elif pipeline_status == "test_gen_needed":
        cells = {
            "scope": "ok",
            "profile": "ok",
            "test_gen": "setup-blocked" if test_gen_setup_block else "pending",
            "refactor": "blocked",
            "migrate": "blocked",
        }
    elif pipeline_status == "refactor_needed":
        cells = {
            "scope": "ok",
            "profile": "ok",
            "test_gen": "ok",
            "refactor": "pending",
            "migrate": "blocked",
        }
    elif pipeline_status == "migrate_needed":
        cells = {
            "scope": "ok",
            "profile": "ok",
            "test_gen": "ok",
            "refactor": "ok",
            "migrate": "pending",
        }
    elif pipeline_status == "complete":
        cells = {
            "scope": "ok",
            "profile": "ok",
            "test_gen": "ok",
            "refactor": "ok",
            "migrate": "ok",
        }
    else:
        cells = {
            "scope": "N/A",
            "profile": "N/A",
            "test_gen": "N/A",
            "refactor": "N/A",
            "migrate": "N/A",
        }

    for stage, severity in diagnostic_stage_flags.items():
        cell_name = "test_gen" if stage == "test-gen" else stage
        if cell_name not in cells or cells[cell_name] == "N/A":
            continue
        if severity in {"error", "warning"}:
            cells[cell_name] = severity
        if severity == "error":
            stage_order = ["scope", "profile", "test_gen", "refactor", "migrate"]
            for later_stage in stage_order[stage_order.index(cell_name) + 1:]:
                if cells[later_stage] != "N/A":
                    cells[later_stage] = "blocked"
    return cells


def _build_pipeline_rows(
    nodes: list[ObjectNode],
    *,
    test_gen_setup_block: str | None,
) -> list[StatusPipelineRow]:
    rows: list[StatusPipelineRow] = []
    for node in sorted(nodes, key=lambda n: n.fqn):
        cells = _pipeline_cells(
            pipeline_status=node.pipeline_status,
            diagnostic_stage_flags=node.diagnostic_stage_flags,
            test_gen_setup_block=test_gen_setup_block,
        )
        rows.append(
            StatusPipelineRow(
                fqn=node.fqn,
                type=node.type,
                scope=cells["scope"],
                profile=cells["profile"],
                test_gen=cells["test_gen"],
                refactor=cells["refactor"],
                migrate=cells["migrate"],
            )
        )
    return rows


def _build_diagnostic_rows(
    *,
    obj_type_map: dict[str, str],
    all_errors: list[CatalogDiagnosticEntry],
    all_warnings: list[CatalogDiagnosticEntry],
    resolved_warning_counts: dict[str, int],
) -> list[StatusDiagnosticRow]:
    fqns = (
        {entry.fqn for entry in all_errors}
        | {entry.fqn for entry in all_warnings}
        | {fqn for fqn, count in resolved_warning_counts.items() if count > 0}
    )
    rows: list[StatusDiagnosticRow] = []
    for fqn in sorted(fqns):
        rows.append(
            StatusDiagnosticRow(
                fqn=fqn,
                type=obj_type_map.get(fqn, "table"),
                errors_unresolved=sum(1 for entry in all_errors if entry.fqn == fqn),
                warnings_unresolved=sum(1 for entry in all_warnings if entry.fqn == fqn),
                warnings_resolved=resolved_warning_counts.get(fqn, 0),
                details_command=f"/status {fqn}",
            )
        )
    return rows


def _first_migrate_node(migrate_batches: list[MigrateBatch]) -> ObjectNode | None:
    for batch in migrate_batches:
        if batch.objects:
            return batch.objects[0]
    return None


def _build_next_action(
    *,
    all_errors: list[CatalogDiagnosticEntry],
    scope_phase: list[ObjectNode],
    profile_phase: list[ObjectNode],
    migrate_batches: list[MigrateBatch],
    test_gen_setup_block: str | None,
) -> StatusNextAction:
    if all_errors:
        return StatusNextAction(kind="diagnostics", reason="unresolved_errors")
    if scope_phase:
        command = "/scope-tables " + " ".join(node.fqn for node in scope_phase[:10])
        return StatusNextAction(kind="command", command=command, reason="scope_needed")
    if profile_phase:
        command = "/profile-tables " + " ".join(node.fqn for node in profile_phase[:10])
        return StatusNextAction(kind="command", command=command, reason="profile_needed")

    first = _first_migrate_node(migrate_batches)
    if first is None:
        return StatusNextAction(kind="none", reason="no_action")
    if first.pipeline_status == "test_gen_needed":
        if test_gen_setup_block == "TARGET_NOT_CONFIGURED":
            return StatusNextAction(
                kind="command",
                command="!ad-migration setup-target",
                reason="target_not_configured",
            )
        if test_gen_setup_block == "SANDBOX_NOT_CONFIGURED":
            return StatusNextAction(
                kind="command",
                command="!ad-migration setup-sandbox",
                reason="sandbox_not_configured",
            )
        return StatusNextAction(
            kind="command",
            command=f"/generate-tests {first.fqn}",
            reason="test_gen_needed",
        )
    if first.pipeline_status == "refactor_needed":
        return StatusNextAction(
            kind="command",
            command=f"/refactor-query {first.fqn}",
            reason="refactor_needed",
        )
    if first.pipeline_status == "migrate_needed":
        return StatusNextAction(
            kind="command",
            command=f"/generate-model {first.fqn}",
            reason="migrate_needed",
        )
    return StatusNextAction(kind="none", reason="no_action")


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
            "status=%s total_objects=0 source_tables=%d source_pending=%d seed_tables=%d",
            "empty"
            if not inv.excluded_fqns
            and not inv.source_table_fqns
            and not inv.source_pending_fqns
            and not inv.seed_table_fqns
            else "success",
            len(inv.source_table_fqns),
            len(inv.source_pending_fqns),
            len(inv.seed_table_fqns),
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
    resolved_warning_counts: dict[str, int] = {}
    reviewed_warnings_hidden = 0
    for fqn in sorted(obj_diagnostics):
        object_type = obj_type_map[fqn]
        visible_warnings, hidden_count = partition_reviewed_warnings(
            project_root,
            fqn=fqn,
            object_type=object_type,
            warnings=[
                diagnostic
                for diagnostic in obj_diagnostics[fqn]
                if diagnostic.get("severity") != "error"
            ],
        )
        reviewed_warnings_hidden += hidden_count
        resolved_warning_counts[fqn] = hidden_count
        visible_warning_keys = {
            (
                warning.get("code"),
                warning.get("message"),
                warning.get("severity", "warning"),
                warning.get("item_id"),
                warning.get("field"),
            )
            for warning in visible_warnings
        }
        for d in obj_diagnostics[fqn]:
            entry = CatalogDiagnosticEntry(fqn=fqn, object_type=object_type, **d)
            if d.get("severity") == "error":
                all_errors.append(entry)
            elif (
                d.get("code"),
                d.get("message"),
                d.get("severity", "warning"),
                d.get("item_id"),
                d.get("field"),
            ) in visible_warning_keys:
                all_warnings.append(entry)

    logger.info(
        "event=batch_plan_complete component=batch_plan operation=build_batch_plan "
        "status=success total_objects=%d excluded=%d scope=%d profile=%d "
        "migrate_candidates=%d migrate_batches=%d source_tables=%d source_pending=%d seed_tables=%d "
        "errors=%d warnings=%d",
        len(inv.all_objects),
        len(inv.excluded_fqns),
        len(scope_phase),
        len(profile_phase),
        len(migrate_candidates),
        len(migrate_batch_lists),
        len(inv.source_table_fqns),
        len(inv.source_pending_fqns),
        len(inv.seed_table_fqns),
        len(all_errors),
        len(all_warnings),
    )

    scope_nodes = [_make_node(fqn, **node_args) for fqn in sorted(scope_phase)]
    profile_nodes = [_make_node(fqn, **node_args) for fqn in sorted(profile_phase)]
    completed_nodes = [_make_node(fqn, **node_args) for fqn in sorted(completed_objects)]
    n_a_nodes = [_make_node(fqn, **node_args) for fqn in sorted(n_a_objects)]
    active_nodes = [
        *scope_nodes,
        *profile_nodes,
        *[node for batch in migrate_batches for node in batch.objects],
        *completed_nodes,
        *n_a_nodes,
    ]
    test_gen_setup_block = _test_gen_setup_block(project_root)
    status_summary = StatusSummaryDashboard(
        pipeline_rows=_build_pipeline_rows(
            active_nodes,
            test_gen_setup_block=test_gen_setup_block,
        ),
        diagnostic_rows=_build_diagnostic_rows(
            obj_type_map=obj_type_map,
            all_errors=all_errors,
            all_warnings=all_warnings,
            resolved_warning_counts=resolved_warning_counts,
        ),
        next_action=_build_next_action(
            all_errors=all_errors,
            scope_phase=scope_nodes,
            profile_phase=profile_nodes,
            migrate_batches=migrate_batches,
            test_gen_setup_block=test_gen_setup_block,
        ),
    )

    return _build_plan_output(
        inv=inv,
        project_root=project_root,
        scope_phase=scope_nodes,
        profile_phase=profile_nodes,
        migrate_batches=migrate_batches,
        completed_objects=completed_nodes,
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
        reviewed_warnings_hidden=reviewed_warnings_hidden,
        status_summary=status_summary,
    )
