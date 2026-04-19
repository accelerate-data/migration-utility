"""Dependency-aware parallel batch scheduler for migration.

Reads all catalog files, builds a transitive dependency graph, and computes
maximally-parallel execution batches respecting the migration pipeline.

Exposed as: migrate-util batch-plan

Output contract: output_models.BatchPlanOutput
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.batch_plan_support.dashboard import _build_status_dashboard
from shared.batch_plan_support.diagnostics import _collect_catalog_diagnostics
from shared.batch_plan_support.inventory import _CatalogInventory, _enumerate_catalog
from shared.batch_plan_support.nodes import _build_plan_output, _make_node
from shared.batch_plan_support.scheduling import (
    _classify_phases,
    _compute_blocking_deps,
    _topological_batches,
)
from shared.deps import _has_dbt_model, collect_deps
from shared.env_config import resolve_dbt_project_path
from shared.output_models.dry_run import (
    BatchPlanOutput,
    CatalogDiagnosticEntry,
    CircularRef,
    MigrateBatch,
)
from shared.pipeline_status import _compute_status_and_diagnostics

logger = logging.getLogger(__name__)


@dataclass
class _BatchPlanInputs:
    """Collected state needed to assemble a batch plan."""

    inv: _CatalogInventory
    obj_type_map: dict[str, str]
    statuses: dict[str, str]
    dbt_status: dict[str, bool]
    obj_diagnostics: dict[str, list[dict[str, Any]]]
    raw_deps: dict[str, set[str]]
    scope_phase: list[str]
    profile_phase: list[str]
    migrate_candidates: list[str]
    completed_objects: list[str]
    n_a_objects: list[str]
    blocking: dict[str, set[str]]


def _collect_plan_inputs(project_root: Path, dbt_root: Path) -> _BatchPlanInputs:
    """Collect catalog, status, dbt, diagnostic, and dependency state."""
    inv = _enumerate_catalog(project_root)
    obj_type_map = inv.obj_type_map
    all_fqns = set(obj_type_map)

    statuses: dict[str, str] = {}
    dbt_status: dict[str, bool] = {}
    obj_diagnostics: dict[str, list[dict[str, Any]]] = {}
    for fqn, obj_type in inv.all_objects:
        status, diags = _compute_status_and_diagnostics(project_root, fqn, obj_type, dbt_root)
        statuses[fqn] = status
        dbt_status[fqn] = _has_dbt_model(fqn, dbt_root)
        obj_diagnostics[fqn] = diags

    raw_deps: dict[str, set[str]] = {}
    for fqn, obj_type in inv.all_objects:
        raw_deps[fqn] = collect_deps(project_root, fqn, obj_type) & all_fqns

    scope_phase, profile_phase, migrate_candidates, completed_objects, n_a_objects = (
        _classify_phases(inv.all_objects, statuses)
    )

    writerless_fqns = {fqn for fqn, _ in inv.all_objects if statuses[fqn] == "n_a"}
    blocking = _compute_blocking_deps(
        migrate_candidates,
        raw_deps,
        dbt_status,
        writerless_fqns,
    )

    return _BatchPlanInputs(
        inv=inv,
        obj_type_map=obj_type_map,
        statuses=statuses,
        dbt_status=dbt_status,
        obj_diagnostics=obj_diagnostics,
        raw_deps=raw_deps,
        scope_phase=scope_phase,
        profile_phase=profile_phase,
        migrate_candidates=migrate_candidates,
        completed_objects=completed_objects,
        n_a_objects=n_a_objects,
        blocking=blocking,
    )


def _node_args(inputs: _BatchPlanInputs) -> dict[str, Any]:
    return {
        "obj_type_map": inputs.obj_type_map,
        "statuses": inputs.statuses,
        "dbt_status": inputs.dbt_status,
        "raw_deps": inputs.raw_deps,
        "blocking": inputs.blocking,
        "obj_diagnostics": inputs.obj_diagnostics,
    }


def _build_migration_batches(
    inputs: _BatchPlanInputs,
) -> tuple[list[MigrateBatch], list[CircularRef]]:
    """Build dependency-ordered migration batches and circular-ref diagnostics."""
    migrate_set = set(inputs.migrate_candidates)
    topo_blocking = {
        fqn: inputs.blocking[fqn] & migrate_set
        for fqn in inputs.migrate_candidates
    }

    migrate_batch_lists = _topological_batches(inputs.migrate_candidates, topo_blocking)
    scheduled = {fqn for batch in migrate_batch_lists for fqn in batch}
    cycle_members = sorted(
        fqn for fqn in inputs.migrate_candidates
        if fqn not in scheduled
    )
    node_args = _node_args(inputs)

    migrate_batches = [
        MigrateBatch(batch=i, objects=[_make_node(fqn, **node_args) for fqn in batch])
        for i, batch in enumerate(migrate_batch_lists)
    ]
    circular_refs = [
        CircularRef(
            fqn=fqn,
            type=inputs.obj_type_map[fqn],
            note="excluded from scheduling — circular dependency detected",
        )
        for fqn in cycle_members
    ]
    return migrate_batches, circular_refs


def _log_empty_plan(inputs: _BatchPlanInputs) -> None:
    inv = inputs.inv
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


def _log_successful_plan(
    inputs: _BatchPlanInputs,
    *,
    migrate_batches: list[MigrateBatch],
    all_errors: list[CatalogDiagnosticEntry],
    all_warnings: list[CatalogDiagnosticEntry],
) -> None:
    inv = inputs.inv
    logger.info(
        "event=batch_plan_complete component=batch_plan operation=build_batch_plan "
        "status=success total_objects=%d excluded=%d scope=%d profile=%d "
        "migrate_candidates=%d migrate_batches=%d source_tables=%d source_pending=%d seed_tables=%d "
        "errors=%d warnings=%d",
        len(inv.all_objects),
        len(inv.excluded_fqns),
        len(inputs.scope_phase),
        len(inputs.profile_phase),
        len(inputs.migrate_candidates),
        len(migrate_batches),
        len(inv.source_table_fqns),
        len(inv.source_pending_fqns),
        len(inv.seed_table_fqns),
        len(all_errors),
        len(all_warnings),
    )


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

    inputs = _collect_plan_inputs(project_root, dbt_root)
    if not inputs.inv.all_objects:
        _log_empty_plan(inputs)
        return _build_plan_output(inv=inputs.inv, project_root=project_root)

    node_args = _node_args(inputs)
    migrate_batches, circular_refs = _build_migration_batches(inputs)
    all_errors, all_warnings, resolved_warning_counts, reviewed_warnings_hidden = (
        _collect_catalog_diagnostics(project_root, inputs)
    )
    _log_successful_plan(
        inputs,
        migrate_batches=migrate_batches,
        all_errors=all_errors,
        all_warnings=all_warnings,
    )

    scope_nodes = [_make_node(fqn, **node_args) for fqn in sorted(inputs.scope_phase)]
    profile_nodes = [_make_node(fqn, **node_args) for fqn in sorted(inputs.profile_phase)]
    completed_nodes = [_make_node(fqn, **node_args) for fqn in sorted(inputs.completed_objects)]
    n_a_nodes = [_make_node(fqn, **node_args) for fqn in sorted(inputs.n_a_objects)]
    status_summary = _build_status_dashboard(
        project_root,
        inputs,
        scope_nodes=scope_nodes,
        profile_nodes=profile_nodes,
        migrate_batches=migrate_batches,
        completed_nodes=completed_nodes,
        n_a_nodes=n_a_nodes,
        all_errors=all_errors,
        all_warnings=all_warnings,
        resolved_warning_counts=resolved_warning_counts,
    )

    return _build_plan_output(
        inv=inputs.inv,
        project_root=project_root,
        scope_phase=scope_nodes,
        profile_phase=profile_nodes,
        migrate_batches=migrate_batches,
        completed_objects=completed_nodes,
        n_a_fqns=inputs.n_a_objects,
        obj_type_map=inputs.obj_type_map,
        circular_refs=circular_refs,
        all_errors=all_errors,
        all_warnings=all_warnings,
        reviewed_warnings_hidden=reviewed_warnings_hidden,
        status_summary=status_summary,
    )
