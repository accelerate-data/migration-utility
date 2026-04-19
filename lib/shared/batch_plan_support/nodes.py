"""Output node and final model builders for batch plans."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.catalog import load_view_catalog
from shared.env_config import resolve_catalog_dir
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
    StatusSummaryDashboard,
)
from shared.pipeline_status import _compute_diagnostic_stage_flags

from .inventory import _CatalogInventory


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

    Single source of truth for the output schema; all return paths use this.
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
