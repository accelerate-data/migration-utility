"""Status dashboard helpers for batch-plan output."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from shared.output_models.dry_run import (
    CatalogDiagnosticEntry,
    MigrateBatch,
    ObjectNode,
    StatusDiagnosticRow,
    StatusNextAction,
    StatusPipelineRow,
    StatusSummaryDashboard,
)
from shared.runtime_config import get_runtime_role


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


def _build_status_dashboard(
    project_root: Path,
    inputs: Any,
    *,
    scope_nodes: list[ObjectNode],
    profile_nodes: list[ObjectNode],
    migrate_batches: list[MigrateBatch],
    completed_nodes: list[ObjectNode],
    n_a_nodes: list[ObjectNode],
    all_errors: list[CatalogDiagnosticEntry],
    all_warnings: list[CatalogDiagnosticEntry],
    resolved_warning_counts: dict[str, int],
) -> StatusSummaryDashboard:
    """Build the compact status dashboard embedded in batch-plan output."""
    active_nodes = [
        *scope_nodes,
        *profile_nodes,
        *[node for batch in migrate_batches for node in batch.objects],
        *completed_nodes,
        *n_a_nodes,
    ]
    test_gen_setup_block = _test_gen_setup_block(project_root)
    return StatusSummaryDashboard(
        pipeline_rows=_build_pipeline_rows(
            active_nodes,
            test_gen_setup_block=test_gen_setup_block,
        ),
        diagnostic_rows=_build_diagnostic_rows(
            obj_type_map=inputs.obj_type_map,
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
