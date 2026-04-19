from __future__ import annotations

from pathlib import Path

from shared.dry_run_support.common import VALID_STAGES, detail
from shared.dry_run_support.readiness_context import (
    ObjectReadinessContext as _ObjectReadinessContext,
    catalog_error_output as _catalog_error_output,
    is_blocking_catalog_error as _is_blocking_catalog_error,
    load_object_context as _load_object_context,
    not_applicable_output as _not_applicable_output,
    object_applicability as _object_applicability,
    object_out as _object_out,
)
from shared.dry_run_support.readiness_stages import (
    STAGE_OBJECT_CHECKS as _STAGE_OBJECT_CHECKS,
    generate_ready as _generate_ready,
    profile_ready as _profile_ready,
    project_stage_ready as _project_stage_ready,
    refactor_ready as _refactor_ready,
    scope_ready as _scope_ready,
    test_gen_ready as _test_gen_ready,
)
from shared.output_models.dry_run import DryRunOutput, ReadinessDetail


def run_ready(project_root: Path, stage: str, object_fqn: str | None = None) -> DryRunOutput:
    """Check stage readiness, with an optional object-level overlay."""
    if stage not in VALID_STAGES:
        return DryRunOutput(stage=stage, ready=False, project=detail(False, "invalid_stage"))

    project = _project_stage_ready(project_root, stage)
    if not project.ready:
        return DryRunOutput(stage=stage, ready=False, project=project)

    if object_fqn is None:
        return DryRunOutput(stage=stage, ready=True, project=project)

    ctx_or_error = _load_object_context(project_root, object_fqn)
    if isinstance(ctx_or_error, ReadinessDetail):
        return DryRunOutput(
            stage=stage,
            ready=False,
            project=project,
            object=ctx_or_error,
        )
    ctx = ctx_or_error

    not_applicable = _object_applicability(stage=stage, project=project, ctx=ctx)
    if not_applicable is not None:
        return not_applicable

    catalog_error = _catalog_error_output(stage=stage, project=project, ctx=ctx)
    if catalog_error is not None:
        return catalog_error

    checker = _STAGE_OBJECT_CHECKS.get(stage)
    if checker is None:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="invalid_stage",
        )
    return checker(project_root, stage, project, ctx)
