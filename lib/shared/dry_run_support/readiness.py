from __future__ import annotations

from pathlib import Path

from shared.dry_run_support.common import VALID_STAGES, detail
from shared.dry_run_support.readiness_context import (
    ObjectReadinessContext as ObjectReadinessContext,
    catalog_error_output as catalog_error_output,
    is_blocking_catalog_error as is_blocking_catalog_error,
    load_object_context as load_object_context,
    not_applicable_output as not_applicable_output,
    object_applicability as object_applicability,
    object_out as object_out,
)
from shared.dry_run_support.readiness_stages import (
    STAGE_OBJECT_CHECKS as STAGE_OBJECT_CHECKS,
    generate_ready as generate_ready,
    profile_ready as profile_ready,
    project_stage_ready as project_stage_ready,
    refactor_ready as refactor_ready,
    scope_ready as scope_ready,
    test_gen_ready as test_gen_ready,
)
from shared.output_models.dry_run import DryRunOutput, ReadinessDetail

_ObjectReadinessContext = ObjectReadinessContext
_catalog_error_output = catalog_error_output
_is_blocking_catalog_error = is_blocking_catalog_error
_load_object_context = load_object_context
_not_applicable_output = not_applicable_output
_object_applicability = object_applicability
_object_out = object_out
_STAGE_OBJECT_CHECKS = STAGE_OBJECT_CHECKS
_generate_ready = generate_ready
_profile_ready = profile_ready
_project_stage_ready = project_stage_ready
_refactor_ready = refactor_ready
_scope_ready = scope_ready
_test_gen_ready = test_gen_ready

__all__ = [
    "_ObjectReadinessContext",
    "_catalog_error_output",
    "_is_blocking_catalog_error",
    "_load_object_context",
    "_not_applicable_output",
    "_object_applicability",
    "_object_out",
    "_STAGE_OBJECT_CHECKS",
    "_generate_ready",
    "_profile_ready",
    "_project_stage_ready",
    "_refactor_ready",
    "_scope_ready",
    "_test_gen_ready",
    "run_ready",
]


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
