from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from shared.catalog import detect_catalog_bucket, detect_object_type, load_proc_catalog, load_table_catalog, load_view_catalog
from shared.dry_run_support.common import (
    VALID_STAGES,
    detail,
    load_manifest_json,
    object_detail,
    runtime_role_is_configured,
)
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models.dry_run import DryRunOutput, ReadinessDetail

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ObjectReadinessContext:
    fqn: str
    obj_type: str
    catalog: Any | None


def _project_stage_ready(project_root: Path, stage: str) -> ReadinessDetail:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return detail(False, "manifest_missing")

    if stage == "setup-ddl":
        return detail(True, "ok")

    if stage == "scope":
        return detail(True, "ok")

    if stage == "profile":
        # Profiling uses catalog + DDL artifacts only; runtime manifest content
        # is not required beyond the project already being initialized.
        return detail(True, "ok")

    manifest = load_manifest_json(manifest_path)
    if manifest is None:
        return detail(False, "manifest_missing")

    if stage == "test-gen":
        if not runtime_role_is_configured(manifest, "target"):
            return detail(False, "target_not_configured", "TARGET_NOT_CONFIGURED")
        if not runtime_role_is_configured(manifest, "sandbox"):
            return detail(False, "sandbox_not_configured", "SANDBOX_NOT_CONFIGURED")
        return detail(True, "ok")

    if stage == "refactor":
        if not runtime_role_is_configured(manifest, "sandbox"):
            return detail(False, "sandbox_not_configured", "SANDBOX_NOT_CONFIGURED")
        return detail(True, "ok")

    if stage == "generate":
        if not runtime_role_is_configured(manifest, "target"):
            return detail(False, "target_not_configured", "TARGET_NOT_CONFIGURED")
        if not runtime_role_is_configured(manifest, "sandbox"):
            return detail(False, "sandbox_not_configured", "SANDBOX_NOT_CONFIGURED")
        if not (project_root / "dbt" / "dbt_project.yml").exists():
            return detail(False, "dbt_project_missing", "DBT_PROJECT_MISSING")
        if not (project_root / "dbt" / "profiles.yml").exists():
            return detail(False, "dbt_profile_missing", "DBT_PROFILE_MISSING")
        return detail(True, "ok")

    return detail(False, "invalid_stage")


def _load_object_context(
    project_root: Path,
    object_fqn: str,
) -> _ObjectReadinessContext | ReadinessDetail:
    norm = normalize(object_fqn)
    obj_type = detect_object_type(project_root, norm)
    if obj_type is None:
        return object_detail(
            norm,
            None,
            False,
            "object_not_found",
            "OBJECT_NOT_FOUND",
        )

    if obj_type == "table":
        try:
            cat = load_table_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
    else:
        try:
            cat = load_view_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
    return _ObjectReadinessContext(fqn=norm, obj_type=obj_type, catalog=cat)


def _object_out(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
    ready: bool,
    reason: str,
    code: str | None = None,
) -> DryRunOutput:
    object_detail_entry = object_detail(ctx.fqn, ctx.obj_type, ready, reason, code)
    return DryRunOutput(
        stage=stage,
        ready=ready and project.ready,
        project=project,
        object=object_detail_entry,
    )


def _not_applicable_output(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
    code: str,
) -> DryRunOutput:
    object_detail_entry = object_detail(
        ctx.fqn,
        ctx.obj_type,
        False,
        "not_applicable",
        code,
        not_applicable=True,
    )
    return DryRunOutput(
        stage=stage,
        ready=False,
        project=project,
        object=object_detail_entry,
    )


def _object_applicability(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput | None:
    cat = ctx.catalog
    if ctx.obj_type == "table" and cat is not None:
        if cat.is_seed:
            return _not_applicable_output(
                stage=stage, project=project, ctx=ctx, code="SEED_TABLE",
            )
        if cat.is_source:
            return _not_applicable_output(
                stage=stage, project=project, ctx=ctx, code="SOURCE_TABLE",
            )
        if cat.excluded:
            return _not_applicable_output(
                stage=stage, project=project, ctx=ctx, code="EXCLUDED",
            )
    if ctx.obj_type in ("view", "mv") and cat is not None and cat.excluded:
        return _not_applicable_output(
            stage=stage, project=project, ctx=ctx, code="EXCLUDED",
        )
    return None


def _catalog_error_output(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput | None:
    cat = ctx.catalog
    errors = getattr(cat, "errors", None) if cat is not None else None
    if not isinstance(errors, list):
        return None
    if not any(_is_blocking_catalog_error(entry) for entry in errors):
        return None
    logger.info(
        "event=readiness_blocked_catalog_errors stage=%s fqn=%s object_type=%s",
        stage, ctx.fqn, ctx.obj_type,
    )
    return _object_out(
        stage=stage,
        project=project,
        ctx=ctx,
        ready=False,
        reason="catalog_errors_unresolved",
        code="CATALOG_ERRORS_UNRESOLVED",
    )


def _is_blocking_catalog_error(entry: object) -> bool:
    if isinstance(entry, dict):
        return entry.get("severity", "error") == "error"
    return getattr(entry, "severity", "error") == "error"


def _scope_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput:
    if detect_catalog_bucket(project_root, ctx.fqn) is None:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def _profile_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput:
    del project_root
    cat = ctx.catalog
    if ctx.obj_type in ("view", "mv"):
        if cat is None:
            return _object_out(
                stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
            )
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status != "analyzed":
            return _object_out(
                stage=stage,
                project=project,
                ctx=ctx,
                ready=False,
                reason="scoping_not_analyzed",
            )
        return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")
    if cat is None:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    scoping_status = cat.scoping.status if cat.scoping else None
    if scoping_status == "no_writer_found":
        return _not_applicable_output(
            stage=stage, project=project, ctx=ctx, code="WRITERLESS_TABLE",
        )
    if scoping_status != "resolved":
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="scoping_not_resolved",
        )
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def _test_gen_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput:
    del project_root
    cat = ctx.catalog
    if cat is None:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    profile_status = cat.profile.status if cat.profile else None
    if profile_status not in ("ok", "partial"):
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="profile_not_complete",
        )
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def _refactor_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput:
    del project_root
    cat = ctx.catalog
    if cat is None:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="test_gen_not_complete",
        )
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def _generate_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: _ObjectReadinessContext,
) -> DryRunOutput:
    cat = ctx.catalog
    if cat is None:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return _object_out(
            stage=stage,
            project=project,
            ctx=ctx,
            ready=False,
            reason="test_gen_not_complete",
            code="TEST_SPEC_MISSING",
        )
    if ctx.obj_type in ("view", "mv"):
        refactor_status = cat.refactor.status if cat.refactor else None
        if refactor_status != "ok":
            return _object_out(
                stage=stage,
                project=project,
                ctx=ctx,
                ready=False,
                reason="refactor_not_complete",
            )
        return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")
    writer = cat.scoping.selected_writer if cat.scoping else None
    if not writer:
        return _object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="no_writer",
        )
    writer_norm = normalize(writer)
    try:
        proc_cat = load_proc_catalog(project_root, writer_norm)
    except (json.JSONDecodeError, OSError, CatalogLoadError):
        proc_cat = None
    refactor_status = (
        proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
    )
    if refactor_status != "ok":
        return _object_out(
            stage=stage,
            project=project,
            ctx=ctx,
            ready=False,
            reason="refactor_not_complete",
        )
    return _object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


_STAGE_OBJECT_CHECKS: dict[
    str,
    Callable[[Path, str, ReadinessDetail, _ObjectReadinessContext], DryRunOutput],
] = {
    "scope": _scope_ready,
    "profile": _profile_ready,
    "test-gen": _test_gen_ready,
    "refactor": _refactor_ready,
    "generate": _generate_ready,
}


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
