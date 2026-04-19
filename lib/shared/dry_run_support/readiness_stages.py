"""Per-stage dry-run readiness policies."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

from shared.catalog import detect_catalog_bucket, load_proc_catalog
from shared.dry_run_support.common import detail, load_manifest_json, runtime_role_is_configured
from shared.dry_run_support.readiness_context import (
    ObjectReadinessContext,
    not_applicable_output,
    object_out,
)
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models.dry_run import DryRunOutput, ReadinessDetail


def project_stage_ready(project_root: Path, stage: str) -> ReadinessDetail:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return detail(False, "manifest_missing")

    if stage == "setup-ddl":
        return detail(True, "ok")

    if stage == "scope":
        return detail(True, "ok")

    if stage == "profile":
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


def scope_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput:
    if detect_catalog_bucket(project_root, ctx.fqn) is None:
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def profile_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput:
    del project_root
    cat = ctx.catalog
    if ctx.obj_type in ("view", "mv"):
        if cat is None:
            return object_out(
                stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
            )
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status != "analyzed":
            return object_out(
                stage=stage,
                project=project,
                ctx=ctx,
                ready=False,
                reason="scoping_not_analyzed",
            )
        return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")
    if cat is None:
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    scoping_status = cat.scoping.status if cat.scoping else None
    if scoping_status == "no_writer_found":
        return not_applicable_output(
            stage=stage, project=project, ctx=ctx, code="WRITERLESS_TABLE",
        )
    if scoping_status != "resolved":
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="scoping_not_resolved",
        )
    return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def test_gen_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput:
    del project_root
    cat = ctx.catalog
    if cat is None:
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    profile_status = cat.profile.status if cat.profile else None
    if profile_status not in ("ok", "partial"):
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="profile_not_complete",
        )
    return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def refactor_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput:
    del project_root
    cat = ctx.catalog
    if cat is None:
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="test_gen_not_complete",
        )
    return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


def generate_ready(
    project_root: Path,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput:
    cat = ctx.catalog
    if cat is None:
        return object_out(
            stage=stage, project=project, ctx=ctx, ready=False, reason="catalog_missing",
        )
    test_gen_status = cat.test_gen.status if cat.test_gen else None
    if test_gen_status != "ok":
        return object_out(
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
            return object_out(
                stage=stage,
                project=project,
                ctx=ctx,
                ready=False,
                reason="refactor_not_complete",
            )
        return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")
    writer = cat.scoping.selected_writer if cat.scoping else None
    if not writer:
        return object_out(
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
        return object_out(
            stage=stage,
            project=project,
            ctx=ctx,
            ready=False,
            reason="refactor_not_complete",
        )
    return object_out(stage=stage, project=project, ctx=ctx, ready=True, reason="ok")


StageObjectCheck = Callable[[Path, str, ReadinessDetail, ObjectReadinessContext], DryRunOutput]

STAGE_OBJECT_CHECKS: dict[str, StageObjectCheck] = {
    "scope": scope_ready,
    "profile": profile_ready,
    "test-gen": test_gen_ready,
    "refactor": refactor_ready,
    "generate": generate_ready,
}
