"""Object context and shared object guards for dry-run readiness checks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.catalog import detect_object_type, load_table_catalog, load_view_catalog
from shared.dry_run_support.common import object_detail
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models.dry_run import DryRunOutput, ReadinessDetail

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ObjectReadinessContext:
    fqn: str
    obj_type: str
    catalog: Any | None


def object_out(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
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


def not_applicable_output(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
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


def load_object_context(
    project_root: Path,
    object_fqn: str,
) -> ObjectReadinessContext | ReadinessDetail:
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
    return ObjectReadinessContext(fqn=norm, obj_type=obj_type, catalog=cat)


def object_applicability(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput | None:
    cat = ctx.catalog
    if ctx.obj_type == "table" and cat is not None:
        if cat.is_seed:
            return not_applicable_output(
                stage=stage, project=project, ctx=ctx, code="SEED_TABLE",
            )
        if cat.is_source:
            return not_applicable_output(
                stage=stage, project=project, ctx=ctx, code="SOURCE_TABLE",
            )
        if cat.excluded:
            return not_applicable_output(
                stage=stage, project=project, ctx=ctx, code="EXCLUDED",
            )
    if ctx.obj_type in ("view", "mv") and cat is not None and cat.excluded:
        return not_applicable_output(
            stage=stage, project=project, ctx=ctx, code="EXCLUDED",
        )
    return None


def catalog_error_output(
    *,
    stage: str,
    project: ReadinessDetail,
    ctx: ObjectReadinessContext,
) -> DryRunOutput | None:
    cat = ctx.catalog
    errors = getattr(cat, "errors", None) if cat is not None else None
    if not isinstance(errors, list):
        return None
    if not any(is_blocking_catalog_error(entry) for entry in errors):
        return None
    logger.info(
        "event=readiness_blocked_catalog_errors stage=%s fqn=%s object_type=%s",
        stage, ctx.fqn, ctx.obj_type,
    )
    return object_out(
        stage=stage,
        project=project,
        ctx=ctx,
        ready=False,
        reason="catalog_errors_unresolved",
        code="CATALOG_ERRORS_UNRESOLVED",
    )


def is_blocking_catalog_error(entry: object) -> bool:
    if isinstance(entry, dict):
        return entry.get("severity", "error") == "error"
    return getattr(entry, "severity", "error") == "error"
