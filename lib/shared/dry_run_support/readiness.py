from __future__ import annotations

import json
from pathlib import Path

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


def run_ready(project_root: Path, stage: str, object_fqn: str | None = None) -> DryRunOutput:
    """Check stage readiness, with an optional object-level overlay."""
    if stage not in VALID_STAGES:
        return DryRunOutput(stage=stage, ready=False, project=detail(False, "invalid_stage"))

    project = _project_stage_ready(project_root, stage)
    if not project.ready:
        return DryRunOutput(stage=stage, ready=False, project=project)

    if object_fqn is None:
        return DryRunOutput(stage=stage, ready=True, project=project)

    norm = normalize(object_fqn)
    obj_type = detect_object_type(project_root, norm)
    if obj_type is None:
        object_detail_entry = object_detail(
            norm,
            None,
            False,
            "object_not_found",
            "OBJECT_NOT_FOUND",
        )
        return DryRunOutput(
            stage=stage,
            ready=False,
            project=project,
            object=object_detail_entry,
        )

    if obj_type == "table":
        try:
            cat = load_table_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None:
            if cat.is_seed:
                object_detail_entry = object_detail(
                    norm,
                    obj_type,
                    False,
                    "not_applicable",
                    "SEED_TABLE",
                    not_applicable=True,
                )
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail_entry,
                )
            if cat.is_source:
                object_detail_entry = object_detail(
                    norm,
                    obj_type,
                    False,
                    "not_applicable",
                    "SOURCE_TABLE",
                    not_applicable=True,
                )
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail_entry,
                )
            if cat.excluded:
                object_detail_entry = object_detail(norm, obj_type, False, "not_applicable", "EXCLUDED", not_applicable=True)
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail_entry,
                )
    else:
        try:
            cat = load_view_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None and cat.excluded:
            object_detail_entry = object_detail(norm, obj_type, False, "not_applicable", "EXCLUDED", not_applicable=True)
            return DryRunOutput(
                stage=stage,
                ready=False,
                project=project,
                object=object_detail_entry,
            )

    def object_out(ready: bool, reason: str, code: str | None = None) -> DryRunOutput:
        object_detail_entry = object_detail(norm, obj_type, ready, reason, code)
        return DryRunOutput(
            stage=stage,
            ready=ready and project.ready,
            project=project,
            object=object_detail_entry,
        )

    if stage == "scope":
        if detect_catalog_bucket(project_root, norm) is None:
            return object_out(False, "catalog_missing")
        return object_out(True, "ok")

    if stage == "profile":
        if obj_type in ("view", "mv"):
            if cat is None:
                return object_out(False, "catalog_missing")
            scoping_status = cat.scoping.status if cat.scoping else None
            if scoping_status != "analyzed":
                return object_out(False, "scoping_not_analyzed")
            return object_out(True, "ok")
        if cat is None:
            return object_out(False, "catalog_missing")
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status == "no_writer_found":
            object_detail_entry = object_detail(norm, obj_type, False, "not_applicable", "WRITERLESS_TABLE", not_applicable=True)
            return DryRunOutput(
                stage=stage,
                ready=False,
                project=project,
                object=object_detail_entry,
            )
        if scoping_status != "resolved":
            return object_out(False, "scoping_not_resolved")
        return object_out(True, "ok")

    if stage == "test-gen":
        if cat is None:
            return object_out(False, "catalog_missing")
        profile_status = cat.profile.status if cat.profile else None
        if profile_status not in ("ok", "partial"):
            return object_out(False, "profile_not_complete")
        return object_out(True, "ok")

    if stage == "refactor":
        if cat is None:
            return object_out(False, "catalog_missing")
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return object_out(False, "test_gen_not_complete")
        return object_out(True, "ok")

    if stage == "generate":
        if cat is None:
            return object_out(False, "catalog_missing")
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return object_out(False, "test_gen_not_complete", "TEST_SPEC_MISSING")
        if obj_type in ("view", "mv"):
            refactor_status = cat.refactor.status if cat.refactor else None
            if refactor_status != "ok":
                return object_out(False, "refactor_not_complete")
            return object_out(True, "ok")
        writer = cat.scoping.selected_writer if cat.scoping else None
        if not writer:
            return object_out(False, "no_writer")
        writer_norm = normalize(writer)
        try:
            proc_cat = load_proc_catalog(project_root, writer_norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            proc_cat = None
        refactor_status = (
            proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        )
        if refactor_status != "ok":
            return object_out(False, "refactor_not_complete")
        return object_out(True, "ok")

    return object_out(False, "invalid_stage")
