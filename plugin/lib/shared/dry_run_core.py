"""Core migrate-util logic extracted from the Typer CLI facade."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import (
    detect_catalog_bucket,
    detect_object_type,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    write_json,
)
from shared.batch_plan import build_batch_plan
from shared.deps import collect_deps
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models import (
    DryRunOutput,
    ExcludeOutput,
    ObjectStatus,
    StageStatuses,
    StatusOutput,
    StatusSummary,
    SyncExcludedWarningsOutput,
)

logger = logging.getLogger(__name__)

VALID_STAGES = frozenset(
    {"scope", "profile", "test-gen", "refactor", "migrate", "generate"}
)


def run_ready(project_root: Path, fqn: str, stage: str) -> DryRunOutput:
    """Check whether the prior stage's CLI-written status allows proceeding."""
    norm = normalize(fqn)
    obj_type = detect_object_type(project_root, norm)

    def out(ready: bool, reason: str, code: str | None = None) -> DryRunOutput:
        return DryRunOutput(ready=ready, reason=reason, code=code)

    if stage not in VALID_STAGES:
        return out(False, "invalid_stage")

    if obj_type == "table":
        try:
            cat = load_table_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None:
            if cat.is_source:
                return out(False, "not_applicable", "SOURCE_TABLE")
            if cat.excluded:
                return out(False, "not_applicable", "EXCLUDED")
    else:
        try:
            cat = load_view_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None and cat.excluded:
            return out(False, "not_applicable", "EXCLUDED")

    if stage == "scope":
        if not (project_root / "manifest.json").exists():
            return out(False, "manifest_missing")
        if detect_catalog_bucket(project_root, norm) is None:
            return out(False, "catalog_missing")
        return out(True, "ok")

    if stage == "profile":
        if obj_type in ("view", "mv"):
            if cat is None:
                return out(False, "catalog_missing")
            scoping_status = cat.scoping.status if cat.scoping else None
            if scoping_status != "analyzed":
                return out(False, "scoping_not_analyzed")
            return out(True, "ok")
        if cat is None:
            return out(False, "catalog_missing")
        scoping_status = cat.scoping.status if cat.scoping else None
        if scoping_status == "no_writer_found":
            return out(False, "not_applicable", "WRITERLESS_TABLE")
        if scoping_status != "resolved":
            return out(False, "scoping_not_resolved")
        return out(True, "ok")

    if stage == "test-gen":
        if cat is None:
            return out(False, "catalog_missing")
        profile_status = cat.profile.status if cat.profile else None
        if profile_status not in ("ok", "partial"):
            return out(False, "profile_not_complete")
        return out(True, "ok")

    if stage == "refactor":
        if cat is None:
            return out(False, "catalog_missing")
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return out(False, "test_gen_not_complete")
        return out(True, "ok")

    if stage in ("migrate", "generate"):
        if cat is None:
            return out(False, "catalog_missing")
        test_gen_status = cat.test_gen.status if cat.test_gen else None
        if test_gen_status != "ok":
            return out(False, "test_gen_not_complete", "TEST_SPEC_MISSING")
        if obj_type in ("view", "mv"):
            refactor_status = cat.refactor.status if cat.refactor else None
            if refactor_status != "ok":
                return out(False, "refactor_not_complete")
            return out(True, "ok")
        writer = cat.scoping.selected_writer if cat.scoping else None
        if not writer:
            return out(False, "no_writer")
        writer_norm = normalize(writer)
        try:
            proc_cat = load_proc_catalog(project_root, writer_norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            proc_cat = None
        refactor_status = (
            proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
        )
        if refactor_status != "ok":
            return out(False, "refactor_not_complete")
        return out(True, "ok")

    return out(False, "invalid_stage")


def _single_object_status(project_root: Path, norm_fqn: str) -> ObjectStatus:
    """Collate all stage statuses for a single object."""
    obj_type = detect_object_type(project_root, norm_fqn)
    scope = profile = test_gen = refactor = generate = None

    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, norm_fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        scope = cat.scoping.status if cat and cat.scoping else None
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor = cat.refactor.status if cat and cat.refactor else None
        generate = cat.generate.status if cat and cat.generate else None
    else:
        try:
            cat = load_table_catalog(project_root, norm_fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        scope = cat.scoping.status if cat and cat.scoping else None
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor_status: str | None = cat.refactor.status if cat and cat.refactor else None
        if refactor_status is None:
            writer = cat.scoping.selected_writer if cat and cat.scoping else None
            if writer:
                try:
                    proc_cat = load_proc_catalog(project_root, normalize(writer))
                except (json.JSONDecodeError, OSError, CatalogLoadError):
                    proc_cat = None
                refactor_status = (
                    proc_cat.refactor.status if proc_cat and proc_cat.refactor else None
                )
        refactor = refactor_status
        generate = cat.generate.status if cat and cat.generate else None

    return ObjectStatus(
        fqn=norm_fqn,
        type=obj_type,
        stages=StageStatuses(
            scope=scope,
            profile=profile,
            test_gen=test_gen,
            refactor=refactor,
            generate=generate,
        ),
    )


def run_status(project_root: Path, fqn: str | None = None) -> StatusOutput | ObjectStatus:
    """Collate CLI-written statuses from catalog files."""
    if fqn is not None:
        return _single_object_status(project_root, normalize(fqn))

    catalog_dir = project_root / "catalog"
    objects: list[ObjectStatus] = []
    stage_counts: dict[str, dict[str, int]] = {
        stage: {} for stage in ("scope", "profile", "test_gen", "refactor", "generate")
    }

    for bucket in ("tables", "views"):
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for path in sorted(bucket_dir.glob("*.json")):
            norm_fqn = path.stem
            if bucket == "tables":
                try:
                    cat_data = json.loads(path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    cat_data = {}
                if cat_data.get("is_source"):
                    continue

            obj_status = _single_object_status(project_root, norm_fqn)
            objects.append(obj_status)
            for stage_name, status_val in obj_status.stages.model_dump().items():
                label = status_val if status_val else "pending"
                stage_counts[stage_name][label] = (
                    stage_counts[stage_name].get(label, 0) + 1
                )

    return StatusOutput(
        objects=objects,
        summary=StatusSummary(total=len(objects), by_stage=stage_counts),
    )


def run_exclude(project_root: Path, fqns: list[str]) -> ExcludeOutput:
    """Set ``excluded: true`` on each named table or view catalog file."""
    marked: list[str] = []
    not_found: list[str] = []

    for raw_fqn in fqns:
        norm = normalize(raw_fqn)
        bucket = detect_catalog_bucket(project_root, norm)
        if bucket is None:
            logger.warning(
                "event=exclude_not_found component=exclude operation=run_exclude fqn=%s",
                norm,
            )
            not_found.append(norm)
            continue

        catalog_path = project_root / "catalog" / bucket / f"{norm}.json"
        try:
            data = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.error(
                "event=exclude_read_error component=exclude operation=run_exclude "
                "fqn=%s error=%s",
                norm,
                exc,
            )
            not_found.append(norm)
            continue

        data["excluded"] = True
        write_json(catalog_path, data)
        marked.append(norm)
        logger.info(
            "event=exclude_marked component=exclude operation=run_exclude "
            "fqn=%s bucket=%s status=success",
            norm,
            bucket,
        )

    return ExcludeOutput(marked=marked, not_found=not_found)


def run_sync_excluded_warnings(project_root: Path) -> SyncExcludedWarningsOutput:
    """Write or clear EXCLUDED_DEP warnings on active catalog objects."""
    catalog_dir = project_root / "catalog"
    excluded_fqns: set[str] = set()
    all_entries: list[tuple[str, str]] = []

    for bucket in ("tables", "views"):
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for path in sorted(bucket_dir.glob("*.json")):
            fqn = path.stem
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            all_entries.append((fqn, bucket))
            if data.get("excluded"):
                excluded_fqns.add(fqn)

    warnings_written = 0
    warnings_cleared = 0

    if not excluded_fqns:
        for fqn, bucket in all_entries:
            catalog_path = catalog_dir / bucket / f"{fqn}.json"
            try:
                data = json.loads(catalog_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            existing_warnings: list[dict[str, Any]] = data.get("warnings") or []
            cleaned = [
                warning
                for warning in existing_warnings
                if warning.get("code") != "EXCLUDED_DEP"
            ]
            if len(cleaned) != len(existing_warnings):
                data["warnings"] = cleaned
                write_json(catalog_path, data)
                warnings_cleared += len(existing_warnings) - len(cleaned)
        return SyncExcludedWarningsOutput(
            warnings_written=warnings_written, warnings_cleared=warnings_cleared
        )

    active_entries = [
        (fqn, bucket) for fqn, bucket in all_entries if fqn not in excluded_fqns
    ]

    for fqn, bucket in active_entries:
        obj_type = "table" if bucket == "tables" else "view"
        try:
            full_deps = collect_deps(project_root, fqn, obj_type)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            full_deps = set()

        excluded_deps = sorted(full_deps & excluded_fqns)
        catalog_path = catalog_dir / bucket / f"{fqn}.json"
        try:
            data = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue

        existing_warnings: list[dict[str, Any]] = data.get("warnings") or []
        non_excluded_warnings = [
            warning
            for warning in existing_warnings
            if warning.get("code") != "EXCLUDED_DEP"
        ]
        old_excluded_warning_count = len(existing_warnings) - len(non_excluded_warnings)

        if excluded_deps:
            dep_list = ", ".join(excluded_deps)
            new_warning: dict[str, Any] = {
                "code": "EXCLUDED_DEP",
                "message": (
                    f"Depends on excluded object(s): {dep_list}. "
                    "Consider adding as a dbt source instead."
                ),
                "severity": "warning",
            }
            data["warnings"] = non_excluded_warnings + [new_warning]
            write_json(catalog_path, data)
            warnings_written += 1
            if old_excluded_warning_count > 0:
                warnings_cleared += old_excluded_warning_count
            logger.info(
                "event=excluded_dep_warning_written component=sync_excluded_warnings "
                "fqn=%s excluded_deps=%s",
                fqn,
                dep_list,
            )
        elif old_excluded_warning_count > 0:
            data["warnings"] = non_excluded_warnings
            write_json(catalog_path, data)
            warnings_cleared += old_excluded_warning_count
            logger.info(
                "event=excluded_dep_warning_cleared component=sync_excluded_warnings fqn=%s",
                fqn,
            )

    return SyncExcludedWarningsOutput(
        warnings_written=warnings_written, warnings_cleared=warnings_cleared
    )
