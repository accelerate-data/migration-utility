"""Core migrate-util logic extracted from the Typer CLI facade."""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from shared.catalog import (
    detect_catalog_bucket,
    detect_object_type,
    load_proc_catalog,
    load_table_catalog,
    load_view_catalog,
    write_json,
)
from shared.catalog_models import TableCatalog
from shared.deps import collect_deps
from shared.loader_data import CatalogLoadError
from shared.name_resolver import normalize
from shared.output_models.dry_run import (
    DryRunOutput,
    ExcludeOutput,
    ObjectStatus,
    ObjectReadiness,
    ReadinessDetail,
    ResetMigrationOutput,
    ResetTargetResult,
    StageStatuses,
    StatusOutput,
    StatusSummary,
    SyncExcludedWarningsOutput,
)
from shared.runtime_config import get_runtime_role

logger = logging.getLogger(__name__)

VALID_STAGES = frozenset(
    {"setup-ddl", "scope", "profile", "test-gen", "refactor", "generate"}
)
RESETTABLE_STAGES = frozenset({"scope", "profile", "generate-tests", "refactor"})
RESET_GLOBAL_PATHS = ("catalog", "ddl", ".staging", "test-specs", "dbt")
RESET_GLOBAL_MANIFEST_SECTIONS = (
    "runtime.source",
    "runtime.target",
    "runtime.sandbox",
    "extraction",
    "init_handoff",
)

_RESET_STAGE_SECTIONS: dict[str, tuple[str, ...]] = {
    "scope": ("scoping", "profile", "test_gen"),
    "profile": ("profile", "test_gen"),
    "generate-tests": ("test_gen",),
    "refactor": (),
}


def _display_scope_status(scope_status: str | None) -> str | None:
    """Map completed internal scope states to the unified /status display value."""
    if scope_status in {"resolved", "analyzed"}:
        return "ok"
    return scope_status


def _detail(ready: bool, reason: str, code: str | None = None) -> ReadinessDetail:
    return ReadinessDetail(ready=ready, reason=reason, code=code)


def _object_detail(
    object_fqn: str,
    object_type: str | None,
    ready: bool,
    reason: str,
    code: str | None = None,
    *,
    not_applicable: bool | None = None,
) -> ObjectReadiness:
    return ObjectReadiness(
        object=object_fqn,
        object_type=object_type,
        ready=ready,
        reason=reason,
        code=code,
        not_applicable=not_applicable,
    )


def _runtime_role_is_configured(manifest: dict[str, Any], role: str) -> bool:
    runtime_role = get_runtime_role(manifest, role)
    if runtime_role is None:
        return False
    connection = runtime_role.connection.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    return bool(connection)


def _read_catalog_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise CatalogLoadError(str(path), exc) from exc


def _project_stage_ready(project_root: Path, stage: str) -> ReadinessDetail:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return _detail(False, "manifest_missing")

    if stage == "setup-ddl":
        return _detail(True, "ok")

    if stage == "scope":
        return _detail(True, "ok")

    if stage == "profile":
        # Profiling uses catalog + DDL artifacts only; runtime manifest content
        # is not required beyond the project already being initialized.
        return _detail(True, "ok")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "event=manifest_parse_failed path=%s error=%s",
            manifest_path, exc,
        )
        return _detail(False, "manifest_missing")

    if stage in {"test-gen", "refactor"}:
        if not _runtime_role_is_configured(manifest, "sandbox"):
            return _detail(False, "sandbox_not_configured", "SANDBOX_NOT_CONFIGURED")
        return _detail(True, "ok")

    if stage == "generate":
        if not _runtime_role_is_configured(manifest, "target"):
            return _detail(False, "target_not_configured", "TARGET_NOT_CONFIGURED")
        if not _runtime_role_is_configured(manifest, "sandbox"):
            return _detail(False, "sandbox_not_configured", "SANDBOX_NOT_CONFIGURED")
        if not (project_root / "dbt" / "dbt_project.yml").exists():
            return _detail(False, "dbt_project_missing", "DBT_PROJECT_MISSING")
        if not (project_root / "dbt" / "profiles.yml").exists():
            return _detail(False, "dbt_profile_missing", "DBT_PROFILE_MISSING")
        return _detail(True, "ok")

    return _detail(False, "invalid_stage")


def run_ready(project_root: Path, stage: str, object_fqn: str | None = None) -> DryRunOutput:
    """Check stage readiness, with an optional object-level overlay."""
    if stage not in VALID_STAGES:
        return DryRunOutput(stage=stage, ready=False, project=_detail(False, "invalid_stage"))

    project = _project_stage_ready(project_root, stage)
    if not project.ready:
        return DryRunOutput(stage=stage, ready=False, project=project)

    if object_fqn is None:
        return DryRunOutput(stage=stage, ready=True, project=project)

    norm = normalize(object_fqn)
    obj_type = detect_object_type(project_root, norm)
    if obj_type is None:
        object_detail = _object_detail(
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
            object=object_detail,
        )

    if obj_type == "table":
        try:
            cat = load_table_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None:
            if cat.is_source:
                object_detail = _object_detail(norm, obj_type, False, "not_applicable", "SOURCE_TABLE", not_applicable=True)
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail,
                )
            if cat.excluded:
                object_detail = _object_detail(norm, obj_type, False, "not_applicable", "EXCLUDED", not_applicable=True)
                return DryRunOutput(
                    stage=stage,
                    ready=False,
                    project=project,
                    object=object_detail,
                )
    else:
        try:
            cat = load_view_catalog(project_root, norm)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            cat = None
        if cat is not None and cat.excluded:
            object_detail = _object_detail(norm, obj_type, False, "not_applicable", "EXCLUDED", not_applicable=True)
            return DryRunOutput(
                stage=stage,
                ready=False,
                project=project,
                object=object_detail,
            )

    def object_out(ready: bool, reason: str, code: str | None = None) -> DryRunOutput:
        object_detail = _object_detail(norm, obj_type, ready, reason, code)
        return DryRunOutput(
            stage=stage,
            ready=ready and project.ready,
            project=project,
            object=object_detail,
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
            object_detail = _object_detail(norm, obj_type, False, "not_applicable", "WRITERLESS_TABLE", not_applicable=True)
            return DryRunOutput(
                stage=stage,
                ready=False,
                project=project,
                object=object_detail,
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


def _single_object_status(
    project_root: Path,
    norm_fqn: str,
    *,
    obj_type: str | None = None,
    cat_data: dict[str, Any] | None = None,
) -> ObjectStatus | StatusOutput:
    """Collate all stage statuses for a single object."""
    obj_type = obj_type or detect_object_type(project_root, norm_fqn)
    if obj_type is None:
        return StatusOutput(
            fqn=norm_fqn,
            type=None,
            stages=None,
        )
    scope = profile = test_gen = refactor = generate = None

    if obj_type in ("view", "mv"):
        try:
            cat = load_view_catalog(project_root, norm_fqn)
        except (json.JSONDecodeError, OSError, CatalogLoadError):
            logger.warning(
                "event=view_catalog_load_failed fqn=%s", norm_fqn,
            )
            cat = None
        scope = _display_scope_status(cat.scoping.status if cat and cat.scoping else None)
        profile = cat.profile.status if cat and cat.profile else None
        test_gen = cat.test_gen.status if cat and cat.test_gen else None
        refactor = cat.refactor.status if cat and cat.refactor else None
        generate = cat.generate.status if cat and cat.generate else None
    else:
        if cat_data is not None:
            try:
                cat = TableCatalog.model_validate(cat_data)
            except ValidationError:
                cat = None
        else:
            try:
                cat = load_table_catalog(project_root, norm_fqn)
            except (json.JSONDecodeError, OSError, CatalogLoadError):
                cat = None
        scope = _display_scope_status(cat.scoping.status if cat and cat.scoping else None)
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
                    cat_data = _read_catalog_json(path)
                except CatalogLoadError:
                    cat_data = {}
                if cat_data.get("is_source"):
                    continue
                obj_status = _single_object_status(
                    project_root,
                    norm_fqn,
                    obj_type="table",
                    cat_data=cat_data,
                )
            else:
                obj_status = _single_object_status(project_root, norm_fqn)

            if not isinstance(obj_status, ObjectStatus):
                continue

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


def _delete_if_present(path: Path) -> bool:
    if not path.exists():
        return False
    path.unlink()
    return True


def _delete_tree_if_present(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


def _reset_table_sections(
    project_root: Path,
    norm: str,
    stage: str,
) -> tuple[list[str], list[str], str | None]:
    table_path = project_root / "catalog" / "tables" / f"{norm}.json"
    table_data = _read_catalog_json(table_path)
    writer = (
        table_data.get("scoping", {}).get("selected_writer")
        if isinstance(table_data.get("scoping"), dict)
        else None
    )

    cleared_sections: list[str] = []
    deleted_files: list[str] = []

    for key in _RESET_STAGE_SECTIONS[stage]:
        if key in table_data:
            del table_data[key]
            cleared_sections.append(f"table.{key}")

    if stage in ("scope", "profile", "generate-tests"):
        spec_path = project_root / "test-specs" / f"{norm}.json"
        if _delete_if_present(spec_path):
            deleted_files.append(f"test-specs/{norm}.json")

    write_json(table_path, table_data)
    return cleared_sections, deleted_files, writer


def _reset_writer_refactor(
    project_root: Path,
    writer_fqn: str | None,
) -> list[str]:
    if not writer_fqn:
        return []

    writer_norm = normalize(writer_fqn)
    proc_path = project_root / "catalog" / "procedures" / f"{writer_norm}.json"
    if not proc_path.exists():
        return []

    proc_data = _read_catalog_json(proc_path)
    if "refactor" not in proc_data:
        return []

    del proc_data["refactor"]
    write_json(proc_path, proc_data)
    return [f"procedure:{writer_norm}.refactor"]


def _prepare_reset_migration_all_manifest(project_root: Path) -> tuple[dict[str, Any] | None, list[str]]:
    """Load manifest cleanup up front; sandbox teardown stays in the command layer."""
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        logger.warning(
            "event=reset_migration_global_manifest_missing component=reset_migration "
            "operation=run_reset_migration path=%s",
            manifest_path,
        )
        return None, []

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    cleared_sections: list[str] = []

    runtime = manifest.get("runtime")
    for section in RESET_GLOBAL_MANIFEST_SECTIONS:
        if section.startswith("runtime."):
            if not isinstance(runtime, dict):
                continue
            runtime_key = section.split(".", 1)[1]
            if runtime_key in runtime:
                del runtime[runtime_key]
                cleared_sections.append(section)
        elif section in manifest:
            del manifest[section]
            cleared_sections.append(section)

    if isinstance(runtime, dict) and not runtime and "runtime" in manifest:
        del manifest["runtime"]

    return manifest, cleared_sections


def _run_reset_migration_all(project_root: Path) -> ResetMigrationOutput:
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    manifest, cleared_manifest_sections = _prepare_reset_migration_all_manifest(project_root)

    for relative_path in RESET_GLOBAL_PATHS:
        path = project_root / relative_path
        if _delete_tree_if_present(path):
            deleted_paths.append(relative_path)
            logger.info(
                "event=reset_migration_global_path_deleted component=reset_migration "
                "operation=run_reset_migration path=%s",
                relative_path,
            )
        else:
            missing_paths.append(relative_path)
            logger.warning(
                "event=reset_migration_global_path_missing component=reset_migration "
                "operation=run_reset_migration path=%s",
                relative_path,
            )

    if manifest is not None and cleared_manifest_sections:
        write_json(project_root / "manifest.json", manifest)

    logger.info(
        "event=reset_migration_global_complete component=reset_migration "
        "operation=run_reset_migration deleted_paths=%s missing_paths=%s "
        "cleared_manifest_sections=%s",
        deleted_paths,
        missing_paths,
        cleared_manifest_sections,
    )

    return ResetMigrationOutput(
        stage="all",
        targets=[],
        reset=[],
        noop=[],
        blocked=[],
        not_found=[],
        deleted_paths=deleted_paths,
        missing_paths=missing_paths,
        cleared_manifest_sections=cleared_manifest_sections,
    )


def run_reset_migration(project_root: Path, stage: str, fqns: list[str]) -> ResetMigrationOutput:
    """Reset pre-model migration state for one or more selected tables.

    The command is intentionally limited to pre-model stages. If any selected
    table already has model generation complete, the entire run is blocked
    before any mutation occurs.
    """
    if stage == "all":
        if fqns:
            raise ValueError("global reset stage 'all' does not accept table arguments")
        return _run_reset_migration_all(project_root)

    if stage not in RESETTABLE_STAGES:
        raise ValueError(f"Unsupported reset stage: {stage}")

    normalized = [normalize(fqn) for fqn in fqns]
    targets: list[ResetTargetResult] = []
    blocked: list[str] = []
    not_found: list[str] = []

    resolved_tables: list[tuple[str, dict[str, Any]]] = []
    for norm in normalized:
        bucket = detect_catalog_bucket(project_root, norm)
        if bucket != "tables":
            targets.append(
                ResetTargetResult(
                    fqn=norm,
                    status="not_found",
                    reason="table_catalog_not_found",
                )
            )
            not_found.append(norm)
            continue

        table_path = project_root / "catalog" / "tables" / f"{norm}.json"
        table_data = _read_catalog_json(table_path)
        generate_status = (
            table_data.get("generate", {}).get("status")
            if isinstance(table_data.get("generate"), dict)
            else None
        )
        if generate_status == "ok":
            targets.append(
                ResetTargetResult(
                    fqn=norm,
                    status="blocked",
                    reason="generate_already_complete",
                )
            )
            blocked.append(norm)
            continue

        resolved_tables.append((norm, table_data))

    if blocked:
        return ResetMigrationOutput(
            stage=stage,
            targets=targets,
            reset=[],
            noop=[],
            blocked=blocked,
            not_found=not_found,
        )

    reset: list[str] = []
    noop: list[str] = []

    for norm, _table_data in resolved_tables:
        cleared_sections, deleted_files, writer = _reset_table_sections(project_root, norm, stage)
        if stage in ("scope", "profile", "generate-tests", "refactor"):
            cleared_sections.extend(_reset_writer_refactor(project_root, writer))

        status = "reset" if cleared_sections or deleted_files else "noop"
        if status == "reset":
            reset.append(norm)
        else:
            noop.append(norm)

        targets.append(
            ResetTargetResult(
                fqn=norm,
                status=status,
                cleared_sections=cleared_sections,
                deleted_files=deleted_files,
            )
        )

    return ResetMigrationOutput(
        stage=stage,
        targets=targets,
        reset=reset,
        noop=noop,
        blocked=[],
        not_found=not_found,
    )


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
