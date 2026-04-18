from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any

from shared.catalog import detect_catalog_bucket, write_json
from shared.dry_run_support.common import (
    RESET_GLOBAL_MANIFEST_SECTIONS,
    RESET_GLOBAL_PATHS,
    RESET_PRESERVE_CATALOG_SECTIONS_BY_BUCKET,
    RESET_PRESERVE_CATALOG_PATHS,
    RESETTABLE_STAGES,
    _RESET_STAGE_SECTIONS,
    read_catalog_json,
)
from shared.name_resolver import normalize
from shared.output_models.dry_run import ResetCatalogSection, ResetMigrationOutput, ResetTargetResult

logger = logging.getLogger(__name__)




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
) -> tuple[list[str], list[str], list[str], str | None]:
    table_path = project_root / "catalog" / "tables" / f"{norm}.json"
    table_data = read_catalog_json(table_path)
    writer = (
        table_data.get("scoping", {}).get("selected_writer")
        if isinstance(table_data.get("scoping"), dict)
        else None
    )

    cleared_sections: list[str] = []
    deleted_files: list[str] = []
    mutated_files: list[str] = []

    for key in _RESET_STAGE_SECTIONS[stage]:
        if key in table_data:
            del table_data[key]
            cleared_sections.append(f"table.{key}")

    if stage in ("scope", "profile", "generate-tests"):
        spec_path = project_root / "test-specs" / f"{norm}.json"
        if _delete_if_present(spec_path):
            deleted_files.append(f"test-specs/{norm}.json")

    write_json(table_path, table_data)
    if cleared_sections:
        mutated_files.append(str(table_path.relative_to(project_root)))
    return cleared_sections, deleted_files, mutated_files, writer


def _reset_writer_refactor(
    project_root: Path,
    writer_fqn: str | None,
) -> tuple[list[str], list[str]]:
    if not writer_fqn:
        return [], []

    writer_norm = normalize(writer_fqn)
    proc_path = project_root / "catalog" / "procedures" / f"{writer_norm}.json"
    if not proc_path.exists():
        return [], []

    proc_data = read_catalog_json(proc_path)
    if "refactor" not in proc_data:
        return [], []

    del proc_data["refactor"]
    write_json(proc_path, proc_data)
    return [f"procedure:{writer_norm}.refactor"], [str(proc_path.relative_to(project_root))]


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


def _load_preserve_catalog_mutations(project_root: Path) -> list[tuple[Path, dict[str, Any], list[ResetCatalogSection]]]:
    catalog_dir = project_root / "catalog"
    mutations: list[tuple[Path, dict[str, Any], list[ResetCatalogSection]]] = []

    for bucket, (label, section_keys) in RESET_PRESERVE_CATALOG_SECTIONS_BY_BUCKET.items():
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for path in sorted(bucket_dir.glob("*.json")):
            data = read_catalog_json(path)
            relative_path = str(path.relative_to(project_root))
            cleared = [
                ResetCatalogSection(path=relative_path, section=f"{label}.{key}")
                for key in section_keys
                if key in data
            ]
            if not cleared:
                continue
            for key in section_keys:
                data.pop(key, None)
            mutations.append((path, data, cleared))

    return mutations


def _run_reset_migration_all_preserve_catalog(project_root: Path) -> ResetMigrationOutput:
    mutations = _load_preserve_catalog_mutations(project_root)
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    cleared_catalog_sections: list[ResetCatalogSection] = []
    cleared_catalog_paths: list[str] = []

    for path, data, cleared in mutations:
        write_json(path, data)
        cleared_catalog_sections.extend(cleared)
        relative_path = str(path.relative_to(project_root))
        if relative_path not in cleared_catalog_paths:
            cleared_catalog_paths.append(relative_path)
        logger.info(
            "event=reset_migration_preserve_catalog_sections_cleared "
            "component=reset_migration operation=run_reset_migration path=%s sections=%s",
            relative_path,
            [item.section for item in cleared],
        )

    for relative_path in RESET_PRESERVE_CATALOG_PATHS:
        path = project_root / relative_path
        if _delete_tree_if_present(path):
            deleted_paths.append(relative_path)
            logger.info(
                "event=reset_migration_preserve_catalog_path_deleted component=reset_migration "
                "operation=run_reset_migration path=%s",
                relative_path,
            )
        else:
            missing_paths.append(relative_path)
            logger.warning(
                "event=reset_migration_preserve_catalog_path_missing component=reset_migration "
                "operation=run_reset_migration path=%s",
                relative_path,
            )

    logger.info(
        "event=reset_migration_preserve_catalog_complete component=reset_migration "
        "operation=run_reset_migration deleted_paths=%s missing_paths=%s "
        "cleared_catalog_sections=%s",
        deleted_paths,
        missing_paths,
        cleared_catalog_sections,
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
        cleared_catalog_sections=cleared_catalog_sections,
        cleared_catalog_paths=cleared_catalog_paths,
    )


def run_reset_migration(
    project_root: Path,
    stage: str,
    fqns: list[str],
    *,
    preserve_catalog: bool = False,
) -> ResetMigrationOutput:
    """Reset pre-model migration state for one or more selected tables.

    The command is intentionally limited to pre-model stages. If any selected
    table already has model generation complete, the entire run is blocked
    before any mutation occurs.
    """
    if stage == "all":
        if fqns:
            raise ValueError("global reset stage 'all' does not accept table arguments")
        if preserve_catalog:
            return _run_reset_migration_all_preserve_catalog(project_root)
        return _run_reset_migration_all(project_root)
    if preserve_catalog:
        raise ValueError("--preserve-catalog is only supported with global reset stage 'all'")

    if stage not in RESETTABLE_STAGES:
        raise ValueError(f"Unsupported reset stage: {stage}")
    if not fqns:
        raise ValueError("reset-migration requires at least one FQN for staged resets")

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
        table_data = read_catalog_json(table_path)
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
        cleared_sections, deleted_files, mutated_files, writer = _reset_table_sections(project_root, norm, stage)
        if stage in ("scope", "profile", "generate-tests", "refactor"):
            writer_sections, writer_files = _reset_writer_refactor(project_root, writer)
            cleared_sections.extend(writer_sections)
            mutated_files.extend(writer_files)

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
                mutated_files=mutated_files,
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
