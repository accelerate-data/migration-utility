"""Preserve-catalog global reset support."""

from __future__ import annotations

import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.catalog import write_json
from shared.dry_run_support.common import (
    RESET_PRESERVE_CATALOG_PATHS,
    RESET_PRESERVE_CATALOG_SECTIONS_BY_BUCKET,
    read_catalog_json,
)
from shared.output_models.dry_run import ResetCatalogSection, ResetMigrationOutput

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PreserveCatalogMutation:
    path: Path
    original: dict[str, Any]
    updated: dict[str, Any]
    cleared: list[ResetCatalogSection]


def load_preserve_catalog_mutations(project_root: Path) -> list[PreserveCatalogMutation]:
    catalog_dir = project_root / "catalog"
    mutations: list[PreserveCatalogMutation] = []

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
            updated = dict(data)
            for key in section_keys:
                updated.pop(key, None)
            mutations.append(
                PreserveCatalogMutation(
                    path=path,
                    original=data,
                    updated=updated,
                    cleared=cleared,
                )
            )

    return mutations


def rollback_preserve_catalog_writes(
    project_root: Path,
    written_catalogs: list[tuple[Path, dict[str, Any]]],
) -> None:
    for rollback_path, original in reversed(written_catalogs):
        write_json(rollback_path, original)
        logger.warning(
            "event=reset_migration_preserve_catalog_rollback "
            "component=reset_migration operation=run_reset_migration path=%s",
            rollback_path.relative_to(project_root),
        )


def delete_preserve_catalog_paths(project_root: Path) -> tuple[list[str], list[str]]:
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    staged_paths: list[tuple[Path, Path]] = []

    with tempfile.TemporaryDirectory(prefix=".preserve-catalog-reset-", dir=project_root) as temp_dir:
        temp_path = Path(temp_dir)
        try:
            for index, relative_path in enumerate(RESET_PRESERVE_CATALOG_PATHS):
                path = project_root / relative_path
                if not path.exists():
                    missing_paths.append(relative_path)
                    logger.warning(
                        "event=reset_migration_preserve_catalog_path_missing component=reset_migration "
                        "operation=run_reset_migration path=%s",
                        relative_path,
                    )
                    continue

                staged_path = temp_path / str(index)
                shutil.move(str(path), str(staged_path))
                staged_paths.append((path, staged_path))
                deleted_paths.append(relative_path)
                logger.info(
                    "event=reset_migration_preserve_catalog_path_deleted component=reset_migration "
                    "operation=run_reset_migration path=%s",
                    relative_path,
                )
        except OSError:
            for original_path, staged_path in reversed(staged_paths):
                if staged_path.exists() and not original_path.exists():
                    shutil.move(str(staged_path), str(original_path))
            raise

    return deleted_paths, missing_paths


def run_reset_migration_all_preserve_catalog(project_root: Path) -> ResetMigrationOutput:
    mutations = load_preserve_catalog_mutations(project_root)
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    cleared_catalog_sections: list[ResetCatalogSection] = []
    cleared_catalog_paths: list[str] = []
    written_catalogs: list[tuple[Path, dict[str, Any]]] = []

    try:
        for mutation in mutations:
            write_json(mutation.path, mutation.updated)
            written_catalogs.append((mutation.path, mutation.original))
            cleared_catalog_sections.extend(mutation.cleared)
            relative_path = str(mutation.path.relative_to(project_root))
            if relative_path not in cleared_catalog_paths:
                cleared_catalog_paths.append(relative_path)
            logger.info(
                "event=reset_migration_preserve_catalog_sections_cleared "
                "component=reset_migration operation=run_reset_migration path=%s sections=%s",
                relative_path,
                [item.section for item in mutation.cleared],
            )
    except OSError:
        rollback_preserve_catalog_writes(project_root, written_catalogs)
        raise

    try:
        deleted_paths, missing_paths = delete_preserve_catalog_paths(project_root)
    except OSError:
        rollback_preserve_catalog_writes(project_root, written_catalogs)
        raise

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
