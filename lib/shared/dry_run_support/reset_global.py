from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import write_json
from shared.dry_run_support.common import RESET_GLOBAL_MANIFEST_SECTIONS, RESET_GLOBAL_PATHS
from shared.dry_run_support.reset_files import delete_tree_if_present
from shared.output_models.dry_run import ResetMigrationOutput

logger = logging.getLogger(__name__)


def prepare_reset_migration_all_manifest(project_root: Path) -> tuple[dict[str, Any] | None, list[str]]:
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


def run_reset_migration_all(project_root: Path) -> ResetMigrationOutput:
    deleted_paths: list[str] = []
    missing_paths: list[str] = []
    manifest, cleared_manifest_sections = prepare_reset_migration_all_manifest(project_root)

    for relative_path in RESET_GLOBAL_PATHS:
        path = project_root / relative_path
        if delete_tree_if_present(path):
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


__all__ = ["prepare_reset_migration_all_manifest", "run_reset_migration_all"]
