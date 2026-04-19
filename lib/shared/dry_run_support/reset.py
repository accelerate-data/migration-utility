from __future__ import annotations

from pathlib import Path

from shared.dry_run_support.common import RESETTABLE_STAGES
from shared.dry_run_support.reset_files import delete_if_present, delete_tree_if_present
from shared.dry_run_support.reset_global import (
    prepare_reset_migration_all_manifest,
    run_reset_migration_all,
)
from shared.dry_run_support.reset_preserve_catalog import run_reset_migration_all_preserve_catalog
from shared.dry_run_support.reset_stage import (
    reset_table_sections,
    reset_writer_refactor,
    run_reset_migration_stage,
)
from shared.output_models.dry_run import ResetMigrationOutput


def run_reset_migration(
    project_root: Path,
    stage: str,
    fqns: list[str],
    *,
    preserve_catalog: bool = False,
) -> ResetMigrationOutput:
    """Reset pre-model migration state for one or more selected tables."""
    if stage == "all":
        if fqns:
            raise ValueError("global reset stage 'all' does not accept table arguments")
        if preserve_catalog:
            return run_reset_migration_all_preserve_catalog(project_root)
        return run_reset_migration_all(project_root)
    if preserve_catalog:
        raise ValueError("--preserve-catalog is only supported with global reset stage 'all'")
    return run_reset_migration_stage(project_root, stage, fqns)


__all__ = [
    "RESETTABLE_STAGES",
    "delete_if_present",
    "delete_tree_if_present",
    "prepare_reset_migration_all_manifest",
    "reset_table_sections",
    "reset_writer_refactor",
    "run_reset_migration",
    "run_reset_migration_all",
    "run_reset_migration_all_preserve_catalog",
    "run_reset_migration_stage",
]
