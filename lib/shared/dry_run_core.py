"""Compatibility barrel for migrate-util command services.

Implementation lives in focused modules under ``shared.dry_run_support``.
"""

from __future__ import annotations

from pathlib import Path

from shared.dry_run_support.common import (
    RESET_GLOBAL_MANIFEST_SECTIONS,
    RESET_GLOBAL_PATHS,
    RESETTABLE_STAGES,
    VALID_STAGES,
    _RESET_STAGE_SECTIONS,
    detail as _detail,
    display_scope_status as _display_scope_status,
    load_manifest_json as _load_manifest_json,
    object_detail as _object_detail,
    read_catalog_json as _read_catalog_json,
    runtime_role_is_configured as _runtime_role_is_configured,
)
from shared.dry_run_support.excluded_warnings import run_sync_excluded_warnings
from shared.dry_run_support.exclusions import run_exclude
from shared.dry_run_support.readiness import _project_stage_ready, run_ready
from shared.dry_run_support.reset import (
    _delete_if_present,
    _delete_tree_if_present,
    _prepare_reset_migration_all_manifest,
    _reset_table_sections,
    _reset_writer_refactor,
    _run_reset_migration_all,
    run_reset_migration,
)
from shared.dry_run_support.status import run_status as _run_status
from shared.output_models.dry_run import ObjectStatus, StatusOutput


def run_status(project_root: Path, fqn: str | None = None) -> StatusOutput | ObjectStatus:
    """Collate CLI-written statuses from catalog files."""
    return _run_status(project_root, fqn)


__all__ = [
    "RESET_GLOBAL_MANIFEST_SECTIONS",
    "RESET_GLOBAL_PATHS",
    "RESETTABLE_STAGES",
    "VALID_STAGES",
    "run_exclude",
    "run_ready",
    "run_reset_migration",
    "run_status",
    "run_sync_excluded_warnings",
]
