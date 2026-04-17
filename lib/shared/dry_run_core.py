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
)
from shared.dry_run_support.excluded_warnings import run_sync_excluded_warnings
from shared.dry_run_support.exclusions import run_exclude
from shared.dry_run_support.readiness import run_ready
from shared.dry_run_support.reset import (
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
