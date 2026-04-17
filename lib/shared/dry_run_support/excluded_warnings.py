from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import write_json
from shared.deps import collect_deps
from shared.loader_data import CatalogLoadError
from shared.output_models.dry_run import SyncExcludedWarningsOutput

logger = logging.getLogger(__name__)


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
