"""Diagnostic execution and catalog result writing."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from shared.catalog import has_catalog, write_json
from shared.diagnostics.context import (
    CatalogContext,
    build_ddl_lookup,
    build_known_fqns,
    load_package_members,
)
from shared.diagnostics.registry import DiagnosticRegistry, DiagnosticResult, _REGISTRY
from shared.env_config import resolve_catalog_dir
from shared.loader import DdlEntry, load_directory

logger = logging.getLogger(__name__)

OBJECT_BUCKETS = ("procedures", "views", "functions")


def run_checks(
    catalog_dir: Path,
    project_root: Path,
    dialect: str,
    known_fqns: dict[str, set[str]],
    ddl_lookup: dict[str, DdlEntry],
    pass_number: int,
    pass1_results: dict[str, list[DiagnosticResult]] | None = None,
    package_members: set[str] | None = None,
    registry: DiagnosticRegistry = _REGISTRY,
) -> tuple[dict[str, list[DiagnosticResult]], int]:
    """Run all checks for a given pass number across all catalog objects."""
    results: dict[str, list[DiagnosticResult]] = defaultdict(list)
    suppressed_checks = 0

    for bucket in OBJECT_BUCKETS:
        object_type = bucket.rstrip("s")
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_path in sorted(bucket_dir.glob("*.json")):
            fqn = json_path.stem
            try:
                catalog_data = json.loads(json_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("event=diagnostics_skip fqn=%s error=%s", fqn, exc)
                continue

            ctx = CatalogContext(
                project_root=project_root,
                dialect=dialect,
                fqn=fqn,
                object_type=object_type,
                catalog_data=catalog_data,
                known_fqns=known_fqns,
                ddl_entry=ddl_lookup.get(fqn),
                pass1_results=pass1_results,
                package_members=package_members,
            )

            checks = registry.checks_for(object_type, dialect, pass_number)
            for spec in checks:
                try:
                    result = spec.fn(ctx)
                except Exception:
                    logger.exception("event=diagnostic_check_error code=%s fqn=%s", spec.code, fqn)
                    suppressed_checks += 1
                    continue
                if result is None:
                    continue
                if isinstance(result, list):
                    results[fqn].extend(result)
                else:
                    results[fqn].append(result)

    return dict(results), suppressed_checks


def write_results(catalog_dir: Path, all_results: dict[str, list[DiagnosticResult]]) -> tuple[int, int]:
    """Write diagnostic results into catalog JSON files. Returns (warnings_added, errors_added)."""
    warnings_added = 0
    errors_added = 0

    for bucket in OBJECT_BUCKETS:
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for json_path in sorted(bucket_dir.glob("*.json")):
            fqn = json_path.stem
            diags = all_results.get(fqn, [])

            try:
                data = json.loads(json_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue

            warnings = [diag.to_dict() for diag in diags if diag.severity == "warning"]
            errors = [diag.to_dict() for diag in diags if diag.severity == "error"]

            data["warnings"] = warnings
            data["errors"] = errors

            write_json(json_path, data)

            warnings_added += len(warnings)
            errors_added += len(errors)

    return warnings_added, errors_added


def run_diagnostics(project_root: Path, dialect: str = "tsql") -> dict[str, Any]:
    """Run all registered diagnostic checks on catalog files."""
    project_root = Path(project_root)

    if not has_catalog(project_root):
        logger.warning("event=run_diagnostics status=skip reason=no_catalog path=%s", project_root)
        return {"objects_checked": 0, "warnings_added": 0, "errors_added": 0}

    catalog_dir = resolve_catalog_dir(project_root)
    ddl_catalog = load_directory(project_root, dialect=dialect)
    ddl_lookup = build_ddl_lookup(ddl_catalog)
    known_fqns = build_known_fqns(catalog_dir)
    package_members = load_package_members(project_root) if dialect == "oracle" else None

    pass1_results, pass1_suppressed = run_checks(
        catalog_dir,
        project_root,
        dialect,
        known_fqns,
        ddl_lookup,
        pass_number=1,
        package_members=package_members,
    )
    pass2_results, pass2_suppressed = run_checks(
        catalog_dir,
        project_root,
        dialect,
        known_fqns,
        ddl_lookup,
        pass_number=2,
        pass1_results=pass1_results,
        package_members=package_members,
    )

    all_results: dict[str, list[DiagnosticResult]] = defaultdict(list)
    for fqn, diags in pass1_results.items():
        all_results[fqn].extend(diags)
    for fqn, diags in pass2_results.items():
        all_results[fqn].extend(diags)

    objects_checked = sum(
        1
        for bucket in OBJECT_BUCKETS
        if (catalog_dir / bucket).is_dir()
        for _path in (catalog_dir / bucket).glob("*.json")
    )
    warnings_added, errors_added = write_results(catalog_dir, dict(all_results))

    summary = {
        "objects_checked": objects_checked,
        "warnings_added": warnings_added,
        "errors_added": errors_added,
        "suppressed_checks": pass1_suppressed + pass2_suppressed,
    }
    logger.info("event=run_diagnostics_complete %s", summary)
    return summary
