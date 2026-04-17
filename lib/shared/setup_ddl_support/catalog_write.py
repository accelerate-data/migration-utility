"""Catalog diff/write helpers for setup-ddl."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from shared.catalog import write_json as write_catalog_json
from shared.env_config import resolve_catalog_dir
from shared.setup_ddl_support.staging_io import load_staging_catalog_inputs
from shared.setup_ddl_support.staging_signals import build_catalog_write_inputs

logger = logging.getLogger(__name__)


def mark_stale(project_root: Path, removed_fqns: set[str]) -> list[str]:
    catalog_dir = resolve_catalog_dir(project_root)
    written_paths: list[str] = []
    for fqn in sorted(removed_fqns):
        for bucket in ("tables", "procedures", "views", "functions"):
            path = catalog_dir / bucket / f"{fqn}.json"
            if not path.exists():
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                data["stale"] = True
                write_catalog_json(path, data)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("event=mark_stale_error fqn=%s error=%s", fqn, exc)
                continue
            written_paths.append(f"catalog/{bucket}/{fqn}.json")
            logger.warning("event=catalog_stale_object fqn=%s bucket=%s", fqn, bucket)
            break
    return written_paths


def mark_all_catalog_stale(project_root: Path) -> list[str]:
    catalog_dir = resolve_catalog_dir(project_root)
    if not catalog_dir.is_dir():
        return []
    count = 0
    written_paths: list[str] = []
    for bucket in ("tables", "procedures", "views", "functions"):
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for path in sorted(bucket_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not data.get("stale"):
                    data["stale"] = True
                    write_catalog_json(path, data)
                    count += 1
                    written_paths.append(f"catalog/{bucket}/{path.name}")
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("event=mark_all_stale_error path=%s error=%s", path, exc)
    logger.info("event=mark_all_stale_identity_changed count=%d", count)
    return written_paths


def reclassify_materialized_views(project_root: Path, mv_fqns: set[str]) -> list[str]:
    if not mv_fqns:
        return []
    catalog_dir = resolve_catalog_dir(project_root)
    written_paths: list[str] = []
    for fqn in mv_fqns:
        table_path = catalog_dir / "tables" / f"{fqn}.json"
        if not table_path.exists():
            continue
        logger.info("event=mv_reclassify fqn=%s from=tables to=views", fqn)
        try:
            table_data = json.loads(table_path.read_text(encoding="utf-8"))
            table_data["is_materialized_view"] = True
            views_dir = catalog_dir / "views"
            views_dir.mkdir(parents=True, exist_ok=True)
            write_catalog_json(views_dir / f"{fqn}.json", table_data)
            table_path.unlink()
            written_paths.extend([
                f"catalog/views/{fqn}.json",
                f"catalog/tables/{fqn}.json",
            ])
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("event=mv_reclassify_error fqn=%s error=%s", fqn, exc)
    return written_paths


def ensure_catalog_subdirectories(project_root: Path) -> None:
    catalog_dir = resolve_catalog_dir(project_root)
    for subdir in ("tables", "procedures", "views", "functions"):
        (catalog_dir / subdir).mkdir(parents=True, exist_ok=True)


def run_write_catalog(staging_dir: Path, project_root: Path, database: str) -> dict[str, object]:
    from shared.catalog import detect_catalog_bucket
    from shared.catalog_diff import classify_objects, compute_object_hashes, load_existing_hashes
    from shared.catalog_dmf import write_catalog_files

    staging_inputs = load_staging_catalog_inputs(staging_dir)
    derived_inputs = build_catalog_write_inputs(staging_inputs)
    fresh_hashes = compute_object_hashes(
        staging_inputs["definitions_rows"],
        derived_inputs["table_signals"],
        derived_inputs["object_types"],
    )
    existing_hashes = load_existing_hashes(project_root)
    diff = classify_objects(fresh_hashes, existing_hashes)
    logger.info(
        "event=catalog_diff unchanged=%d changed=%d new=%d removed=%d",
        len(diff.unchanged), len(diff.changed), len(diff.new), len(diff.removed),
    )
    early_written_paths: list[str] = []
    if diff.removed:
        early_written_paths.extend(mark_stale(project_root, diff.removed))
    early_written_paths.extend(reclassify_materialized_views(project_root, derived_inputs["mv_fqns"]))
    ensure_catalog_subdirectories(project_root)
    write_filter = diff.changed | diff.new
    counts = write_catalog_files(
        project_root,
        table_signals=derived_inputs["table_signals"],
        proc_dmf_rows=staging_inputs["proc_dmf_rows"],
        view_dmf_rows=staging_inputs["view_dmf_rows"],
        func_dmf_rows=staging_inputs["func_dmf_rows"],
        object_types=derived_inputs["object_types"],
        routing_flags=derived_inputs["routing_flags"],
        database=database,
        proc_params=derived_inputs["proc_params"],
        write_filter=write_filter,
        hashes=fresh_hashes,
        view_definitions=derived_inputs["view_definitions"],
        view_columns=derived_inputs["view_columns"],
        mv_fqns=derived_inputs["mv_fqns"],
        subtypes=derived_inputs["function_subtypes"],
        long_truncation_fqns=derived_inputs["long_truncation_fqns"],
    )
    counts["unchanged"] = len(diff.unchanged)
    counts["changed"] = len(diff.changed)
    counts["new"] = len(diff.new)
    counts["removed"] = len(diff.removed)
    written_paths: list[str] = list(early_written_paths)
    for fqn in sorted(write_filter):
        bucket = derived_inputs["object_types"].get(fqn)
        if not bucket and fqn in derived_inputs["table_signals"]:
            bucket = "tables"
        if bucket:
            written_paths.append(f"catalog/{bucket}/{fqn}.json")
    for fqn in sorted(diff.removed):
        bucket = detect_catalog_bucket(project_root, fqn)
        if bucket:
            written_paths.append(f"catalog/{bucket}/{fqn}.json")
    counts["written_paths"] = sorted(set(written_paths))
    return counts
