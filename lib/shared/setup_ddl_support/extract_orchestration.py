"""Extract orchestration for setup-ddl source extraction."""

from __future__ import annotations

import logging
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any

from shared.setup_ddl_support.assembly import assemble_ddl_from_staging
from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale, run_write_catalog
from shared.setup_ddl_support.db_extraction import run_db_extraction
from shared.setup_ddl_support.manifest import (
    TECH_DIALECT,
    get_connection_identity,
    identity_changed,
    read_manifest_strict,
    require_technology,
    run_write_manifest,
)

logger = logging.getLogger(__name__)


def _catalog_snapshot(project_root: Path) -> dict[str, str]:
    catalog_dir = project_root / "catalog"
    if not catalog_dir.is_dir():
        return {}
    return {
        str(path.relative_to(project_root)): sha256(path.read_bytes()).hexdigest()
        for path in sorted(catalog_dir.glob("*/*.json"))
        if path.is_file()
    }


def _changed_catalog_paths(project_root: Path, before: dict[str, str]) -> list[str]:
    after = _catalog_snapshot(project_root)
    return sorted(path for path, digest in after.items() if before.get(path) != digest)


def run_extract(project_root: Path, database: str | None, schemas: list[str]) -> dict[str, Any]:
    from shared.catalog import restore_enriched_fields, snapshot_enriched_fields
    from shared.catalog_enrich import enrich_catalog
    from shared.diagnostics import run_diagnostics

    technology = require_technology(project_root)
    if not schemas:
        raise ValueError("--schemas is required and must be non-empty")
    if technology == "sql_server" and not database:
        raise ValueError(f"--database is required for technology '{technology}'")
    dialect = TECH_DIALECT.get(technology, "tsql")
    db_name = database or ""
    logger.info("event=extract_start technology=%s database=%s schemas=%s", technology, db_name, schemas)
    existing_manifest = read_manifest_strict(project_root)
    current_identity = get_connection_identity(technology, db_name)
    if identity_changed(existing_manifest, current_identity):
        logger.info("event=identity_changed technology=%s pre_stale_all=true", technology)
        stale_paths = mark_all_catalog_stale(project_root)
    else:
        stale_paths = []
    enriched_snapshot = snapshot_enriched_fields(project_root)
    with tempfile.TemporaryDirectory() as tmp:
        staging_dir = Path(tmp)
        run_db_extraction(technology, staging_dir, db_name, schemas)
        ddl_paths = assemble_ddl_from_staging(staging_dir, project_root)
        run_write_manifest(project_root, technology, db_name, schemas)
        counts = run_write_catalog(staging_dir, project_root, db_name)
    catalog_snapshot = _catalog_snapshot(project_root)
    restore_enriched_fields(project_root, enriched_snapshot)
    enrich_result = enrich_catalog(project_root, dialect=dialect)
    diag_result = run_diagnostics(project_root, dialect=dialect)
    logger.info(
        "event=extract_complete technology=%s tables=%s procedures=%s enrich=%s diagnostics=%s",
        technology,
        counts.get("tables"),
        counts.get("procedures"),
        enrich_result,
        diag_result,
    )
    written_paths = ["manifest.json", *stale_paths, *ddl_paths]
    written_paths.extend(str(path) for path in counts.get("written_paths", []))
    written_paths.extend(_changed_catalog_paths(project_root, catalog_snapshot))
    return {
        **counts,
        "written_paths": sorted(set(written_paths)),
        "enrich": enrich_result.model_dump() if hasattr(enrich_result, "model_dump") else enrich_result,
        "diagnostics": diag_result,
    }
