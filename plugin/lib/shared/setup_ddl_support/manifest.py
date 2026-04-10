"""Manifest and source-identity helpers for setup-ddl."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class UnsupportedOperationError(Exception):
    """Raised when an operation is not supported for the configured technology."""


TECH_DIALECT = {
    "sql_server": "tsql",
    "fabric_warehouse": "tsql",
    "fabric_lakehouse": "spark",
    "snowflake": "snowflake",
    "oracle": "oracle",
}

KNOWN_TECHNOLOGIES = frozenset(
    {"sql_server", "fabric_warehouse", "fabric_lakehouse", "snowflake", "oracle"}
)


def require_technology(project_root: Path) -> str:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(
            "manifest.json not found. Run /init-ad-migration to initialise the project."
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"manifest.json is not valid JSON: {exc}") from exc
    technology = manifest.get("technology")
    if technology is None:
        raise ValueError(
            "manifest.json has no 'technology' field. Run /init-ad-migration."
        )
    if technology not in KNOWN_TECHNOLOGIES:
        raise ValueError(
            f"technology '{technology}' is not recognised. "
            f"Known: {sorted(KNOWN_TECHNOLOGIES)}."
        )
    return technology


def build_oracle_schema_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entry_type = dict[str, Any]
    buckets: dict[str, entry_type] = {}
    for row in rows:
        owner = row.get("OWNER") or row.get("owner") or ""
        obj_type = (row.get("OBJECT_TYPE") or row.get("object_type") or "").upper()
        if not owner:
            continue
        if owner not in buckets:
            buckets[owner] = {
                "owner": owner,
                "tables": 0,
                "procedures": 0,
                "views": 0,
                "functions": 0,
                "materialized_views": 0,
            }
        if obj_type == "TABLE":
            buckets[owner]["tables"] += 1
        elif obj_type == "PROCEDURE":
            buckets[owner]["procedures"] += 1
        elif obj_type == "VIEW":
            buckets[owner]["views"] += 1
        elif obj_type == "MATERIALIZED VIEW":
            buckets[owner]["materialized_views"] += 1
        elif obj_type == "FUNCTION":
            buckets[owner]["functions"] += 1
    return sorted(buckets.values(), key=lambda x: x["owner"])


def read_manifest_strict(project_root: Path) -> dict[str, Any]:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(
            "manifest.json not found. Run /init-ad-migration to initialise the project."
        )
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"manifest.json is not valid JSON: {exc}") from exc


def read_manifest_or_empty(project_root: Path) -> dict[str, Any]:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("event=manifest_read_error path=%s error=%s", manifest_path, exc)
        return {}


def get_connection_identity(technology: str, database: str) -> dict[str, Any]:
    if technology in ("sql_server", "fabric_warehouse"):
        return {
            "source_host": os.environ.get("MSSQL_HOST", ""),
            "source_port": os.environ.get("MSSQL_PORT", ""),
            "source_database": database,
        }
    if technology == "oracle":
        return {"source_dsn": os.environ.get("ORACLE_DSN", "")}
    return {}


def identity_changed(existing_manifest: dict[str, Any], current_identity: dict[str, Any]) -> bool:
    non_empty = {k: v for k, v in current_identity.items() if v}
    if not non_empty:
        return False
    for key, value in non_empty.items():
        if existing_manifest.get(key, "") != value:
            return True
    return False


def run_write_partial_manifest(
    project_root: Path,
    technology: str,
    prereqs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if technology not in TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {list(TECH_DIALECT.keys())}."
        )
    project_root.mkdir(parents=True, exist_ok=True)
    out_path = project_root / "manifest.json"
    existing = read_manifest_or_empty(project_root)
    manifest: dict[str, Any] = {
        **existing,
        "schema_version": "1.0",
        "technology": technology,
        "dialect": TECH_DIALECT[technology],
    }
    if prereqs is not None:
        manifest["init_handoff"] = {
            **prereqs,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "event=write_partial_manifest technology=%s has_handoff=%s",
        technology,
        prereqs is not None,
    )
    return {"file": str(out_path)}


def run_read_handoff(project_root: Path) -> dict[str, Any] | None:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("event=read_handoff_error operation=read_manifest error=%s", exc)
        return None
    handoff = manifest.get("init_handoff")
    if handoff is not None:
        logger.info("event=read_handoff status=found")
    return handoff


def run_write_manifest(
    project_root: Path, technology: str, database: str, schemas: list[str],
) -> dict[str, Any]:
    if technology not in TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {list(TECH_DIALECT.keys())}."
        )
    out_path = project_root / "manifest.json"
    existing = read_manifest_or_empty(project_root)
    manifest = {
        **existing,
        "schema_version": "1.0",
        "technology": technology,
        "dialect": TECH_DIALECT[technology],
        "source_database": database,
        "extracted_schemas": schemas,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        **get_connection_identity(technology, database),
    }
    project_root.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {"file": str(out_path)}
