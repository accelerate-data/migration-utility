"""Manifest and source-identity helpers for setup-ddl."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.runtime_config import (
    KNOWN_TECHNOLOGIES,
    TECH_DIALECT,
    dialect_for_technology,
    get_primary_technology,
    get_runtime_role,
    set_extraction,
    set_runtime_role,
)
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

logger = logging.getLogger(__name__)


class UnsupportedOperationError(Exception):
    """Raised when an operation is not supported for the configured technology."""


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
    technology = get_primary_technology(manifest)
    if technology is None:
        raise ValueError(
            "manifest.json has no source technology configured. Run /init-ad-migration."
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
    if technology == "sql_server":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                host=os.environ.get("MSSQL_HOST", "") or None,
                port=os.environ.get("MSSQL_PORT", "") or None,
                database=database or None,
                user=os.environ.get("MSSQL_USER", "sa") or None,
                password_env="SA_PASSWORD",
                driver=os.environ.get("MSSQL_DRIVER", "FreeTDS") or None,
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    if technology == "oracle":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                dsn=os.environ.get("ORACLE_DSN", "") or None,
                host=os.environ.get("ORACLE_HOST", "") or None,
                port=os.environ.get("ORACLE_PORT", "") or None,
                service=os.environ.get("ORACLE_SERVICE", "") or None,
                user=os.environ.get("ORACLE_USER", "") or None,
                schema=database or os.environ.get("ORACLE_SCHEMA", "") or None,
                password_env="ORACLE_PASSWORD",
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    if technology == "duckdb":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                path=database or os.environ.get("DUCKDB_PATH", "") or None,
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    return {}


def identity_changed(existing_manifest: dict[str, Any], current_identity: dict[str, Any]) -> bool:
    if not current_identity:
        return False
    existing_source = get_runtime_role(existing_manifest, "source")
    if existing_source is None:
        return False
    identity_fields_by_tech = {
        "sql_server": {"host", "port", "database"},
        "oracle": {"dsn", "host", "port", "service", "schema"},
        "duckdb": {"path"},
    }
    identity_fields = identity_fields_by_tech.get(
        existing_source.technology,
        set(current_identity.get("connection", {}).keys()),
    )
    for key, value in current_identity.get("connection", {}).items():
        if key not in identity_fields:
            continue
        if not value:
            continue
        field_name = "schema_name" if key == "schema" else key
        if getattr(existing_source.connection, field_name, None) != value:
            return True
    return False


def build_runtime_role(technology: str, database: str) -> RuntimeRole:
    """Build a typed runtime role from the current environment for a technology."""
    return RuntimeRole.model_validate(get_connection_identity(technology, database))


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
    source_role = build_runtime_role(technology, database)
    manifest = {**existing, "schema_version": "1.0"}
    manifest = set_runtime_role(manifest, "source", source_role)
    manifest = set_extraction(manifest, schemas, datetime.now(timezone.utc).isoformat())
    project_root.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {"file": str(out_path)}
