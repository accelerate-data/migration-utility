"""Manifest read/write and handoff helpers for setup-ddl."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shared.runtime_config import (
    TECH_DIALECT,
    get_primary_technology,
    get_runtime_role,
    sanitize_manifest,
    set_extraction,
    set_runtime_role,
    validate_supported_dialects,
    validate_supported_technologies,
)
from shared.runtime_config_models import RuntimeRole
from shared.setup_ddl_support.runtime_identity import build_runtime_role

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
    validate_supported_technologies(manifest)
    validate_supported_dialects(manifest)
    technology = get_primary_technology(manifest)
    if technology is None:
        raise ValueError(
            "manifest.json has no source technology configured. Run /init-ad-migration."
        )
    return technology


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


def _seed_runtime_role(
    manifest: dict[str, Any],
    role_name: str,
    technology: str,
) -> dict[str, Any]:
    existing_role = get_runtime_role(manifest, role_name)
    if existing_role is not None and existing_role.technology == technology:
        seeded_role = existing_role.model_copy(update={"dialect": TECH_DIALECT[technology]})
    else:
        seeded_role = RuntimeRole(technology=technology, dialect=TECH_DIALECT[technology])
    return set_runtime_role(manifest, role_name, seeded_role)


def run_write_partial_manifest(
    project_root: Path,
    technology: str,
    target_technology: str,
    prereqs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if technology not in TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {list(TECH_DIALECT.keys())}."
        )
    if target_technology not in TECH_DIALECT:
        raise ValueError(
            "Unknown target technology: "
            f"{target_technology}. Must be one of {list(TECH_DIALECT.keys())}."
        )
    project_root.mkdir(parents=True, exist_ok=True)
    out_path = project_root / "manifest.json"
    existing = read_manifest_or_empty(project_root)
    existing = sanitize_manifest(existing)
    manifest: dict[str, Any] = {**existing, "schema_version": "1.0"}
    manifest = _seed_runtime_role(manifest, "source", technology)
    manifest = _seed_runtime_role(manifest, "sandbox", technology)
    manifest = _seed_runtime_role(manifest, "target", target_technology)
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
    project_root: Path,
    technology: str,
    database: str,
    schemas: list[str],
) -> dict[str, Any]:
    if technology not in TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {list(TECH_DIALECT.keys())}."
        )
    out_path = project_root / "manifest.json"
    existing = read_manifest_or_empty(project_root)
    existing = sanitize_manifest(existing)
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
