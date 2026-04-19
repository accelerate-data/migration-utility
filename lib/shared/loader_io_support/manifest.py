"""Manifest read/write helpers for shared.loader_io."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.runtime_config import (
    get_primary_dialect,
    get_runtime_role,
    set_runtime_role,
    validate_supported_technologies,
)
from shared.runtime_config_models import RuntimeRole

logger = logging.getLogger(__name__)


def read_manifest(project_root: Path) -> dict[str, Any]:
    """Read manifest.json from project_root if present.

    Returns the full manifest dict with dialect defaulting to tsql.
    If manifest.json does not exist, returns a minimal dict with only dialect.
    """
    manifest_file = Path(project_root) / "manifest.json"
    if manifest_file.exists():
        try:
            with manifest_file.open() as f:
                m = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json in {project_root} is not valid JSON: {exc}") from exc
        validate_supported_technologies(m)
        m["dialect"] = get_primary_dialect(m)
        return m
    return {"dialect": "tsql"}


def _require_manifest_file(project_root: Path) -> Path:
    """Return manifest.json path, raising if it does not exist on disk."""
    manifest_file = Path(project_root) / "manifest.json"
    if not manifest_file.exists():
        raise FileNotFoundError(f"manifest.json not found in {project_root}")
    return manifest_file


def write_manifest_sandbox(project_root: Path, database: str) -> None:
    """Persist sandbox database name into manifest.json."""
    manifest_file = _require_manifest_file(project_root)
    manifest = read_manifest(project_root)
    sandbox_role = get_runtime_role(manifest, "sandbox")
    if sandbox_role is None:
        raise ValueError("manifest.json is missing runtime.sandbox")

    connection = sandbox_role.connection.model_copy(deep=True)
    if sandbox_role.technology == "oracle":
        connection.schema_name = database
    elif sandbox_role.technology == "sql_server":
        connection.database = database
    else:
        raise ValueError(f"Unsupported sandbox technology: {sandbox_role.technology}")

    manifest = set_runtime_role(
        manifest,
        "sandbox",
        RuntimeRole(
            technology=sandbox_role.technology,
            dialect=sandbox_role.dialect,
            connection=connection,
            schemas=sandbox_role.schemas,
        ),
    )
    manifest_file.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info(
        "event=manifest_update operation=write_sandbox database=%s",
        database,
    )


def clear_manifest_sandbox(project_root: Path) -> None:
    """Remove the sandbox key from manifest.json."""
    manifest_file = _require_manifest_file(project_root)
    manifest = read_manifest(project_root)
    manifest = set_runtime_role(manifest, "sandbox", None)
    manifest_file.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("event=manifest_update operation=clear_sandbox")
