"""Target runtime manifest helpers."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from shared.runtime_config import dialect_for_technology, get_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole, RuntimeSchemas
from shared.setup_ddl_support.manifest import read_manifest_strict

logger = logging.getLogger(__name__)

TARGET_ENV_MAPS: dict[str, dict[str, str]] = {
    "sql_server": {
        "host": "TARGET_MSSQL_HOST",
        "port": "TARGET_MSSQL_PORT",
        "database": "TARGET_MSSQL_DB",
        "user": "TARGET_MSSQL_USER",
        "password_env": "TARGET_MSSQL_PASSWORD",
    },
    "oracle": {
        "host": "TARGET_ORACLE_HOST",
        "port": "TARGET_ORACLE_PORT",
        "service": "TARGET_ORACLE_SERVICE",
        "user": "TARGET_ORACLE_USER",
        "password_env": "TARGET_ORACLE_PASSWORD",
    },
}


def write_target_runtime_from_env(
    project_root: Path,
    technology: str,
    source_schema: str = "bronze",
) -> RuntimeRole:
    """Read TARGET_* env vars and write runtime.target to manifest.json."""
    if technology not in TARGET_ENV_MAPS:
        raise ValueError(
            f"Unknown target technology '{technology}'. "
            f"Supported: {list(TARGET_ENV_MAPS)}"
        )

    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"manifest.json not found at {manifest_path}. Run setup-source first.")

    env_map = TARGET_ENV_MAPS[technology]
    connection_kwargs: dict[str, str] = {}
    for field, env_var in env_map.items():
        if field == "password_env":
            connection_kwargs["password_env"] = env_var
        else:
            value = os.environ.get(env_var, "")
            if value:
                connection_kwargs[field] = value

    role = RuntimeRole(
        technology=technology,
        dialect=dialect_for_technology(technology),
        connection=RuntimeConnection(**connection_kwargs),
        schemas=RuntimeSchemas(source=source_schema, marts=None),
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if "runtime" not in manifest or not isinstance(manifest["runtime"], dict):
        manifest["runtime"] = {}
    manifest["runtime"]["target"] = role.model_dump(mode="json", exclude_none=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    logger.info(
        "event=write_target_runtime status=success component=target_setup technology=%s source_schema=%s",
        technology,
        source_schema,
    )

    return role


def require_target_role(project_root: Path) -> RuntimeRole:
    manifest = read_manifest_strict(project_root)
    target_role = get_runtime_role(manifest, "target")
    if target_role is None:
        raise ValueError("manifest.json is missing runtime.target. Run /setup-target first.")
    return target_role


def require_source_role(project_root: Path) -> RuntimeRole:
    manifest = read_manifest_strict(project_root)
    source_role = get_runtime_role(manifest, "source")
    if source_role is None:
        raise ValueError("manifest.json is missing runtime.source. Run /setup-source first.")
    return source_role


def get_target_source_schema(project_root: Path, default: str = "bronze") -> str:
    """Return the configured target source schema, defaulting to bronze."""
    target_role = require_target_role(project_root)
    if target_role.schemas is None or not target_role.schemas.source:
        return default
    return target_role.schemas.source


def _catalog_has_completed_generate(path: Path) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return False
    generate = payload.get("generate")
    return isinstance(generate, dict) and generate.get("status") == "ok"


def ensure_setup_target_can_rerun(project_root: Path) -> None:
    """Fail fast when downstream generated models already exist."""
    completed: list[str] = []
    catalog_dir = project_root / "catalog"
    for bucket in ("tables", "views"):
        bucket_dir = catalog_dir / bucket
        if not bucket_dir.is_dir():
            continue
        for catalog_path in sorted(bucket_dir.glob("*.json")):
            if _catalog_has_completed_generate(catalog_path):
                completed.append(catalog_path.stem)

    if not completed:
        logger.info("event=setup_target_rerun_guard status=ok generated_models=0")
        return

    preview = ", ".join(completed[:5])
    suffix = "" if len(completed) <= 5 else f", and {len(completed) - 5} more"
    logger.error(
        "event=setup_target_rerun_guard status=blocked generated_models=%d",
        len(completed),
    )
    raise ValueError(
        "setup-target cannot rerun after downstream dbt models have been generated. "
        "Run `ad-migration reset all --preserve-catalog`, then run "
        f"`ad-migration setup-target` again. Existing generated objects: {preview}{suffix}"
    )
