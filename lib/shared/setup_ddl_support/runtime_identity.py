"""Runtime connection identity helpers for setup-ddl."""

from __future__ import annotations

import os
from typing import Any

from shared.runtime_config import TECH_DIALECT, dialect_for_technology, get_runtime_role
from shared.runtime_config_models import RuntimeConnection, RuntimeRole


def get_connection_identity(technology: str, database: str) -> dict[str, Any]:
    if technology == "sql_server":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                host=os.environ.get("SOURCE_MSSQL_HOST", "") or None,
                port=os.environ.get("SOURCE_MSSQL_PORT", "") or None,
                database=database or None,
                user=os.environ.get("SOURCE_MSSQL_USER", "") or None,
                password_env="SOURCE_MSSQL_PASSWORD",
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    if technology == "oracle":
        role = RuntimeRole(
            technology=technology,
            dialect=dialect_for_technology(technology),
            connection=RuntimeConnection(
                dsn=os.environ.get("ORACLE_DSN", "") or None,
                host=os.environ.get("SOURCE_ORACLE_HOST", "") or None,
                port=os.environ.get("SOURCE_ORACLE_PORT", "") or None,
                service=os.environ.get("SOURCE_ORACLE_SERVICE", "") or None,
                user=os.environ.get("SOURCE_ORACLE_USER", "") or None,
                schema=database or None,
                password_env="SOURCE_ORACLE_PASSWORD",
            ),
        )
        return role.model_dump(mode="json", by_alias=True, exclude_none=True)
    raise ValueError(
        f"Unknown technology: {technology}. Must be one of {sorted(TECH_DIALECT)}."
    )


def identity_changed(existing_manifest: dict[str, Any], current_identity: dict[str, Any]) -> bool:
    if not current_identity:
        return False
    existing_source = get_runtime_role(existing_manifest, "source")
    if existing_source is None:
        return False
    identity_fields_by_tech = {
        "sql_server": {"host", "port", "database"},
        "oracle": {"dsn", "host", "port", "service", "schema"},
    }
    try:
        identity_fields = identity_fields_by_tech[existing_source.technology]
    except KeyError as exc:
        raise ValueError(
            f"Unknown technology: {existing_source.technology}. "
            f"Must be one of {sorted(TECH_DIALECT)}."
        ) from exc
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
