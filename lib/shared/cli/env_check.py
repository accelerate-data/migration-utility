"""Env var validation for ad-migration CLI commands.

Validates required env vars before a command runs.
Exits 1 with a clear message listing every missing var.
"""
from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

_SOURCE_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "MSSQL_HOST": "SQL Server hostname",
        "MSSQL_PORT": "SQL Server port",
        "MSSQL_DB": "SQL Server database name",
        "SA_PASSWORD": "SQL Server SA password",
    },
    "oracle": {
        "ORACLE_HOST": "Oracle hostname",
        "ORACLE_PORT": "Oracle port",
        "ORACLE_SERVICE": "Oracle service name",
        "ORACLE_USER": "Oracle username",
        "ORACLE_PASSWORD": "Oracle password",
    },
}

_TARGET_VARS: dict[str, dict[str, str]] = {
    "fabric": {
        "TARGET_WORKSPACE": "Microsoft Fabric workspace name",
        "TARGET_LAKEHOUSE": "Microsoft Fabric lakehouse name",
        "TARGET_CLIENT_ID": "Azure service principal client ID",
        "TARGET_CLIENT_SECRET": "Azure service principal client secret",
        "TARGET_TENANT_ID": "Azure tenant ID",
    },
    "snowflake": {
        "TARGET_ACCOUNT": "Snowflake account identifier",
        "TARGET_DATABASE": "Snowflake target database",
        "TARGET_SCHEMA": "Snowflake target schema",
        "TARGET_WAREHOUSE": "Snowflake virtual warehouse",
        "TARGET_USER": "Snowflake username",
        "TARGET_PASSWORD": "Snowflake password",
    },
    "duckdb": {
        "TARGET_PATH": "DuckDB file path (e.g. /path/to/warehouse.duckdb)",
    },
}


def require_source_vars(technology: str) -> None:
    """Validate source env vars. Exits 1 if any are missing or technology unknown."""
    if technology not in _SOURCE_VARS:
        print(
            f"Error: unknown source technology '{technology}'. Valid: {list(_SOURCE_VARS)}",
            file=sys.stderr,
        )
        logger.error(
            "event=env_check status=failure component=env_check technology=%s reason=unknown_technology",
            technology,
        )
        sys.exit(1)
    _check(_SOURCE_VARS[technology], technology, "setup-source")


def require_target_vars(technology: str) -> None:
    """Validate target env vars. Exits 1 if any are missing or technology unknown."""
    if technology not in _TARGET_VARS:
        print(
            f"Error: unknown target technology '{technology}'. Valid: {list(_TARGET_VARS)}",
            file=sys.stderr,
        )
        logger.error(
            "event=env_check status=failure component=env_check technology=%s reason=unknown_technology",
            technology,
        )
        sys.exit(1)
    _check(_TARGET_VARS[technology], technology, "setup-target")


def _check(required: dict[str, str], technology: str, command: str) -> None:
    missing = [var for var in required if not os.environ.get(var)]
    if not missing:
        logger.debug(
            "event=env_check status=success component=env_check technology=%s command=%s",
            technology,
            command,
        )
        return
    col = max(len(v) for v in missing) + 2
    lines = [f"Error: missing required environment variables for {technology}:\n"]
    for var in missing:
        lines.append(f"  {var:<{col}} not set")
    lines.append(f"\nSet these in your shell or .envrc before running {command}.")
    print("\n".join(lines), file=sys.stderr)
    logger.error(
        "event=env_check status=failure component=env_check technology=%s command=%s missing_count=%d",
        technology,
        command,
        len(missing),
    )
    sys.exit(1)
