"""Env var validation for ad-migration CLI commands.

Validates required env vars before a command runs.
Exits 1 with a clear message listing every missing var.
"""
from __future__ import annotations

import os
import sys

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
    """Validate source env vars. Exits 1 if any are missing."""
    _check(_SOURCE_VARS.get(technology, {}), technology, "setup-source")


def require_target_vars(technology: str) -> None:
    """Validate target env vars. Exits 1 if any are missing."""
    _check(_TARGET_VARS.get(technology, {}), technology, "setup-target")


def _check(required: dict[str, str], technology: str, command: str) -> None:
    missing = [var for var in required if not os.environ.get(var)]
    if not missing:
        return
    lines = [f"Error: missing required environment variables for {technology}:\n"]
    for var in missing:
        lines.append(f"  {var:<30} not set")
    lines.append(f"\nSet these in your shell or .envrc before running {command}.")
    print("\n".join(lines), file=sys.stderr)
    sys.exit(1)
