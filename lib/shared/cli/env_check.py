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
        "SOURCE_MSSQL_HOST": "SQL Server hostname",
        "SOURCE_MSSQL_PORT": "SQL Server port",
        "SOURCE_MSSQL_DB": "SQL Server database name",
        "SOURCE_MSSQL_USER": "SQL Server username",
        "SOURCE_MSSQL_PASSWORD": "SQL Server password",
    },
    "oracle": {
        "SOURCE_ORACLE_HOST": "Oracle hostname",
        "SOURCE_ORACLE_PORT": "Oracle port",
        "SOURCE_ORACLE_SERVICE": "Oracle service name",
        "SOURCE_ORACLE_USER": "Oracle username",
        "SOURCE_ORACLE_PASSWORD": "Oracle password",
    },
}

_SANDBOX_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "SANDBOX_MSSQL_HOST": "Sandbox SQL Server hostname",
        "SANDBOX_MSSQL_PORT": "Sandbox SQL Server port",
        "SANDBOX_MSSQL_USER": "Sandbox SQL Server username",
        "SANDBOX_MSSQL_PASSWORD": "Sandbox SQL Server password",
    },
    "oracle": {
        "SANDBOX_ORACLE_HOST": "Sandbox Oracle hostname",
        "SANDBOX_ORACLE_PORT": "Sandbox Oracle port",
        "SANDBOX_ORACLE_SERVICE": "Sandbox Oracle service name",
        "SANDBOX_ORACLE_USER": "Sandbox Oracle admin username",
        "SANDBOX_ORACLE_PASSWORD": "Sandbox Oracle admin password",
    },
}

_TARGET_VARS: dict[str, dict[str, str]] = {
    "sql_server": {
        "TARGET_MSSQL_HOST": "Target SQL Server hostname",
        "TARGET_MSSQL_PORT": "Target SQL Server port",
        "TARGET_MSSQL_DB": "Target SQL Server database name",
        "TARGET_MSSQL_USER": "Target SQL Server username",
        "TARGET_MSSQL_PASSWORD": "Target SQL Server password",
    },
    "oracle": {
        "TARGET_ORACLE_HOST": "Target Oracle hostname",
        "TARGET_ORACLE_PORT": "Target Oracle port",
        "TARGET_ORACLE_SERVICE": "Target Oracle service name",
        "TARGET_ORACLE_USER": "Target Oracle username",
        "TARGET_ORACLE_PASSWORD": "Target Oracle password",
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


def require_sandbox_vars(technology: str) -> None:
    """Validate sandbox env vars. Exits 1 if any are missing or technology unknown."""
    if technology not in _SANDBOX_VARS:
        print(
            f"Error: unknown sandbox technology '{technology}'. Valid: {list(_SANDBOX_VARS)}",
            file=sys.stderr,
        )
        logger.error(
            "event=env_check status=failure component=env_check technology=%s reason=unknown_technology",
            technology,
        )
        sys.exit(1)
    _check(_SANDBOX_VARS[technology], technology, "setup-sandbox")


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
