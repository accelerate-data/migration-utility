"""db_connect.py — Shared database connection factories.

Provides public connection factories for SQL Server (via pyodbc) and Oracle
(via oracledb).  Callers: setup_ddl.py, sqlserver_extract.py, oracle_extract.py.
"""

from __future__ import annotations

import os
from typing import Any


def cursor_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    """Convert a cursor result set to a list of column-keyed dicts.

    Returns an empty list if the cursor has no result set (description is None).
    """
    if cursor.description is None:
        return []
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _escape_odbc_value(value: str) -> str:
    """Escape a value for use in an ODBC connection string."""
    return "{" + value.replace("}", "}}") + "}"


def build_sql_server_connection_string(
    *,
    host: str,
    port: str,
    database: str,
    user: str,
    password: str,
    driver: str,
    login_timeout: int | None = None,
) -> str:
    """Build a SQL Server ODBC connection string with safe value escaping."""
    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={host},{port}",
        f"DATABASE={database}",
        f"UID={user}",
        f"PWD={_escape_odbc_value(password)}",
        "TrustServerCertificate=yes",
    ]
    if login_timeout is not None:
        parts.append(f"LoginTimeout={login_timeout}")
    return ";".join(parts) + ";"


def sql_server_connect(database: str) -> Any:
    """Open a pyodbc connection to SQL Server.

    Reads SOURCE_MSSQL_HOST, SOURCE_MSSQL_PORT, SOURCE_MSSQL_USER,
    SOURCE_MSSQL_PASSWORD, MSSQL_DRIVER from the environment.
    Raises ValueError if required variables are missing.
    Raises RuntimeError if pyodbc is not installed.
    """
    try:
        import pyodbc  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "pyodbc is required for SQL Server connectivity. "
            "Install it with: uv pip install pyodbc"
        ) from exc

    host = os.environ.get("SOURCE_MSSQL_HOST", "")
    port = os.environ.get("SOURCE_MSSQL_PORT", "1433")
    user = os.environ.get("SOURCE_MSSQL_USER", "sa")
    password = os.environ.get("SOURCE_MSSQL_PASSWORD", "")
    driver = os.environ.get("MSSQL_DRIVER", "FreeTDS")

    missing = [name for name, val in [("SOURCE_MSSQL_HOST", host), ("SOURCE_MSSQL_PASSWORD", password)] if not val]
    if missing:
        raise ValueError(f"Required environment variables not set: {missing}")

    conn_str = build_sql_server_connection_string(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        driver=driver,
    )
    try:
        return pyodbc.connect(conn_str, autocommit=True)
    except pyodbc.Error as exc:
        msg = str(exc)
        if "Can't open lib" in msg:
            raise RuntimeError(
                f"ODBC driver '{driver}' not found. "
                "Install FreeTDS: brew install freetds"
            ) from exc
        raise


def oracle_connect() -> Any:
    """Open an oracledb connection to Oracle.

    Reads SOURCE_ORACLE_USER, SOURCE_ORACLE_PASSWORD, ORACLE_DSN from the environment.
    Raises ValueError if required variables are missing.
    Raises RuntimeError if oracledb is not installed.
    """
    try:
        import oracledb  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "oracledb is required for Oracle connectivity. "
            "Install it with: uv pip install oracledb"
        ) from exc

    user = os.environ.get("SOURCE_ORACLE_USER", "")
    password = os.environ.get("SOURCE_ORACLE_PASSWORD", "")
    dsn = os.environ.get("ORACLE_DSN", "")

    missing = [name for name, val in [
        ("SOURCE_ORACLE_USER", user), ("SOURCE_ORACLE_PASSWORD", password), ("ORACLE_DSN", dsn),
    ] if not val]
    if missing:
        raise ValueError(f"Required environment variables not set: {missing}")

    return oracledb.connect(user=user, password=password, dsn=dsn)
