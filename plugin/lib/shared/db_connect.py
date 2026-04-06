"""db_connect.py — Shared database connection factories.

Provides public connection factories for SQL Server (via pyodbc) and Oracle
(via oracledb).  Callers: setup_ddl.py, sqlserver_extract.py, oracle_extract.py.
"""

from __future__ import annotations

import os
from typing import Any


def sql_server_connect(database: str) -> Any:
    """Open a pyodbc connection to SQL Server.

    Reads MSSQL_HOST, MSSQL_PORT, MSSQL_USER, SA_PASSWORD, MSSQL_DRIVER from
    the environment. Raises ValueError if required variables are missing.
    Raises RuntimeError if pyodbc is not installed.
    """
    try:
        import pyodbc  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError(
            "pyodbc is required for SQL Server connectivity. "
            "Install it with: uv pip install pyodbc"
        ) from exc

    host = os.environ.get("MSSQL_HOST", "")
    port = os.environ.get("MSSQL_PORT", "1433")
    user = os.environ.get("MSSQL_USER", "sa")
    password = os.environ.get("SA_PASSWORD", "")
    driver = os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server")

    missing = [name for name, val in [("MSSQL_HOST", host), ("SA_PASSWORD", password)] if not val]
    if missing:
        raise ValueError(f"Required environment variables not set: {missing}")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def oracle_connect() -> Any:
    """Open an oracledb connection to Oracle.

    Reads ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN from the environment.
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

    user = os.environ.get("ORACLE_USER", "")
    password = os.environ.get("ORACLE_PASSWORD", "")
    dsn = os.environ.get("ORACLE_DSN", "")

    missing = [name for name, val in [
        ("ORACLE_USER", user), ("ORACLE_PASSWORD", password), ("ORACLE_DSN", dsn),
    ] if not val]
    if missing:
        raise ValueError(f"Required environment variables not set: {missing}")

    return oracledb.connect(user=user, password=password, dsn=dsn)
