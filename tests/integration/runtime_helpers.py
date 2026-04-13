from __future__ import annotations

import os
import shutil
from typing import Any

import pytest
from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

from tests.helpers import (
    REPO_ROOT,
    SQL_SERVER_FIXTURE_DATABASE,
    SQL_SERVER_FIXTURE_SCHEMA,
)

ORACLE_MIGRATION_SCHEMA = os.environ.get("ORACLE_SCHEMA", "MIGRATIONTEST").upper()
ORACLE_MIGRATION_SCHEMA_PASSWORD = os.environ.get(
    "ORACLE_SCHEMA_PASSWORD",
    ORACLE_MIGRATION_SCHEMA.lower(),
)

_ORACLE_MIGRATION_TEST_READY = False
_SQL_SERVER_MIGRATION_TEST_READY = False
SQL_SERVER_MIGRATION_DATABASE = SQL_SERVER_FIXTURE_DATABASE
SQL_SERVER_MIGRATION_SCHEMA = SQL_SERVER_FIXTURE_SCHEMA


def find_oracle_cli() -> str | None:
    configured = os.environ.get("SQLCL_BIN")
    if configured:
        return configured
    for candidate in ("sql", "sqlplus"):
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None
def build_sql_server_connection_string(
    *,
    database: str = SQL_SERVER_MIGRATION_DATABASE,
    login_timeout: int | None = None,
) -> str:
    parts = [
        f"DRIVER={{{os.environ.get('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}}",
        f"SERVER={os.environ.get('MSSQL_HOST', 'localhost')},{os.environ.get('MSSQL_PORT', '1433')}",
        f"DATABASE={database}",
        f"UID={os.environ.get('MSSQL_USER', 'sa')}",
        f"PWD={os.environ.get('SA_PASSWORD', '')}",
        "TrustServerCertificate=yes",
    ]
    if login_timeout is not None:
        parts.append(f"LoginTimeout={login_timeout}")
    return ";".join(parts) + ";"


def build_sql_server_source_role() -> RuntimeRole:
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ.get("MSSQL_HOST", "localhost"),
            port=os.environ.get("MSSQL_PORT", "1433"),
            database=SQL_SERVER_MIGRATION_DATABASE,
            schema=SQL_SERVER_MIGRATION_SCHEMA,
            user=os.environ.get("MSSQL_USER", "sa"),
            driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            password_env="SA_PASSWORD",
        ),
    )


def build_sql_server_sandbox_manifest() -> dict[str, object]:
    return {
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("MSSQL_HOST", "localhost"),
                    "port": os.environ.get("MSSQL_PORT", "1433"),
                    "database": SQL_SERVER_MIGRATION_DATABASE,
                    "schema": SQL_SERVER_MIGRATION_SCHEMA,
                    "user": os.environ.get("MSSQL_USER", "sa"),
                    "driver": os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
                    "password_env": "SA_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("MSSQL_HOST", "localhost"),
                    "port": os.environ.get("MSSQL_PORT", "1433"),
                    "user": os.environ.get("MSSQL_USER", "sa"),
                    "driver": os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
                    "password_env": "SA_PASSWORD",
                },
            },
        }
    }


def build_oracle_admin_role() -> RuntimeRole:
    return RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host=os.environ.get("ORACLE_HOST", "localhost"),
            port=os.environ.get("ORACLE_PORT", "1521"),
            service=os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
            user=os.environ.get("ORACLE_ADMIN_USER", "sys"),
            schema=ORACLE_MIGRATION_SCHEMA,
            password_env="ORACLE_PWD",
        ),
    )


def build_oracle_dsn() -> str:
    return (
        f"{os.environ.get('ORACLE_HOST', 'localhost')}:"
        f"{os.environ.get('ORACLE_PORT', '1521')}/"
        f"{os.environ.get('ORACLE_SERVICE', 'FREEPDB1')}"
    )


def build_oracle_admin_connect_kwargs(oracledb_module: Any) -> dict[str, object]:
    return {
        "user": os.environ.get("ORACLE_ADMIN_USER", "sys"),
        "password": os.environ["ORACLE_PWD"],
        "dsn": build_oracle_dsn(),
        "mode": (
            oracledb_module.AUTH_MODE_SYSDBA
            if os.environ.get("ORACLE_ADMIN_USER", "sys").lower() == "sys"
            else oracledb_module.AUTH_MODE_DEFAULT
        ),
    }


def build_oracle_sandbox_manifest() -> dict[str, object]:
    return {
        "runtime": {
            "source": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ.get("ORACLE_HOST", "localhost"),
                    "port": os.environ.get("ORACLE_PORT", "1521"),
                    "service": os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
                    "user": os.environ.get("ORACLE_SOURCE_USER", ORACLE_MIGRATION_SCHEMA),
                    "schema": os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA),
                    "password_env": os.environ.get(
                        "ORACLE_SOURCE_PASSWORD_ENV",
                        "ORACLE_SCHEMA_PASSWORD",
                    ),
                },
            },
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ.get("ORACLE_HOST", "localhost"),
                    "port": os.environ.get("ORACLE_PORT", "1521"),
                    "service": os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
                    "user": os.environ.get("ORACLE_ADMIN_USER", "sys"),
                    "password_env": "ORACLE_PWD",
                },
            },
        }
    }


def sql_server_is_available(pyodbc_module: Any) -> bool:
    if not all(os.environ.get(name) for name in ("MSSQL_HOST", "SA_PASSWORD")):
        return False
    try:
        conn = pyodbc_module.connect(
            build_sql_server_connection_string(
                database=os.environ.get("MSSQL_ADMIN_DATABASE", "master"),
                login_timeout=1,
            ),
            autocommit=True,
        )
        conn.close()
        return True
    except pyodbc_module.Error:
        return False


def oracle_is_available(oracledb_module: Any) -> bool:
    if not os.environ.get("ORACLE_PWD"):
        return False
    try:
        conn = oracledb_module.connect(**build_oracle_admin_connect_kwargs(oracledb_module))
        conn.close()
        return True
    except oracledb_module.Error:
        return False


def ensure_sql_server_migration_test_materialized() -> None:
    global _SQL_SERVER_MIGRATION_TEST_READY
    if _SQL_SERVER_MIGRATION_TEST_READY:
        return

    role = build_sql_server_source_role()
    result = materialize_migration_test(role, REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "SQL Server MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _SQL_SERVER_MIGRATION_TEST_READY = True


def ensure_oracle_migration_test_materialized() -> None:
    global _ORACLE_MIGRATION_TEST_READY
    if _ORACLE_MIGRATION_TEST_READY:
        return

    if not os.environ.get("ORACLE_PWD"):
        pytest.skip("ORACLE_PWD not set")
    if find_oracle_cli() is None:
        pytest.importorskip(
            "oracledb",
            reason="no Oracle CLI (SQLCL/sql or sqlplus) is installed and python package 'oracledb' is unavailable for Oracle materialization",
        )

    role = build_oracle_admin_role()
    result = materialize_migration_test(
        role,
        REPO_ROOT,
        extra_env={"ORACLE_SCHEMA_PASSWORD": ORACLE_MIGRATION_SCHEMA_PASSWORD},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Oracle MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _ORACLE_MIGRATION_TEST_READY = True


def configure_oracle_extract_env(monkeypatch: pytest.MonkeyPatch) -> None:
    ensure_oracle_migration_test_materialized()
    monkeypatch.setenv("ORACLE_USER", ORACLE_MIGRATION_SCHEMA)
    monkeypatch.setenv("ORACLE_PASSWORD", ORACLE_MIGRATION_SCHEMA_PASSWORD)
    monkeypatch.setenv("ORACLE_DSN", build_oracle_dsn())


def require_oracle_extract_env() -> None:
    oracledb = pytest.importorskip(
        "oracledb",
        reason="oracledb not installed - skipping Oracle integration tests",
    )
    for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
        if not os.environ.get(var):
            pytest.skip(f"{var} not set")
    try:
        conn = oracledb.connect(
            user=os.environ["ORACLE_USER"],
            password=os.environ["ORACLE_PASSWORD"],
            dsn=os.environ["ORACLE_DSN"],
        )
        conn.close()
    except oracledb.Error as exc:
        pytest.skip(f"Oracle test database not reachable: {exc}")
