from __future__ import annotations

import os
from typing import Any

import pytest
from shared.db_connect import build_sql_server_connection_string as _build_sql_server_connection_string
from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

from tests.helpers import (
    REPO_ROOT,
    SQL_SERVER_FIXTURE_DATABASE,
    SQL_SERVER_FIXTURE_SCHEMA,
)

SQL_SERVER_SOURCE_ENV = (
    "SOURCE_MSSQL_HOST",
    "SOURCE_MSSQL_PORT",
    "SOURCE_MSSQL_DB",
    "SOURCE_MSSQL_SCHEMA",
    "SOURCE_MSSQL_USER",
    "SOURCE_MSSQL_PASSWORD",
)
SQL_SERVER_SANDBOX_ENV = (
    "SANDBOX_MSSQL_HOST",
    "SANDBOX_MSSQL_PORT",
    "SANDBOX_MSSQL_USER",
    "SANDBOX_MSSQL_PASSWORD",
)
ORACLE_SOURCE_ENV = (
    "SOURCE_ORACLE_HOST",
    "SOURCE_ORACLE_PORT",
    "SOURCE_ORACLE_SERVICE",
    "SOURCE_ORACLE_SCHEMA",
    "SOURCE_ORACLE_USER",
    "SOURCE_ORACLE_PASSWORD",
)
ORACLE_SANDBOX_ENV = (
    "SANDBOX_ORACLE_HOST",
    "SANDBOX_ORACLE_PORT",
    "SANDBOX_ORACLE_SERVICE",
    "SANDBOX_ORACLE_USER",
    "SANDBOX_ORACLE_PASSWORD",
)

SQL_SERVER_MIGRATION_DATABASE = os.environ.get("SOURCE_MSSQL_DB", SQL_SERVER_FIXTURE_DATABASE)
SQL_SERVER_MIGRATION_SCHEMA = os.environ.get("SOURCE_MSSQL_SCHEMA", SQL_SERVER_FIXTURE_SCHEMA)
ORACLE_MIGRATION_SCHEMA = os.environ.get("SOURCE_ORACLE_SCHEMA", "MIGRATIONTEST").upper()

_ORACLE_MIGRATION_TEST_READY = False
_SQL_SERVER_MIGRATION_TEST_READY = False


def require_env(role: str, variable_names: tuple[str, ...] | list[str]) -> None:
    missing = [name for name in variable_names if not os.environ.get(name)]
    if missing:
        pytest.skip(f"{role} env missing: {', '.join(missing)}")


def build_sql_server_connection_string(
    *,
    database: str = SQL_SERVER_MIGRATION_DATABASE,
    login_timeout: int | None = None,
) -> str:
    return _build_sql_server_connection_string(
        host=os.environ["SOURCE_MSSQL_HOST"],
        port=os.environ.get("SOURCE_MSSQL_PORT", "1433"),
        database=database,
        user=os.environ["SOURCE_MSSQL_USER"],
        password=os.environ["SOURCE_MSSQL_PASSWORD"],
        driver="FreeTDS",
        login_timeout=login_timeout,
    )


def build_sql_server_admin_connection_string(
    *,
    database: str = "master",
    login_timeout: int | None = None,
) -> str:
    return _build_sql_server_connection_string(
        host=os.environ["SANDBOX_MSSQL_HOST"],
        port=os.environ.get("SANDBOX_MSSQL_PORT", "1433"),
        database=database,
        user=os.environ["SANDBOX_MSSQL_USER"],
        password=os.environ["SANDBOX_MSSQL_PASSWORD"],
        driver="FreeTDS",
        login_timeout=login_timeout,
    )


def build_sql_server_fixture_admin_role() -> RuntimeRole:
    if os.environ["SOURCE_MSSQL_HOST"] != os.environ["SANDBOX_MSSQL_HOST"]:
        raise RuntimeError(
            "SQL Server fixture materialization requires sandbox/admin credentials "
            "that can connect to SOURCE_MSSQL_HOST. Set SOURCE_MSSQL_HOST and "
            "SANDBOX_MSSQL_HOST to the same test instance for local bootstrap."
        )
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ["SOURCE_MSSQL_HOST"],
            port=os.environ.get("SOURCE_MSSQL_PORT", "1433"),
            database=SQL_SERVER_MIGRATION_DATABASE,
            schema=SQL_SERVER_MIGRATION_SCHEMA,
            user=os.environ["SANDBOX_MSSQL_USER"],
            password_env="SANDBOX_MSSQL_PASSWORD",
        ),
    )


def build_sql_server_sandbox_manifest() -> dict[str, object]:
    return {
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ["SOURCE_MSSQL_HOST"],
                    "port": os.environ.get("SOURCE_MSSQL_PORT", "1433"),
                    "database": SQL_SERVER_MIGRATION_DATABASE,
                    "schema": SQL_SERVER_MIGRATION_SCHEMA,
                    "user": os.environ["SOURCE_MSSQL_USER"],
                    "password_env": "SOURCE_MSSQL_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ["SANDBOX_MSSQL_HOST"],
                    "port": os.environ.get("SANDBOX_MSSQL_PORT", "1433"),
                    "user": os.environ["SANDBOX_MSSQL_USER"],
                    "password_env": "SANDBOX_MSSQL_PASSWORD",
                },
            },
        }
    }


def build_oracle_fixture_admin_role() -> RuntimeRole:
    if os.environ["SOURCE_ORACLE_HOST"] != os.environ["SANDBOX_ORACLE_HOST"]:
        raise RuntimeError(
            "Oracle fixture materialization requires sandbox/admin credentials "
            "that can connect to SOURCE_ORACLE_HOST. Set SOURCE_ORACLE_HOST and "
            "SANDBOX_ORACLE_HOST to the same test instance for local bootstrap."
        )
    return RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host=os.environ["SOURCE_ORACLE_HOST"],
            port=os.environ.get("SOURCE_ORACLE_PORT", "1521"),
            service=os.environ["SOURCE_ORACLE_SERVICE"],
            user=os.environ["SANDBOX_ORACLE_USER"],
            schema=ORACLE_MIGRATION_SCHEMA,
            password_env="SANDBOX_ORACLE_PASSWORD",
        ),
    )


def build_oracle_dsn() -> str:
    return (
        f"{os.environ['SOURCE_ORACLE_HOST']}:"
        f"{os.environ.get('SOURCE_ORACLE_PORT', '1521')}/"
        f"{os.environ['SOURCE_ORACLE_SERVICE']}"
    )


def build_oracle_sandbox_dsn() -> str:
    return (
        f"{os.environ['SANDBOX_ORACLE_HOST']}:"
        f"{os.environ.get('SANDBOX_ORACLE_PORT', '1521')}/"
        f"{os.environ['SANDBOX_ORACLE_SERVICE']}"
    )


def build_oracle_sandbox_admin_connect_kwargs(oracledb_module: Any) -> dict[str, object]:
    user = os.environ["SANDBOX_ORACLE_USER"]
    return {
        "user": user,
        "password": os.environ["SANDBOX_ORACLE_PASSWORD"],
        "dsn": build_oracle_sandbox_dsn(),
        "mode": (
            oracledb_module.AUTH_MODE_SYSDBA
            if user.lower() == "sys"
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
                    "host": os.environ["SOURCE_ORACLE_HOST"],
                    "port": os.environ.get("SOURCE_ORACLE_PORT", "1521"),
                    "service": os.environ["SOURCE_ORACLE_SERVICE"],
                    "user": os.environ["SOURCE_ORACLE_USER"],
                    "schema": ORACLE_MIGRATION_SCHEMA,
                    "password_env": "SOURCE_ORACLE_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ["SANDBOX_ORACLE_HOST"],
                    "port": os.environ.get("SANDBOX_ORACLE_PORT", "1521"),
                    "service": os.environ["SANDBOX_ORACLE_SERVICE"],
                    "user": os.environ["SANDBOX_ORACLE_USER"],
                    "password_env": "SANDBOX_ORACLE_PASSWORD",
                },
            },
        }
    }


def sql_server_is_available(pyodbc_module: Any) -> bool:
    if any(not os.environ.get(name) for name in SQL_SERVER_SOURCE_ENV):
        return False
    try:
        conn = pyodbc_module.connect(
            build_sql_server_connection_string(
                database=SQL_SERVER_MIGRATION_DATABASE,
                login_timeout=1,
            ),
            autocommit=True,
        )
        conn.close()
        return True
    except (pyodbc_module.Error, EnvironmentError):
        return False


def sql_server_sandbox_is_available(pyodbc_module: Any) -> bool:
    if any(not os.environ.get(name) for name in SQL_SERVER_SANDBOX_ENV):
        return False
    try:
        conn = pyodbc_module.connect(
            build_sql_server_admin_connection_string(
                database=os.environ.get("SANDBOX_MSSQL_ADMIN_DATABASE", "master"),
                login_timeout=1,
            ),
            autocommit=True,
        )
        conn.close()
        return True
    except (pyodbc_module.Error, EnvironmentError):
        return False


def oracle_is_available(oracledb_module: Any) -> bool:
    if any(not os.environ.get(name) for name in ORACLE_SOURCE_ENV):
        return False
    try:
        conn = oracledb_module.connect(
            user=os.environ["SOURCE_ORACLE_USER"],
            password=os.environ["SOURCE_ORACLE_PASSWORD"],
            dsn=build_oracle_dsn(),
        )
        conn.close()
        return True
    except (oracledb_module.Error, EnvironmentError):
        return False


def oracle_sandbox_is_available(oracledb_module: Any) -> bool:
    if any(not os.environ.get(name) for name in ORACLE_SANDBOX_ENV):
        return False
    try:
        conn = oracledb_module.connect(
            **build_oracle_sandbox_admin_connect_kwargs(oracledb_module)
        )
        conn.close()
        return True
    except (oracledb_module.Error, EnvironmentError):
        return False


def ensure_sql_server_migration_test_materialized() -> None:
    global _SQL_SERVER_MIGRATION_TEST_READY
    if _SQL_SERVER_MIGRATION_TEST_READY:
        return

    require_env("source", SQL_SERVER_SOURCE_ENV)
    require_env("sandbox", SQL_SERVER_SANDBOX_ENV)
    role = build_sql_server_fixture_admin_role()
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

    require_env("source", ORACLE_SOURCE_ENV)
    require_env("sandbox", ORACLE_SANDBOX_ENV)
    pytest.importorskip(
        "oracledb",
        reason="oracledb not installed — required for Oracle materialization",
    )

    role = build_oracle_fixture_admin_role()
    result = materialize_migration_test(role, REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "Oracle MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _ORACLE_MIGRATION_TEST_READY = True


def configure_oracle_extract_env(monkeypatch: pytest.MonkeyPatch) -> None:
    ensure_oracle_migration_test_materialized()
    monkeypatch.setenv("SOURCE_ORACLE_USER", os.environ["SOURCE_ORACLE_USER"])
    monkeypatch.setenv("SOURCE_ORACLE_PASSWORD", os.environ["SOURCE_ORACLE_PASSWORD"])
    monkeypatch.setenv("ORACLE_DSN", build_oracle_dsn())


def require_oracle_extract_env() -> None:
    oracledb = pytest.importorskip(
        "oracledb",
        reason="oracledb not installed - skipping Oracle integration tests",
    )
    for var in ("SOURCE_ORACLE_USER", "SOURCE_ORACLE_PASSWORD", "ORACLE_DSN"):
        if not os.environ.get(var):
            pytest.skip(f"source env missing: {var}")
    try:
        conn = oracledb.connect(
            user=os.environ["SOURCE_ORACLE_USER"],
            password=os.environ["SOURCE_ORACLE_PASSWORD"],
            dsn=os.environ["ORACLE_DSN"],
        )
        conn.close()
    except oracledb.Error as exc:
        pytest.skip(f"Oracle test database not reachable: {exc}")
