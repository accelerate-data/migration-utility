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


def _require_env(name: str) -> str:
    """Return env var value or raise with a clear message."""
    value = os.environ.get(name, "")
    if not value:
        raise EnvironmentError(
            f"Required environment variable {name} is not set. Check .envrc and .env"
        )
    return value


ORACLE_MIGRATION_SCHEMA = os.environ.get("ORACLE_SCHEMA", "MIGRATIONTEST").upper()
ORACLE_MIGRATION_SCHEMA_PASSWORD = os.environ.get(
    "ORACLE_SCHEMA_PASSWORD",
    ORACLE_MIGRATION_SCHEMA.lower(),
)

_ORACLE_MIGRATION_TEST_READY = False
_SQL_SERVER_MIGRATION_TEST_READY = False
SQL_SERVER_MIGRATION_DATABASE = SQL_SERVER_FIXTURE_DATABASE
SQL_SERVER_MIGRATION_SCHEMA = SQL_SERVER_FIXTURE_SCHEMA


def build_sql_server_connection_string(
    *,
    database: str = SQL_SERVER_MIGRATION_DATABASE,
    login_timeout: int | None = None,
) -> str:
    return _build_sql_server_connection_string(
        host=_require_env("MSSQL_HOST"),
        port=_require_env("MSSQL_PORT"),
        database=database,
        user=_require_env("MSSQL_USER"),
        password=_require_env("SA_PASSWORD"),
        driver=_require_env("MSSQL_DRIVER"),
        login_timeout=login_timeout,
    )


def build_sql_server_source_role() -> RuntimeRole:
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=_require_env("MSSQL_HOST"),
            port=_require_env("MSSQL_PORT"),
            database=SQL_SERVER_MIGRATION_DATABASE,
            schema=SQL_SERVER_MIGRATION_SCHEMA,
            user=_require_env("MSSQL_USER"),
            driver=_require_env("MSSQL_DRIVER"),
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
                    "host": _require_env("MSSQL_HOST"),
                    "port": _require_env("MSSQL_PORT"),
                    "database": SQL_SERVER_MIGRATION_DATABASE,
                    "schema": SQL_SERVER_MIGRATION_SCHEMA,
                    "user": _require_env("MSSQL_USER"),
                    "driver": _require_env("MSSQL_DRIVER"),
                    "password_env": "SA_PASSWORD",
                },
            },
            "sandbox": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": _require_env("MSSQL_HOST"),
                    "port": _require_env("MSSQL_PORT"),
                    "user": _require_env("MSSQL_USER"),
                    "driver": _require_env("MSSQL_DRIVER"),
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
            host=_require_env("ORACLE_HOST"),
            port=_require_env("ORACLE_PORT"),
            service=_require_env("ORACLE_SERVICE"),
            user=_require_env("ORACLE_ADMIN_USER"),
            schema=ORACLE_MIGRATION_SCHEMA,
            password_env="ORACLE_PWD",
        ),
    )


def build_oracle_dsn() -> str:
    return (
        f"{_require_env('ORACLE_HOST')}:"
        f"{_require_env('ORACLE_PORT')}/"
        f"{_require_env('ORACLE_SERVICE')}"
    )


def build_oracle_admin_connect_kwargs(oracledb_module: Any) -> dict[str, object]:
    admin_user = _require_env("ORACLE_ADMIN_USER")
    return {
        "user": admin_user,
        "password": os.environ["ORACLE_PWD"],
        "dsn": build_oracle_dsn(),
        "mode": (
            oracledb_module.AUTH_MODE_SYSDBA
            if admin_user.lower() == "sys"
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
                    "host": _require_env("ORACLE_HOST"),
                    "port": _require_env("ORACLE_PORT"),
                    "service": _require_env("ORACLE_SERVICE"),
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
                    "host": _require_env("ORACLE_HOST"),
                    "port": _require_env("ORACLE_PORT"),
                    "service": _require_env("SANDBOX_ORACLE_SERVICE"),
                    "user": _require_env("ORACLE_ADMIN_USER"),
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
    except (pyodbc_module.Error, EnvironmentError):
        return False


def oracle_is_available(oracledb_module: Any) -> bool:
    if not os.environ.get("ORACLE_PWD"):
        return False
    try:
        conn = oracledb_module.connect(**build_oracle_admin_connect_kwargs(oracledb_module))
        conn.close()
        return True
    except (oracledb_module.Error, EnvironmentError):
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
    pytest.importorskip(
        "oracledb",
        reason="oracledb not installed — required for Oracle materialization",
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
