"""SQL Server integration coverage for canonical MigrationTest materialization."""

from __future__ import annotations

import os

import pytest

pyodbc = pytest.importorskip(
    "pyodbc",
    reason="pyodbc not installed — skipping SQL Server materialization integration tests",
)

from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from shared.fixture_materialization import materialize_migration_test
from tests.helpers import (
    REPO_ROOT,
    SQL_SERVER_FIXTURE_BRONZE_CURRENCY,
    SQL_SERVER_FIXTURE_DATABASE,
    SQL_SERVER_FIXTURE_SCHEMA,
    SQL_SERVER_FIXTURE_SILVER_CONFIG,
    SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY,
    SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC,
)
from tests.integration.runtime_helpers import (
    build_sql_server_connection_string,
    sql_server_is_available,
)

pytestmark = pytest.mark.integration


def _have_mssql_env() -> bool:
    return sql_server_is_available(pyodbc)


def _build_sql_server_fixture_role() -> RuntimeRole:
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ.get("MSSQL_HOST", "localhost"),
            port=os.environ.get("MSSQL_PORT", "1433"),
            database=SQL_SERVER_FIXTURE_DATABASE,
            schema=SQL_SERVER_FIXTURE_SCHEMA,
            user=os.environ.get("MSSQL_USER", "sa"),
            driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            password_env="SA_PASSWORD",
        ),
    )


def _table_exists(cursor: pyodbc.Cursor, table_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (SQL_SERVER_FIXTURE_SCHEMA, table_name),
    )
    return cursor.fetchone()[0] == 1


def _procedure_exists(cursor: pyodbc.Cursor, procedure_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES "
        "WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ? AND ROUTINE_TYPE = 'PROCEDURE'",
        (SQL_SERVER_FIXTURE_SCHEMA, procedure_name),
    )
    return cursor.fetchone()[0] == 1


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_creates_core_objects() -> None:
    role = _build_sql_server_fixture_role()
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = pyodbc.connect(
        build_sql_server_connection_string(database=SQL_SERVER_FIXTURE_DATABASE),
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        assert _table_exists(cursor, SQL_SERVER_FIXTURE_BRONZE_CURRENCY)
        assert _table_exists(cursor, SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY)
        assert _table_exists(cursor, SQL_SERVER_FIXTURE_SILVER_CONFIG)
        assert _procedure_exists(cursor, SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC)
    finally:
        conn.close()


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_is_idempotent() -> None:
    role = _build_sql_server_fixture_role()
    first = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr

    conn = pyodbc.connect(
        build_sql_server_connection_string(database=SQL_SERVER_FIXTURE_DATABASE),
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"DROP PROCEDURE [{SQL_SERVER_FIXTURE_SCHEMA}].[{SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC}]"
        )
        assert not _procedure_exists(cursor, SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC)
    finally:
        conn.close()

    second = materialize_migration_test(role, REPO_ROOT)
    assert second.returncode == 0, second.stderr

    conn = pyodbc.connect(
        build_sql_server_connection_string(database=SQL_SERVER_FIXTURE_DATABASE),
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        assert _procedure_exists(cursor, SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC)
    finally:
        conn.close()
