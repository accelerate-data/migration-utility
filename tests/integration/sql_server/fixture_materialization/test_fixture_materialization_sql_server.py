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
    SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY,
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
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (SQL_SERVER_FIXTURE_SCHEMA, SQL_SERVER_FIXTURE_BRONZE_CURRENCY),
        )
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (SQL_SERVER_FIXTURE_SCHEMA, SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY),
        )
        assert cursor.fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_is_idempotent() -> None:
    role = _build_sql_server_fixture_role()
    first = materialize_migration_test(role, REPO_ROOT)
    second = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
