"""SQL Server integration coverage for canonical MigrationTest materialization."""

from __future__ import annotations

import os

import pytest

pyodbc = pytest.importorskip(
    "pyodbc",
    reason="pyodbc not installed — skipping SQL Server materialization integration tests",
)

from shared.fixture_materialization import materialize_migration_test
from tests.helpers import REPO_ROOT
from tests.integration.runtime_helpers import (
    SQL_SERVER_MIGRATION_DATABASE,
    build_sql_server_connection_string,
    build_sql_server_source_role,
    sql_server_is_available,
)

pytestmark = pytest.mark.integration


def _have_mssql_env() -> bool:
    return sql_server_is_available(pyodbc)


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_creates_core_objects() -> None:
    role = build_sql_server_source_role()
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = pyodbc.connect(
        build_sql_server_connection_string(database=SQL_SERVER_MIGRATION_DATABASE),
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'Currency'"
        )
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'DimCurrency'"
        )
        assert cursor.fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_is_idempotent() -> None:
    role = build_sql_server_source_role()
    first = materialize_migration_test(role, REPO_ROOT)
    second = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
