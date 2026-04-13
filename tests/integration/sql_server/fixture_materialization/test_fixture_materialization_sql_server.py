"""SQL Server integration coverage for canonical MigrationTest materialization."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pyodbc = pytest.importorskip(
    "pyodbc",
    reason="pyodbc not installed — skipping SQL Server materialization integration tests",
)

from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from tests.helpers import REPO_ROOT

pytestmark = pytest.mark.integration


def _have_mssql_env() -> bool:
    return all(os.environ.get(name) for name in ("MSSQL_HOST", "MSSQL_DB", "SA_PASSWORD"))


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_creates_core_objects() -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ.get("MSSQL_HOST", "localhost"),
            port=os.environ.get("MSSQL_PORT", "1433"),
            database=os.environ.get("MSSQL_DB", "MigrationTest"),
            user=os.environ.get("MSSQL_USER", "sa"),
            driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            password_env="SA_PASSWORD",
        ),
    )
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = pyodbc.connect(
        (
            f"DRIVER={{{os.environ.get('MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
            f"SERVER={os.environ.get('MSSQL_HOST', 'localhost')},{os.environ.get('MSSQL_PORT', '1433')};"
            f"DATABASE={os.environ.get('MSSQL_DB', 'MigrationTest')};"
            f"UID={os.environ.get('MSSQL_USER', 'sa')};PWD={os.environ['SA_PASSWORD']};"
            "TrustServerCertificate=yes;"
        ),
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
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ.get("MSSQL_HOST", "localhost"),
            port=os.environ.get("MSSQL_PORT", "1433"),
            database=os.environ.get("MSSQL_DB", "MigrationTest"),
            user=os.environ.get("MSSQL_USER", "sa"),
            driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            password_env="SA_PASSWORD",
        ),
    )
    first = materialize_migration_test(role, REPO_ROOT)
    second = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
