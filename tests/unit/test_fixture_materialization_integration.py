"""Integration coverage for canonical MigrationTest materialization scripts."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

pyodbc = pytest.importorskip("pyodbc", reason="pyodbc not installed — skipping SQL Server materialization integration tests")
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed — skipping Oracle materialization integration tests")
duckdb = pytest.importorskip("duckdb", reason="duckdb not installed — skipping DuckDB materialization tests")

REPO_ROOT = Path(__file__).resolve().parents[2]


def _have_mssql_env() -> bool:
    return all(os.environ.get(name) for name in ("MSSQL_HOST", "MSSQL_DB", "SA_PASSWORD"))


def _have_oracle_env() -> bool:
    return bool(os.environ.get("ORACLE_PWD"))


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.oracle
@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_creates_core_objects() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", "SH")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host=os.environ.get("ORACLE_HOST", "localhost"),
            port=os.environ.get("ORACLE_PORT", "1521"),
            service=os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
            user=os.environ.get("ORACLE_ADMIN_USER", "sys"),
            schema=schema,
            password_env="ORACLE_PWD",
        ),
    )
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = oracledb.connect(
        user=os.environ.get("ORACLE_ADMIN_USER", "sys"),
        password=os.environ["ORACLE_PWD"],
        dsn=f"{os.environ.get('ORACLE_HOST', 'localhost')}:{os.environ.get('ORACLE_PORT', '1521')}/{os.environ.get('ORACLE_SERVICE', 'FREEPDB1')}",
        mode=oracledb.AUTH_MODE_SYSDBA if os.environ.get("ORACLE_ADMIN_USER", "sys").lower() == "sys" else oracledb.AUTH_MODE_DEFAULT,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = 'CHANNELS'",
            [schema.upper()],
        )
        assert cursor.fetchone()[0] == 1
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_PROCEDURES WHERE OWNER = :1 AND OBJECT_NAME = 'SUMMARIZE_CHANNEL_SALES'",
            [schema.upper()],
        )
        assert cursor.fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.oracle
@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_is_idempotent() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", "SH")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host=os.environ.get("ORACLE_HOST", "localhost"),
            port=os.environ.get("ORACLE_PORT", "1521"),
            service=os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
            user=os.environ.get("ORACLE_ADMIN_USER", "sys"),
            schema=schema,
            password_env="ORACLE_PWD",
        ),
    )
    first = materialize_migration_test(role, REPO_ROOT)
    second = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr


def test_materialize_migration_test_duckdb_creates_core_objects(tmp_path: Path) -> None:
    db_path = tmp_path / ".runtime" / "duckdb" / "migrationtest.duckdb"
    role = RuntimeRole(
        technology="duckdb",
        dialect="duckdb",
        connection=RuntimeConnection(path=str(db_path)),
    )
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = duckdb.connect(str(db_path))
    try:
        assert conn.execute("select count(*) from migrationtest_fixture_info").fetchone()[0] == 1
        assert conn.execute("select count(*) from bronze.Currency").fetchone()[0] > 0
        assert conn.execute("select count(*) from information_schema.tables where table_schema = 'silver' and table_name = 'DimProduct'").fetchone()[0] == 1
    finally:
        conn.close()
