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
    _require_env,
    build_sql_server_connection_string,
    sql_server_is_available,
)

pytestmark = pytest.mark.integration

SQL_SERVER_FIXTURE_BRONZE_PRODUCT = "bronze_product"
SQL_SERVER_FIXTURE_SILVER_DIMPRODUCT = "silver_dimproduct"
SQL_SERVER_FIXTURE_SILVER_DIMPROMOTION = "silver_dimpromotion"
SQL_SERVER_FIXTURE_SILVER_FACTINTERNETSALES = "silver_factinternetsales"
SQL_SERVER_FIXTURE_SILVER_DIMSALESTERRITORY = "silver_dimsalesterritory"
SQL_SERVER_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC = "silver_usp_load_dimcurrency"
SQL_SERVER_FIXTURE_SILVER_LOAD_DIMPRODUCT_PROC = "silver_usp_load_dimproduct"
SQL_SERVER_FIXTURE_SILVER_LOAD_DIMPROMOTION_PROC = "silver_usp_load_dimpromotion"
SQL_SERVER_FIXTURE_SILVER_PROMOTION_VIEW = "silver_vw_dimpromotion"
SQL_SERVER_FIXTURE_SILVER_TERRITORY_VIEW = "silver_vdimsalesterritory"

REQUIRED_FIXTURE_TABLES = (
    SQL_SERVER_FIXTURE_BRONZE_CURRENCY,
    SQL_SERVER_FIXTURE_BRONZE_PRODUCT,
    SQL_SERVER_FIXTURE_SILVER_CONFIG,
    SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY,
    SQL_SERVER_FIXTURE_SILVER_DIMPRODUCT,
    SQL_SERVER_FIXTURE_SILVER_DIMPROMOTION,
    SQL_SERVER_FIXTURE_SILVER_FACTINTERNETSALES,
    SQL_SERVER_FIXTURE_SILVER_DIMSALESTERRITORY,
)
REQUIRED_FIXTURE_PROCEDURES = (
    SQL_SERVER_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC,
    SQL_SERVER_FIXTURE_SILVER_LOAD_DIMPRODUCT_PROC,
    SQL_SERVER_FIXTURE_SILVER_LOAD_DIMPROMOTION_PROC,
    SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC,
)
REQUIRED_FIXTURE_VIEWS = (
    SQL_SERVER_FIXTURE_SILVER_PROMOTION_VIEW,
    SQL_SERVER_FIXTURE_SILVER_TERRITORY_VIEW,
)

STALE_FIXTURE_TABLE = "stale_contract_probe"


def _have_mssql_env() -> bool:
    return sql_server_is_available(pyodbc)


def _build_sql_server_fixture_role() -> RuntimeRole:
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=_require_env("MSSQL_HOST"),
            port=_require_env("MSSQL_PORT"),
            database=SQL_SERVER_FIXTURE_DATABASE,
            schema=SQL_SERVER_FIXTURE_SCHEMA,
            user=_require_env("MSSQL_USER"),
            driver=_require_env("MSSQL_DRIVER"),
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


def _view_exists(cursor: pyodbc.Cursor, view_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        (SQL_SERVER_FIXTURE_SCHEMA, view_name),
    )
    return cursor.fetchone()[0] == 1


def _assert_fixture_contract(cursor: pyodbc.Cursor) -> None:
    for table_name in REQUIRED_FIXTURE_TABLES:
        assert _table_exists(cursor, table_name), f"missing required fixture table {table_name}"
    for procedure_name in REQUIRED_FIXTURE_PROCEDURES:
        assert _procedure_exists(
            cursor, procedure_name
        ), f"missing required fixture procedure {procedure_name}"
    for view_name in REQUIRED_FIXTURE_VIEWS:
        assert _view_exists(cursor, view_name), f"missing required fixture view {view_name}"


def test_materialize_migration_test_sql_server_script_rebuilds_instead_of_short_circuiting() -> None:
    script_path = REPO_ROOT / "tests" / "integration" / "sql_server" / "fixtures" / "materialize.sh"
    script_text = script_path.read_text(encoding="utf-8")

    assert "OBJECTS_EXIST_SQL" not in script_text
    assert "leaving it in place" not in script_text


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
        _assert_fixture_contract(cursor)
    finally:
        conn.close()


@pytest.mark.skipif(not _have_mssql_env(), reason="SQL Server fixture env not configured")
def test_materialize_migration_test_sql_server_repairs_downstream_contract_objects() -> None:
    role = _build_sql_server_fixture_role()
    first = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr

    conn = pyodbc.connect(
        build_sql_server_connection_string(database=SQL_SERVER_FIXTURE_DATABASE),
        autocommit=True,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE [{SQL_SERVER_FIXTURE_SCHEMA}].[{STALE_FIXTURE_TABLE}] (id INT NOT NULL)")
        cursor.execute(
            f"DROP VIEW [{SQL_SERVER_FIXTURE_SCHEMA}].[{SQL_SERVER_FIXTURE_SILVER_TERRITORY_VIEW}]"
        )
        cursor.execute(
            f"DROP PROCEDURE [{SQL_SERVER_FIXTURE_SCHEMA}].[{SQL_SERVER_FIXTURE_SILVER_LOAD_DIMPROMOTION_PROC}]"
        )
        cursor.execute(
            f"DROP TABLE [{SQL_SERVER_FIXTURE_SCHEMA}].[{SQL_SERVER_FIXTURE_SILVER_FACTINTERNETSALES}]"
        )
        cursor.execute(
            f"DROP TABLE [{SQL_SERVER_FIXTURE_SCHEMA}].[{SQL_SERVER_FIXTURE_SILVER_DIMSALESTERRITORY}]"
        )
        assert not _view_exists(cursor, SQL_SERVER_FIXTURE_SILVER_TERRITORY_VIEW)
        assert not _procedure_exists(cursor, SQL_SERVER_FIXTURE_SILVER_LOAD_DIMPROMOTION_PROC)
        assert not _table_exists(cursor, SQL_SERVER_FIXTURE_SILVER_FACTINTERNETSALES)
        assert not _table_exists(cursor, SQL_SERVER_FIXTURE_SILVER_DIMSALESTERRITORY)
        assert _table_exists(cursor, STALE_FIXTURE_TABLE)
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
        _assert_fixture_contract(cursor)
        assert not _table_exists(cursor, STALE_FIXTURE_TABLE)
    finally:
        conn.close()
