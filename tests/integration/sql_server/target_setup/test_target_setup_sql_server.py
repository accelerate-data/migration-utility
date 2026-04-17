"""SQL Server integration coverage for target setup source and seed materialization."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

pyodbc = pytest.importorskip(
    "pyodbc",
    reason="pyodbc not installed - skipping SQL Server target setup integration tests",
)

from shared.db_connect import build_sql_server_connection_string
from shared.target_setup import run_setup_target
from tests.helpers import SQL_SERVER_FIXTURE_DATABASE, SQL_SERVER_FIXTURE_SCHEMA
from tests.integration.runtime_helpers import (
    ensure_sql_server_migration_test_materialized,
    sql_server_is_available,
)

pytestmark = pytest.mark.integration


def _require_sql_server_target_env() -> None:
    if not sql_server_is_available(pyodbc):
        pytest.skip("SQL Server test database not reachable")
    missing = [
        name
        for name in (
            "SOURCE_MSSQL_HOST",
            "SOURCE_MSSQL_PASSWORD",
            "TARGET_MSSQL_HOST",
            "TARGET_MSSQL_PASSWORD",
        )
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"Missing SQL Server target setup env vars: {', '.join(missing)}")
    if shutil.which("dbt") is None:
        pytest.skip("dbt executable not found on PATH")


def _target_connection(database: str | None = None):
    return pyodbc.connect(
        build_sql_server_connection_string(
            host=os.environ.get("TARGET_MSSQL_HOST", os.environ.get("MSSQL_HOST", "localhost")),
            port=os.environ.get("TARGET_MSSQL_PORT", os.environ.get("MSSQL_PORT", "1433")),
            database=database or os.environ.get("TARGET_MSSQL_DB", SQL_SERVER_FIXTURE_DATABASE),
            user=os.environ.get("TARGET_MSSQL_USER", os.environ.get("MSSQL_USER", "sa")),
            password=os.environ["TARGET_MSSQL_PASSWORD"],
            driver=os.environ.get("MSSQL_DRIVER", "FreeTDS"),
        ),
        autocommit=True,
    )


def _ensure_target_database() -> str:
    database = os.environ.get("TARGET_MSSQL_DB", SQL_SERVER_FIXTURE_DATABASE)
    with _target_connection(os.environ.get("MSSQL_ADMIN_DATABASE", "master")) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DB_ID(?)", database)
        if cursor.fetchone()[0] is None:
            cursor.execute(f"CREATE DATABASE [{database}]")
    return database


def _cleanup_target_tables(database: str) -> None:
    with _target_connection(database) as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS [bronze].[bronze_currency]")
        cursor.execute("DROP TABLE IF EXISTS [bronze].[silver_config]")


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        schema,
        table,
    )
    return cursor.fetchone()[0] == 1


def _write_manifest(project_root: Path, target_database: str) -> None:
    manifest = {
        "schema_version": "1.0",
        "technology": "sql_server",
        "dialect": "tsql",
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("SOURCE_MSSQL_HOST", os.environ.get("MSSQL_HOST", "localhost")),
                    "port": os.environ.get("SOURCE_MSSQL_PORT", os.environ.get("MSSQL_PORT", "1433")),
                    "database": os.environ.get("SOURCE_MSSQL_DB", SQL_SERVER_FIXTURE_DATABASE),
                    "schema": SQL_SERVER_FIXTURE_SCHEMA,
                    "user": os.environ.get("SOURCE_MSSQL_USER", os.environ.get("MSSQL_USER", "sa")),
                    "driver": os.environ.get("MSSQL_DRIVER", "FreeTDS"),
                    "password_env": "SOURCE_MSSQL_PASSWORD",
                },
            },
            "target": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ.get("TARGET_MSSQL_HOST", os.environ.get("MSSQL_HOST", "localhost")),
                    "port": os.environ.get("TARGET_MSSQL_PORT", os.environ.get("MSSQL_PORT", "1433")),
                    "database": target_database,
                    "user": os.environ.get("TARGET_MSSQL_USER", os.environ.get("MSSQL_USER", "sa")),
                    "driver": os.environ.get("MSSQL_DRIVER", "FreeTDS"),
                    "password_env": "TARGET_MSSQL_PASSWORD",
                },
                "schemas": {"source": "bronze"},
            },
        },
    }
    (project_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )


def _write_catalog(project_root: Path) -> None:
    tables_dir = project_root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "migrationtest.silver_config.json").write_text(
        json.dumps(
            {
                "schema": SQL_SERVER_FIXTURE_SCHEMA,
                "name": "silver_config",
                "scoping": {"status": "no_writer_found"},
                "is_source": True,
                "columns": [
                    {"name": "ConfigID", "sql_type": "INT", "is_nullable": False},
                    {"name": "ConfigKey", "sql_type": "NVARCHAR(100)", "is_nullable": False},
                    {"name": "ConfigValue", "sql_type": "NVARCHAR(255)", "is_nullable": True},
                ],
            }
        ),
        encoding="utf-8",
    )
    (tables_dir / "migrationtest.bronze_currency.json").write_text(
        json.dumps(
            {
                "schema": SQL_SERVER_FIXTURE_SCHEMA,
                "name": "bronze_currency",
                "scoping": {"status": "no_writer_found"},
                "is_source": False,
                "is_seed": True,
                "columns": [
                    {"name": "CurrencyCode", "sql_type": "NCHAR(3)", "is_nullable": False},
                    {"name": "CurrencyName", "sql_type": "NVARCHAR(50)", "is_nullable": False},
                    {"name": "ModifiedDate", "sql_type": "DATETIME", "is_nullable": False},
                ],
            }
        ),
        encoding="utf-8",
    )


def test_setup_target_materializes_source_and_seed_tables_in_bronze(tmp_path: Path) -> None:
    _require_sql_server_target_env()
    ensure_sql_server_migration_test_materialized()
    target_database = _ensure_target_database()
    _cleanup_target_tables(target_database)
    _write_manifest(tmp_path, target_database)
    _write_catalog(tmp_path)

    try:
        result = run_setup_target(tmp_path)

        assert result.target_source_schema == "bronze"
        assert result.created_tables == ["bronze.silver_config"]
        assert result.seed_files == ["dbt/seeds/bronze_currency.csv"]
        assert result.dbt_seed_ran is True
        assert result.seed_row_counts["migrationtest.bronze_currency"] > 0

        seed_csv = tmp_path / "dbt" / "seeds" / "bronze_currency.csv"
        assert seed_csv.exists()
        assert seed_csv.read_text(encoding="utf-8").splitlines()[0] == (
            "CurrencyCode,CurrencyName,ModifiedDate"
        )

        with _target_connection(target_database) as conn:
            cursor = conn.cursor()
            assert _table_exists(cursor, "bronze", "silver_config")
            assert _table_exists(cursor, "bronze", "bronze_currency")
            cursor.execute("SELECT COUNT(*) FROM [bronze].[silver_config]")
            assert cursor.fetchone()[0] == 0
            cursor.execute("SELECT COUNT(*) FROM [bronze].[bronze_currency]")
            assert cursor.fetchone()[0] == result.seed_row_counts["migrationtest.bronze_currency"]
    finally:
        _cleanup_target_tables(target_database)
