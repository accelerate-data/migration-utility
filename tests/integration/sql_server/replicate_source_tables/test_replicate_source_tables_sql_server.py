"""SQL Server integration coverage for source-table replication."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest

pyodbc = pytest.importorskip(
    "pyodbc",
    reason="pyodbc not installed - skipping SQL Server replication integration tests",
)

from shared.db_connect import build_sql_server_connection_string
from shared.replicate_source_tables import MAX_REPLICATE_LIMIT, run_replicate_source_tables
from tests.integration.runtime_helpers import (
    SQL_SERVER_SANDBOX_ENV,
    SQL_SERVER_SOURCE_ENV,
    build_sql_server_admin_connection_string,
    require_env,
    sql_server_is_available,
    sql_server_sandbox_is_available,
)

pytestmark = pytest.mark.integration

TARGET_MSSQL_ENV = (
    "TARGET_MSSQL_HOST",
    "TARGET_MSSQL_PORT",
    "TARGET_MSSQL_DB",
    "TARGET_MSSQL_USER",
    "TARGET_MSSQL_PASSWORD",
)


def _require_sql_server_target_env() -> None:
    require_env("source", SQL_SERVER_SOURCE_ENV)
    require_env("sandbox", SQL_SERVER_SANDBOX_ENV)
    require_env("target", TARGET_MSSQL_ENV)
    if not sql_server_is_available(pyodbc):
        pytest.skip("SQL Server source env not reachable")
    if not sql_server_sandbox_is_available(pyodbc):
        pytest.skip("SQL Server sandbox env not reachable")
    try:
        with _target_connection():
            pass
    except pyodbc.Error as exc:
        pytest.skip(f"SQL Server target env not reachable: {exc}")


def _source_connection():
    return pyodbc.connect(
        build_sql_server_connection_string(
            host=os.environ["SOURCE_MSSQL_HOST"],
            port=os.environ.get("SOURCE_MSSQL_PORT", "1433"),
            database=os.environ["SOURCE_MSSQL_DB"],
            user=os.environ["SOURCE_MSSQL_USER"],
            password=os.environ["SOURCE_MSSQL_PASSWORD"],
            driver="FreeTDS",
        ),
        autocommit=True,
    )


def _source_admin_connection():
    return pyodbc.connect(
        build_sql_server_admin_connection_string(database=os.environ["SOURCE_MSSQL_DB"]),
        autocommit=True,
    )


def _target_connection():
    return pyodbc.connect(
        build_sql_server_connection_string(
            host=os.environ["TARGET_MSSQL_HOST"],
            port=os.environ.get("TARGET_MSSQL_PORT", "1433"),
            database=os.environ["TARGET_MSSQL_DB"],
            user=os.environ["TARGET_MSSQL_USER"],
            password=os.environ["TARGET_MSSQL_PASSWORD"],
            driver="FreeTDS",
        ),
        autocommit=True,
    )


def _ensure_schema(cursor, schema_name: str) -> None:
    cursor.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema_name)
    if cursor.fetchone() is None:
        cursor.execute(f"CREATE SCHEMA [{schema_name}]")


def _ensure_schema_or_skip(cursor, schema_name: str) -> None:
    try:
        _ensure_schema(cursor, schema_name)
    except pyodbc.ProgrammingError as exc:
        pytest.skip(f"SQL Server login cannot create schema {schema_name}: {exc}")


def _quote_identifier(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


def _drop_table_and_schema(conn, schema_name: str, table_name: str) -> None:
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS [{schema_name}].[{table_name}]")
    cursor.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema_name)
    if cursor.fetchone() is not None:
        cursor.execute(f"DROP SCHEMA [{schema_name}]")


def _write_project(project_root: Path, source_schema: str, target_schema: str, table_name: str) -> None:
    manifest = {
        "schema_version": "1.0",
        "technology": "sql_server",
        "dialect": "tsql",
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ["SOURCE_MSSQL_HOST"],
                    "port": os.environ.get("SOURCE_MSSQL_PORT", "1433"),
                    "database": os.environ["SOURCE_MSSQL_DB"],
                    "schema": source_schema,
                    "user": os.environ["SOURCE_MSSQL_USER"],
                    "password_env": "SOURCE_MSSQL_PASSWORD",
                },
            },
            "target": {
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {
                    "host": os.environ["TARGET_MSSQL_HOST"],
                    "port": os.environ.get("TARGET_MSSQL_PORT", "1433"),
                    "database": os.environ["TARGET_MSSQL_DB"],
                    "user": os.environ["TARGET_MSSQL_USER"],
                    "password_env": "TARGET_MSSQL_PASSWORD",
                },
                "schemas": {"source": target_schema},
            },
        },
    }
    (project_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    tables_dir = project_root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / f"{source_schema.lower()}.{table_name.lower()}.json").write_text(
        json.dumps(
            {
                "schema": source_schema,
                "name": table_name,
                "is_source": True,
                "columns": [
                    {"name": "id", "sql_type": "INT", "is_nullable": False},
                    {"name": "name", "sql_type": "NVARCHAR(64)", "is_nullable": True},
                ],
            }
        ),
        encoding="utf-8",
    )


def _prepare_source_table(schema_name: str, table_name: str) -> None:
    with _source_admin_connection() as conn:
        cursor = conn.cursor()
        _drop_table_and_schema(conn, schema_name, table_name)
        _ensure_schema(cursor, schema_name)
        cursor.execute(
            f"CREATE TABLE [{schema_name}].[{table_name}] "
            "([id] INT NOT NULL, [name] NVARCHAR(64) NULL)"
        )
        cursor.fast_executemany = True
        cursor.executemany(
            f"INSERT INTO [{schema_name}].[{table_name}] ([id], [name]) VALUES (?, ?)",
            [(index, f"name-{index}") for index in range(1, MAX_REPLICATE_LIMIT + 1)],
        )
        cursor.execute(
            f"GRANT SELECT ON OBJECT::[{schema_name}].[{table_name}] "
            f"TO {_quote_identifier(os.environ['SOURCE_MSSQL_USER'])}"
        )


def _prepare_target_table(schema_name: str, table_name: str, *, reject_positive_ids: bool = False) -> None:
    with _target_connection() as conn:
        cursor = conn.cursor()
        _drop_table_and_schema(conn, schema_name, table_name)
        _ensure_schema_or_skip(cursor, schema_name)
        check_constraint = " CHECK ([id] < 0)" if reject_positive_ids else ""
        cursor.execute(
            f"CREATE TABLE [{schema_name}].[{table_name}] "
            f"([id] INT NOT NULL{check_constraint}, [name] NVARCHAR(64) NULL)"
        )
        cursor.execute(
            f"INSERT INTO [{schema_name}].[{table_name}] ([id], [name]) VALUES (?, ?)",
            -1,
            "stale",
        )


def _read_target_summary(schema_name: str, table_name: str) -> tuple[int, int, int, int]:
    with _target_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*), MIN([id]), MAX([id]), "
            f"SUM(CASE WHEN [id] = -1 THEN 1 ELSE 0 END) "
            f"FROM [{schema_name}].[{table_name}]"
        )
        row = cursor.fetchone()
        return int(row[0]), int(row[1]), int(row[2]), int(row[3] or 0)


def test_replicate_source_tables_copies_10k_rows_with_truncate_load(tmp_path: Path) -> None:
    _require_sql_server_target_env()
    suffix = uuid.uuid4().hex[:8]
    source_schema = f"rst_src_{suffix}"
    target_schema = f"rst_tgt_{suffix}"
    table_name = "Replicate10k"
    _write_project(tmp_path, source_schema, target_schema, table_name)

    try:
        _prepare_source_table(source_schema, table_name)
        _prepare_target_table(target_schema, table_name)

        started_at = time.perf_counter()
        result = run_replicate_source_tables(
            tmp_path,
            limit=MAX_REPLICATE_LIMIT,
            select=[f"{source_schema}.{table_name}"],
        )
        elapsed_seconds = time.perf_counter() - started_at
        print(f"sql_server_replicate_source_tables_10k_seconds={elapsed_seconds:.3f}")

        assert result.status == "ok", result.tables[0].error
        assert result.tables[0].rows_copied == MAX_REPLICATE_LIMIT
        assert _read_target_summary(target_schema, table_name) == (
            MAX_REPLICATE_LIMIT,
            1,
            MAX_REPLICATE_LIMIT,
            0,
        )
    finally:
        with _source_admin_connection() as conn:
            _drop_table_and_schema(conn, source_schema, table_name)
        with _target_connection() as conn:
            _drop_table_and_schema(conn, target_schema, table_name)


def test_replicate_source_tables_rolls_back_target_replace_failure(tmp_path: Path) -> None:
    _require_sql_server_target_env()
    suffix = uuid.uuid4().hex[:8]
    source_schema = f"rst_src_{suffix}"
    target_schema = f"rst_tgt_{suffix}"
    table_name = "ReplicateFail"
    _write_project(tmp_path, source_schema, target_schema, table_name)

    try:
        _prepare_source_table(source_schema, table_name)
        _prepare_target_table(target_schema, table_name, reject_positive_ids=True)

        result = run_replicate_source_tables(
            tmp_path,
            limit=10,
            select=[f"{source_schema}.{table_name}"],
        )

        assert result.status == "error"
        assert result.tables[0].status == "error"
        assert result.tables[0].error
        assert _read_target_summary(target_schema, table_name) == (1, -1, -1, 1)
    finally:
        with _source_admin_connection() as conn:
            _drop_table_and_schema(conn, source_schema, table_name)
        with _target_connection() as conn:
            _drop_table_and_schema(conn, target_schema, table_name)
