"""Oracle integration coverage for source-table replication."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest

oracledb = pytest.importorskip(
    "oracledb",
    reason="oracledb not installed - skipping Oracle replication integration tests",
)

from shared.replicate_source_tables import MAX_REPLICATE_LIMIT, run_replicate_source_tables
from tests.integration.runtime_helpers import (
    ORACLE_SOURCE_ENV,
    build_oracle_dsn,
    oracle_is_available,
)

pytestmark = [pytest.mark.integration, pytest.mark.oracle]

ORACLE_TARGET_ENV = (
    "TARGET_ORACLE_HOST",
    "TARGET_ORACLE_PORT",
    "TARGET_ORACLE_SERVICE",
    "TARGET_ORACLE_USER",
    "TARGET_ORACLE_PASSWORD",
)
TARGET_SCHEMA = os.environ.get("TARGET_ORACLE_SCHEMA", "BRONZE").upper()


def _require_oracle_target_env() -> None:
    missing = [
        name
        for name in (*ORACLE_SOURCE_ENV, *ORACLE_TARGET_ENV)
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"Missing Oracle replication env vars: {', '.join(missing)}")
    if not oracle_is_available(oracledb):
        pytest.skip("Oracle source env not reachable")
    try:
        with _target_connection():
            pass
    except oracledb.Error as exc:
        pytest.skip(f"Oracle target env not reachable: {exc}")


def _source_connection():
    return oracledb.connect(
        user=os.environ["SOURCE_ORACLE_USER"],
        password=os.environ["SOURCE_ORACLE_PASSWORD"],
        dsn=build_oracle_dsn(),
    )


def _target_connection():
    user = os.environ["TARGET_ORACLE_USER"]
    mode = (
        oracledb.AUTH_MODE_SYSDBA
        if user.lower() == "sys"
        else oracledb.AUTH_MODE_DEFAULT
    )
    return oracledb.connect(
        user=user,
        password=os.environ["TARGET_ORACLE_PASSWORD"],
        dsn=(
            f"{os.environ['TARGET_ORACLE_HOST']}:"
            f"{os.environ['TARGET_ORACLE_PORT']}/"
            f"{os.environ['TARGET_ORACLE_SERVICE']}"
        ),
        mode=mode,
    )


def _drop_table(conn, schema_name: str, table_name: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(f'DROP TABLE "{schema_name}"."{table_name}" PURGE')
    except oracledb.DatabaseError:
        pass


def _ensure_target_schema(schema_name: str) -> None:
    with _target_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM all_users WHERE username = :1", [schema_name])
        if cursor.fetchone()[0] > 0:
            return
        if os.environ["TARGET_ORACLE_USER"].lower() != "sys":
            pytest.skip(f"Oracle target schema {schema_name} does not exist")
        password = f"P{uuid.uuid4().hex[:16]}x"
        cursor.execute(f'CREATE USER "{schema_name}" IDENTIFIED BY "{password}"')
        cursor.execute(f'GRANT CONNECT, RESOURCE TO "{schema_name}"')
        cursor.execute(f'GRANT UNLIMITED TABLESPACE TO "{schema_name}"')
        conn.commit()


def _write_project(project_root: Path, source_schema: str, target_schema: str, table_name: str) -> None:
    manifest = {
        "schema_version": "1.0",
        "technology": "oracle",
        "dialect": "oracle",
        "runtime": {
            "source": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ["SOURCE_ORACLE_HOST"],
                    "port": os.environ["SOURCE_ORACLE_PORT"],
                    "service": os.environ["SOURCE_ORACLE_SERVICE"],
                    "schema": source_schema,
                    "user": os.environ["SOURCE_ORACLE_USER"],
                    "password_env": "SOURCE_ORACLE_PASSWORD",
                },
            },
            "target": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": os.environ["TARGET_ORACLE_HOST"],
                    "port": os.environ["TARGET_ORACLE_PORT"],
                    "service": os.environ["TARGET_ORACLE_SERVICE"],
                    "user": os.environ["TARGET_ORACLE_USER"],
                    "password_env": "TARGET_ORACLE_PASSWORD",
                },
                "schemas": {"source": target_schema},
            },
        },
    }
    (project_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    tables_dir = project_root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / f"{source_schema}.{table_name}.json").write_text(
        json.dumps(
            {
                "schema": source_schema,
                "name": table_name,
                "is_source": True,
                "columns": [
                    {"name": "ID", "sql_type": "NUMBER(10)", "is_nullable": False},
                    {"name": "NAME", "sql_type": "VARCHAR2(64)", "is_nullable": True},
                ],
            }
        ),
        encoding="utf-8",
    )


def _prepare_source_table(schema_name: str, table_name: str) -> None:
    with _source_connection() as conn:
        _drop_table(conn, schema_name, table_name)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f'CREATE TABLE "{schema_name}"."{table_name}" '
                '("ID" NUMBER(10) NOT NULL, "NAME" VARCHAR2(64) NULL)'
            )
        except oracledb.DatabaseError as exc:
            pytest.skip(f"Oracle source user cannot create replication test table: {exc}")
        cursor.executemany(
            f'INSERT INTO "{schema_name}"."{table_name}" ("ID", "NAME") VALUES (:1, :2)',
            [(index, f"name-{index}") for index in range(1, MAX_REPLICATE_LIMIT + 1)],
        )
        conn.commit()


def _prepare_target_table(schema_name: str, table_name: str, *, reject_positive_ids: bool = False) -> None:
    _ensure_target_schema(schema_name)
    with _target_connection() as conn:
        _drop_table(conn, schema_name, table_name)
        cursor = conn.cursor()
        check_constraint = ' CHECK ("ID" < 0)' if reject_positive_ids else ""
        try:
            cursor.execute(
                f'CREATE TABLE "{schema_name}"."{table_name}" '
                f'("ID" NUMBER(10) NOT NULL{check_constraint}, "NAME" VARCHAR2(64) NULL)'
            )
        except oracledb.DatabaseError as exc:
            pytest.skip(f"Oracle target user cannot create replication test table: {exc}")
        cursor.execute(
            f'INSERT INTO "{schema_name}"."{table_name}" ("ID", "NAME") VALUES (:1, :2)',
            [-1, "stale"],
        )
        conn.commit()


def _read_target_summary(schema_name: str, table_name: str) -> tuple[int, int, int, int]:
    with _target_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            f'SELECT COUNT(*), MIN("ID"), MAX("ID"), '
            f'SUM(CASE WHEN "ID" = -1 THEN 1 ELSE 0 END) '
            f'FROM "{schema_name}"."{table_name}"'
        )
        row = cursor.fetchone()
        return int(row[0]), int(row[1]), int(row[2]), int(row[3] or 0)


def test_replicate_source_tables_copies_10k_rows_with_truncate_load(tmp_path: Path) -> None:
    _require_oracle_target_env()
    source_schema = os.environ["SOURCE_ORACLE_USER"].upper()
    target_schema = TARGET_SCHEMA
    table_name = f"RST10K{uuid.uuid4().hex[:8].upper()}"
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
        print(f"oracle_replicate_source_tables_10k_seconds={elapsed_seconds:.3f}")

        assert result.status == "ok"
        assert result.tables[0].rows_copied == MAX_REPLICATE_LIMIT
        assert _read_target_summary(target_schema, table_name) == (
            MAX_REPLICATE_LIMIT,
            1,
            MAX_REPLICATE_LIMIT,
            0,
        )
    finally:
        with _source_connection() as conn:
            _drop_table(conn, source_schema, table_name)
        with _target_connection() as conn:
            _drop_table(conn, target_schema, table_name)


def test_replicate_source_tables_rolls_back_target_replace_failure(tmp_path: Path) -> None:
    _require_oracle_target_env()
    source_schema = os.environ["SOURCE_ORACLE_USER"].upper()
    target_schema = TARGET_SCHEMA
    table_name = f"RSTFAIL{uuid.uuid4().hex[:8].upper()}"
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
        with _source_connection() as conn:
            _drop_table(conn, source_schema, table_name)
        with _target_connection() as conn:
            _drop_table(conn, target_schema, table_name)
