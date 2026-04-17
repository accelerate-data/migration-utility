"""Oracle integration coverage for target setup source and seed materialization."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

oracledb = pytest.importorskip(
    "oracledb",
    reason="oracledb not installed - skipping Oracle target setup integration tests",
)

from shared.target_setup import run_setup_target
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    ensure_oracle_migration_test_materialized,
    oracle_is_available,
)

pytestmark = pytest.mark.oracle

# Target schema where tables are created — matches the bronze/source layer.
_TARGET_SCHEMA = os.environ.get("TARGET_ORACLE_SCHEMA", "BRONZE")


def _require_oracle_target_env() -> None:
    if not oracle_is_available(oracledb):
        pytest.skip("Oracle test database not reachable")
    missing = [
        name
        for name in (
            "TARGET_ORACLE_HOST",
            "TARGET_ORACLE_PASSWORD",
        )
        if not os.environ.get(name)
    ]
    if missing:
        pytest.skip(f"Missing Oracle target setup env vars: {', '.join(missing)}")
    if shutil.which("dbt") is None:
        pytest.skip("dbt executable not found on PATH")


def _target_connection():
    host = os.environ.get("TARGET_ORACLE_HOST", os.environ.get("ORACLE_HOST", "localhost"))
    port = os.environ.get("TARGET_ORACLE_PORT", os.environ.get("ORACLE_PORT", "1521"))
    service = os.environ.get("TARGET_ORACLE_SERVICE", os.environ.get("ORACLE_SERVICE", "FREEPDB1"))
    user = os.environ.get("TARGET_ORACLE_USER", os.environ.get("ORACLE_ADMIN_USER", "sys"))
    password = os.environ["TARGET_ORACLE_PASSWORD"]
    mode = (
        oracledb.AUTH_MODE_SYSDBA
        if user.lower() == "sys"
        else oracledb.AUTH_MODE_DEFAULT
    )
    return oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:{port}/{service}",
        mode=mode,
    )


def _ensure_target_schema() -> str:
    """Ensure the target schema (user) exists in Oracle."""
    schema = _TARGET_SCHEMA.upper()
    with _target_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM all_users WHERE username = :1",
            [schema],
        )
        if cursor.fetchone()[0] == 0:
            import uuid

            temp_password = f"P{uuid.uuid4().hex[:16]}x"
            cursor.execute(f'CREATE USER "{schema}" IDENTIFIED BY "{temp_password}"')
            cursor.execute(f'GRANT CONNECT, RESOURCE TO "{schema}"')
            cursor.execute(f'GRANT UNLIMITED TABLESPACE TO "{schema}"')
    return schema


def _cleanup_target_tables(schema: str) -> None:
    with _target_connection() as conn:
        cursor = conn.cursor()
        for table_name in ("SILVER_CONFIG", "BRONZE_CURRENCY"):
            try:
                cursor.execute(f'DROP TABLE "{schema}"."{table_name}" PURGE')
            except oracledb.DatabaseError:
                pass


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2",
        [schema.upper(), table.upper()],
    )
    return cursor.fetchone()[0] == 1


def _write_manifest(project_root: Path, target_schema: str) -> None:
    host = os.environ.get("TARGET_ORACLE_HOST", os.environ.get("ORACLE_HOST", "localhost"))
    port = os.environ.get("TARGET_ORACLE_PORT", os.environ.get("ORACLE_PORT", "1521"))
    service = os.environ.get("TARGET_ORACLE_SERVICE", os.environ.get("ORACLE_SERVICE", "FREEPDB1"))
    user = os.environ.get("TARGET_ORACLE_USER", os.environ.get("ORACLE_ADMIN_USER", "sys"))

    source_host = os.environ.get("ORACLE_HOST", "localhost")
    source_port = os.environ.get("ORACLE_PORT", "1521")
    source_service = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
    source_user = os.environ.get("ORACLE_SOURCE_USER", ORACLE_MIGRATION_SCHEMA)

    manifest = {
        "schema_version": "1.0",
        "technology": "oracle",
        "dialect": "oracle",
        "runtime": {
            "source": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": source_host,
                    "port": source_port,
                    "service": source_service,
                    "user": source_user,
                    "schema": ORACLE_MIGRATION_SCHEMA,
                    "password_env": os.environ.get(
                        "ORACLE_SOURCE_PASSWORD_ENV",
                        "ORACLE_SCHEMA_PASSWORD",
                    ),
                },
            },
            "target": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {
                    "host": host,
                    "port": port,
                    "service": service,
                    "user": user,
                    "password_env": "TARGET_ORACLE_PASSWORD",
                },
                "schemas": {"source": target_schema},
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
                "schema": ORACLE_MIGRATION_SCHEMA.lower(),
                "name": "SILVER_CONFIG",
                "scoping": {"status": "no_writer_found"},
                "is_source": True,
                "columns": [
                    {"name": "CONFIGID", "sql_type": "NUMBER(10)", "is_nullable": False},
                    {"name": "CONFIGKEY", "sql_type": "VARCHAR2(100)", "is_nullable": False},
                    {"name": "CONFIGVALUE", "sql_type": "VARCHAR2(255)", "is_nullable": True},
                ],
            }
        ),
        encoding="utf-8",
    )
    (tables_dir / "migrationtest.bronze_currency.json").write_text(
        json.dumps(
            {
                "schema": ORACLE_MIGRATION_SCHEMA.lower(),
                "name": "BRONZE_CURRENCY",
                "scoping": {"status": "no_writer_found"},
                "is_source": False,
                "is_seed": True,
                "columns": [
                    {"name": "CURRENCYCODE", "sql_type": "CHAR(3)", "is_nullable": False},
                    {"name": "CURRENCYNAME", "sql_type": "VARCHAR2(50)", "is_nullable": False},
                    {"name": "MODIFIEDDATE", "sql_type": "DATE", "is_nullable": False},
                ],
            }
        ),
        encoding="utf-8",
    )


def test_setup_target_materializes_source_and_seed_tables(tmp_path: Path) -> None:
    _require_oracle_target_env()
    ensure_oracle_migration_test_materialized()
    target_schema = _ensure_target_schema()
    _cleanup_target_tables(target_schema)
    _write_manifest(tmp_path, target_schema)
    _write_catalog(tmp_path)

    try:
        result = run_setup_target(tmp_path)

        assert result.target_source_schema == target_schema
        assert any("SILVER_CONFIG" in t for t in result.created_tables)
        assert any("BRONZE_CURRENCY" in f for f in result.seed_files)
        assert result.dbt_seed_ran is True
        assert any(
            count > 0 for count in result.seed_row_counts.values()
        ), "Expected at least one seed table with rows"

        seed_csv = tmp_path / "dbt" / "seeds" / "bronze_currency.csv"
        assert seed_csv.exists()
        header = seed_csv.read_text(encoding="utf-8").splitlines()[0]
        assert "CURRENCYCODE" in header

        with _target_connection() as conn:
            cursor = conn.cursor()
            assert _table_exists(cursor, target_schema, "SILVER_CONFIG")
            assert _table_exists(cursor, target_schema, "BRONZE_CURRENCY")
            cursor.execute(
                f'SELECT COUNT(*) FROM "{target_schema}"."SILVER_CONFIG"'
            )
            assert cursor.fetchone()[0] == 0
            cursor.execute(
                f'SELECT COUNT(*) FROM "{target_schema}"."BRONZE_CURRENCY"'
            )
            assert cursor.fetchone()[0] > 0
    finally:
        _cleanup_target_tables(target_schema)
