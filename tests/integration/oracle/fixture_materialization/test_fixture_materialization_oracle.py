"""Oracle integration coverage for canonical MigrationTest materialization."""

from __future__ import annotations

import os

import pytest

oracledb = pytest.importorskip(
    "oracledb",
    reason="oracledb not installed — skipping Oracle materialization integration tests",
)

from shared.fixture_materialization import materialize_migration_test
from tests.helpers import REPO_ROOT
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    build_oracle_admin_connect_kwargs,
    build_oracle_admin_role,
    oracle_is_available,
)

pytestmark = pytest.mark.oracle

MIGRATION_FIXTURE_SCHEMA = "MIGRATIONTEST"
MIGRATION_FIXTURE_BRONZE_CURRENCY = "BRONZE_CURRENCY"
MIGRATION_FIXTURE_BRONZE_PROMOTION = "BRONZE_PROMOTION"
MIGRATION_FIXTURE_SILVER_CONFIG = "SILVER_CONFIG"
MIGRATION_FIXTURE_SILVER_DIMCURRENCY = "SILVER_DIMCURRENCY"
MIGRATION_FIXTURE_SILVER_DIMPROMOTION = "SILVER_DIMPROMOTION"
MIGRATION_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC = "SILVER_USP_LOAD_DIMCURRENCY"
MIGRATION_FIXTURE_SILVER_PATTERN_PROC = "SILVER_USP_UNIONALL"
LEGACY_FIXTURE_TABLE = "CHANNELS"
LEGACY_FIXTURE_PROCEDURE = "SALES"


def _have_oracle_env() -> bool:
    return oracle_is_available(oracledb)


def _table_exists(cursor: oracledb.Cursor, schema: str, table_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2",
        [schema, table_name],
    )
    return cursor.fetchone()[0] == 1


def _procedure_exists(cursor: oracledb.Cursor, schema: str, procedure_name: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM ALL_PROCEDURES "
        "WHERE OWNER = :1 AND OBJECT_NAME = :2 AND OBJECT_TYPE = 'PROCEDURE'",
        [schema, procedure_name],
    )
    return cursor.fetchone()[0] == 1


def _table_names(cursor: oracledb.Cursor, schema: str) -> set[str]:
    cursor.execute(
        "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :1",
        [schema],
    )
    return {row[0] for row in cursor.fetchall()}


def _procedure_names(cursor: oracledb.Cursor, schema: str) -> set[str]:
    cursor.execute(
        "SELECT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = :1 AND OBJECT_TYPE = 'PROCEDURE'",
        [schema],
    )
    return {row[0] for row in cursor.fetchall()}


@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_creates_core_objects() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA).upper()
    assert schema == MIGRATION_FIXTURE_SCHEMA
    role = build_oracle_admin_role()
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = oracledb.connect(**build_oracle_admin_connect_kwargs(oracledb))
    try:
        cursor = conn.cursor()
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_BRONZE_CURRENCY)
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_BRONZE_PROMOTION)
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_DIMCURRENCY)
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_DIMPROMOTION)
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_CONFIG)
        assert _procedure_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC)
        assert _procedure_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_PATTERN_PROC)
        assert not _table_exists(cursor, schema, LEGACY_FIXTURE_TABLE)
        assert not _procedure_exists(cursor, schema, LEGACY_FIXTURE_PROCEDURE)
    finally:
        conn.close()


@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_is_idempotent() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA).upper()
    role = build_oracle_admin_role()
    first = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr

    conn = oracledb.connect(**build_oracle_admin_connect_kwargs(oracledb))
    try:
        cursor = conn.cursor()
        cursor.execute(f'DROP PROCEDURE "{schema}"."{MIGRATION_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC}"')
        assert not _procedure_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC)
    finally:
        conn.close()

    second = materialize_migration_test(role, REPO_ROOT)
    assert second.returncode == 0, second.stderr

    conn = oracledb.connect(**build_oracle_admin_connect_kwargs(oracledb))
    try:
        cursor = conn.cursor()
        assert _procedure_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_LOAD_DIMCURRENCY_PROC)
    finally:
        conn.close()


@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_removes_legacy_objects() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA).upper()
    role = build_oracle_admin_role()

    first = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr

    conn = oracledb.connect(**build_oracle_admin_connect_kwargs(oracledb))
    try:
        cursor = conn.cursor()
        cursor.execute(
            f'CREATE TABLE "{schema}"."{LEGACY_FIXTURE_TABLE}" ("CHANNEL_ID" NUMBER PRIMARY KEY)'
        )
        cursor.execute(
            f'CREATE OR REPLACE PROCEDURE "{schema}"."{LEGACY_FIXTURE_PROCEDURE}" AS '
            "BEGIN NULL; END;"
        )
        conn.commit()
        assert _table_exists(cursor, schema, LEGACY_FIXTURE_TABLE)
        assert _procedure_exists(cursor, schema, LEGACY_FIXTURE_PROCEDURE)
    finally:
        conn.close()

    second = materialize_migration_test(role, REPO_ROOT)
    assert second.returncode == 0, second.stderr

    conn = oracledb.connect(**build_oracle_admin_connect_kwargs(oracledb))
    try:
        cursor = conn.cursor()
        assert LEGACY_FIXTURE_TABLE not in _table_names(cursor, schema)
        assert LEGACY_FIXTURE_PROCEDURE not in _procedure_names(cursor, schema)
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_BRONZE_CURRENCY)
        assert _table_exists(cursor, schema, MIGRATION_FIXTURE_SILVER_DIMPROMOTION)
    finally:
        conn.close()
