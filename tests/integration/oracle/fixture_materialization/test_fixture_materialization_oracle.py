"""Oracle integration coverage for canonical MigrationTest materialization."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

oracledb = pytest.importorskip(
    "oracledb",
    reason="oracledb not installed — skipping Oracle materialization integration tests",
)

from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole
from tests.helpers import ORACLE_MIGRATION_SCHEMA, REPO_ROOT

pytestmark = pytest.mark.oracle


def _have_oracle_env() -> bool:
    return bool(os.environ.get("ORACLE_PWD"))


@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_creates_core_objects() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA)
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
        mode=(
            oracledb.AUTH_MODE_SYSDBA
            if os.environ.get("ORACLE_ADMIN_USER", "sys").lower() == "sys"
            else oracledb.AUTH_MODE_DEFAULT
        ),
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


@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_is_idempotent() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA)
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
