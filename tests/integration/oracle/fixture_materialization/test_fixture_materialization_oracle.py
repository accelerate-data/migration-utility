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


def _have_oracle_env() -> bool:
    return oracle_is_available(oracledb)


@pytest.mark.skipif(not _have_oracle_env(), reason="Oracle fixture env not configured")
def test_materialize_migration_test_oracle_creates_core_objects() -> None:
    schema = os.environ.get("ORACLE_SCHEMA", ORACLE_MIGRATION_SCHEMA)
    role = build_oracle_admin_role()
    result = materialize_migration_test(role, REPO_ROOT)
    assert result.returncode == 0, result.stderr

    conn = oracledb.connect(**build_oracle_admin_connect_kwargs(oracledb))
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
    role = build_oracle_admin_role()
    first = materialize_migration_test(role, REPO_ROOT)
    second = materialize_migration_test(role, REPO_ROOT)
    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
