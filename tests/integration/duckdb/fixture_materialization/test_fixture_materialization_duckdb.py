"""DuckDB integration coverage for canonical MigrationTest materialization."""

from __future__ import annotations

from pathlib import Path

import pytest

duckdb = pytest.importorskip(
    "duckdb",
    reason="duckdb not installed — skipping DuckDB materialization tests",
)

from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[4]


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
        assert (
            conn.execute(
                "select count(*) from information_schema.tables "
                "where table_schema = 'silver' and table_name = 'DimProduct'"
            ).fetchone()[0]
            == 1
        )
    finally:
        conn.close()
