from __future__ import annotations

import os
import shutil

import pytest
from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

from tests.helpers import REPO_ROOT

ORACLE_MIGRATION_SCHEMA = os.environ.get("ORACLE_SCHEMA", "MIGRATIONTEST").upper()
ORACLE_MIGRATION_SCHEMA_PASSWORD = os.environ.get(
    "ORACLE_SCHEMA_PASSWORD",
    ORACLE_MIGRATION_SCHEMA.lower(),
)

_ORACLE_MIGRATION_TEST_READY = False
_SQL_SERVER_MIGRATION_TEST_READY = False
SQL_SERVER_MIGRATION_DATABASE = "MigrationTest"


def build_sql_server_source_role() -> RuntimeRole:
    return RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host=os.environ.get("MSSQL_HOST", "localhost"),
            port=os.environ.get("MSSQL_PORT", "1433"),
            database=SQL_SERVER_MIGRATION_DATABASE,
            user=os.environ.get("MSSQL_USER", "sa"),
            driver=os.environ.get("MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
            password_env="SA_PASSWORD",
        ),
    )


def build_oracle_admin_role() -> RuntimeRole:
    return RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host=os.environ.get("ORACLE_HOST", "localhost"),
            port=os.environ.get("ORACLE_PORT", "1521"),
            service=os.environ.get("ORACLE_SERVICE", "FREEPDB1"),
            user=os.environ.get("ORACLE_ADMIN_USER", "sys"),
            schema=ORACLE_MIGRATION_SCHEMA,
            password_env="ORACLE_PWD",
        ),
    )


def ensure_sql_server_migration_test_materialized() -> None:
    global _SQL_SERVER_MIGRATION_TEST_READY
    if _SQL_SERVER_MIGRATION_TEST_READY:
        return

    role = build_sql_server_source_role()
    result = materialize_migration_test(role, REPO_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "SQL Server MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _SQL_SERVER_MIGRATION_TEST_READY = True


def ensure_oracle_migration_test_materialized() -> None:
    global _ORACLE_MIGRATION_TEST_READY
    if _ORACLE_MIGRATION_TEST_READY:
        return

    if not os.environ.get("ORACLE_PWD"):
        pytest.skip("ORACLE_PWD not set")
    if shutil.which("sqlplus") is None:
        pytest.importorskip(
            "oracledb",
            reason="sqlplus not installed and oracledb not available for Oracle materialization",
        )

    role = build_oracle_admin_role()
    result = materialize_migration_test(
        role,
        REPO_ROOT,
        extra_env={"ORACLE_SCHEMA_PASSWORD": ORACLE_MIGRATION_SCHEMA_PASSWORD},
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Oracle MigrationTest materialization failed:\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    _ORACLE_MIGRATION_TEST_READY = True


def configure_oracle_extract_env(monkeypatch: pytest.MonkeyPatch) -> None:
    ensure_oracle_migration_test_materialized()
    monkeypatch.setenv("ORACLE_USER", ORACLE_MIGRATION_SCHEMA)
    monkeypatch.setenv("ORACLE_PASSWORD", ORACLE_MIGRATION_SCHEMA_PASSWORD)
    monkeypatch.setenv(
        "ORACLE_DSN",
        f"{os.environ.get('ORACLE_HOST', 'localhost')}:"
        f"{os.environ.get('ORACLE_PORT', '1521')}/"
        f"{os.environ.get('ORACLE_SERVICE', 'FREEPDB1')}",
    )


def require_oracle_extract_env() -> None:
    oracledb = pytest.importorskip(
        "oracledb",
        reason="oracledb not installed - skipping Oracle integration tests",
    )
    for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
        if not os.environ.get(var):
            pytest.skip(f"{var} not set")
    try:
        conn = oracledb.connect(
            user=os.environ["ORACLE_USER"],
            password=os.environ["ORACLE_PASSWORD"],
            dsn=os.environ["ORACLE_DSN"],
        )
        conn.close()
    except oracledb.Error as exc:
        pytest.skip(f"Oracle test database not reachable: {exc}")
