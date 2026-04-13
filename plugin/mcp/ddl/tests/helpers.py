from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from shared.fixture_materialization import materialize_migration_test
from shared.runtime_config_models import RuntimeConnection, RuntimeRole

REPO_ROOT = Path(__file__).resolve().parents[4]
SHARED_LIB_DIR = REPO_ROOT / "plugin" / "lib"
ORACLE_MIGRATION_SCHEMA = os.environ.get("ORACLE_SCHEMA", "MIGRATIONTEST").upper()
ORACLE_MIGRATION_SCHEMA_PASSWORD = os.environ.get(
    "ORACLE_SCHEMA_PASSWORD", ORACLE_MIGRATION_SCHEMA.lower()
)
_ORACLE_MIGRATION_TEST_READY = False


def git_init(path: Path) -> None:
    subprocess.run(["git", "init", str(path)], capture_output=True, check=True)


def run_setup_ddl(args: list[str], *, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["uv", "run", "setup-ddl", *args],
        cwd=str(SHARED_LIB_DIR),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


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
    role = RuntimeRole(
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
