from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SHARED_LIB_DIR = REPO_ROOT / "plugin" / "lib"


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
