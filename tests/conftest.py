import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

# Ensure repo, lib, and tests/unit are importable regardless of cwd.
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent / "unit"))


def pytest_configure(config):
    # Disable pyodbc connection pooling early — FreeTDS reuses connections
    # with stale cursor state, causing "Invalid cursor state" errors.
    try:
        import pyodbc
        pyodbc.pooling = False
    except ImportError:
        pass

    config.addinivalue_line(
        "markers",
        "integration: requires Docker SQL Server with the canonical MigrationTest schema fixture materialized in the configured database",
    )
    config.addinivalue_line(
        "markers",
        "oracle: requires Docker Oracle with SH schema loaded "
        "(ORACLE_USER / ORACLE_PASSWORD / ORACLE_DSN)",
    )
