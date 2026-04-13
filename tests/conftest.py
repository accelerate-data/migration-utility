import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Ensure repo, plugin/lib, and tests/unit are importable regardless of cwd.
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "plugin" / "lib"))
sys.path.insert(0, str(Path(__file__).resolve().parent / "unit"))


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: requires Docker SQL Server (MigrationTest database)"
    )
    config.addinivalue_line(
        "markers",
        "oracle: requires Docker Oracle with SH schema loaded "
        "(ORACLE_USER / ORACLE_PASSWORD / ORACLE_DSN)",
    )
