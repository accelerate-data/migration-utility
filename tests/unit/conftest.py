import sys
from pathlib import Path

import pytest

# Ensure plugin/lib/ is on the path so tests can import shared.* directly.
sys.path.insert(0, str(Path(__file__).parents[2] / "plugin" / "lib"))
# Ensure tests/unit/ is on the path so test modules can import shared helpers.
sys.path.insert(0, str(Path(__file__).parent))


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: requires Docker SQL Server (MigrationTest database)"
    )
    config.addinivalue_line(
        "markers",
        "oracle: requires Docker Oracle with SH schema loaded "
        "(ORACLE_USER / ORACLE_PASSWORD / ORACLE_DSN)",
    )
