import sys
from pathlib import Path

import pytest

# Ensure lib/ is on the path so tests can import shared.* directly.
sys.path.insert(0, str(Path(__file__).parents[2] / "lib"))


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: requires Docker SQL Server (MigrationTest database)"
    )
