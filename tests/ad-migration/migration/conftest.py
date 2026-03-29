import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: requires Docker SQL Server (MigrationTest database)"
    )
