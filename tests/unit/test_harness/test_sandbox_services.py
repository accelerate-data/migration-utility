"""Sandbox service module boundary tests."""

from __future__ import annotations

from shared.sandbox.oracle import OracleSandbox
from shared.sandbox.oracle_comparison import OracleComparisonService
from shared.sandbox.oracle_execution import OracleExecutionService
from shared.sandbox.oracle_fixtures import OracleFixtureService
from shared.sandbox.oracle_lifecycle import OracleLifecycleService
from shared.sandbox.sql_server import SqlServerSandbox
from shared.sandbox.sql_server_comparison import SqlServerComparisonService
from shared.sandbox.sql_server_execution import SqlServerExecutionService
from shared.sandbox.sql_server_fixtures import SqlServerFixtureService
from shared.sandbox.sql_server_lifecycle import SqlServerLifecycleService


def test_sql_server_facade_uses_dedicated_service_modules() -> None:
    backend = SqlServerSandbox(host="localhost", port="1433", password="pw")

    assert isinstance(backend._lifecycle, SqlServerLifecycleService)
    assert isinstance(backend._execution, SqlServerExecutionService)
    assert isinstance(backend._fixtures, SqlServerFixtureService)
    assert isinstance(backend._comparison, SqlServerComparisonService)


def test_oracle_facade_uses_dedicated_service_modules() -> None:
    backend = OracleSandbox(
        host="localhost",
        port="1521",
        service="FREEPDB1",
        password="pw",
        admin_user="sys",
        source_schema="SH",
    )

    assert isinstance(backend._lifecycle, OracleLifecycleService)
    assert isinstance(backend._execution, OracleExecutionService)
    assert isinstance(backend._fixtures, OracleFixtureService)
    assert isinstance(backend._comparison, OracleComparisonService)
