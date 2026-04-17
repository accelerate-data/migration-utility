"""Oracle sandbox execution and comparison service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.output_models.sandbox import TestHarnessExecuteOutput
from shared.sandbox.oracle_services import _OracleSandboxCore

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox


class OracleExecutionService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        return _OracleSandboxCore.execute_scenario(self._backend, sandbox_db, scenario)

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        return _OracleSandboxCore.execute_select(self._backend, sandbox_db, sql, fixtures)

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return _OracleSandboxCore.compare_two_sql(
            self._backend, sandbox_db, sql_a, sql_b, fixtures,
        )
