"""Public Oracle sandbox facade."""

from __future__ import annotations

from typing import Any

from shared.output_models.sandbox import (
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.sandbox.oracle_services import (
    _get_oracle_not_null_defaults,
    _import_oracledb,
    _OracleSandboxCore,
    _validate_fixtures,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
    _validate_readonly_sql,
)


class _OracleLifecycleService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        return _OracleSandboxCore.sandbox_up(self._backend, schemas)

    def sandbox_reset(self, sandbox_db: str, schemas: list[str]) -> SandboxUpOutput:
        return _OracleSandboxCore.sandbox_reset(self._backend, sandbox_db, schemas)

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        return _OracleSandboxCore.sandbox_down(self._backend, sandbox_db)

    def sandbox_status(
        self,
        sandbox_db: str,
        schemas: list[str] | None = None,
    ) -> SandboxStatusOutput:
        return _OracleSandboxCore.sandbox_status(self._backend, sandbox_db, schemas)


class _OracleExecutionService:
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


class OracleSandbox(_OracleSandboxCore):
    """Thin public facade for Oracle sandbox operations."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._lifecycle = _OracleLifecycleService(self)
        self._execution = _OracleExecutionService(self)

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        return self._lifecycle.sandbox_up(schemas)

    def sandbox_reset(self, sandbox_db: str, schemas: list[str]) -> SandboxUpOutput:
        return self._lifecycle.sandbox_reset(sandbox_db, schemas)

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        return self._lifecycle.sandbox_down(sandbox_db)

    def sandbox_status(
        self,
        sandbox_db: str,
        schemas: list[str] | None = None,
    ) -> SandboxStatusOutput:
        return self._lifecycle.sandbox_status(sandbox_db, schemas)

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        return self._execution.execute_scenario(sandbox_db, scenario)

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        return self._execution.execute_select(sandbox_db, sql, fixtures)

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self._execution.compare_two_sql(sandbox_db, sql_a, sql_b, fixtures)


__all__ = [
    "OracleSandbox",
    "_get_oracle_not_null_defaults",
    "_import_oracledb",
    "_validate_fixtures",
    "_validate_oracle_identifier",
    "_validate_oracle_sandbox_name",
    "_validate_readonly_sql",
]
