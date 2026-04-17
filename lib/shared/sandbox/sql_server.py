"""Public SQL Server sandbox facade."""

from __future__ import annotations

from typing import Any

from shared.output_models.sandbox import (
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.sandbox.sql_server_services import (
    _detect_remote_exec_target,
    _get_identity_columns,
    _get_not_null_defaults,
    _import_pyodbc,
    _quote_identifier,
    _split_identifier_parts,
    _SqlServerSandboxCore,
    _validate_fixtures,
    _validate_identifier,
    _validate_readonly_sql,
    _validate_sandbox_db_name,
)


class _SqlServerLifecycleService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        return _SqlServerSandboxCore.sandbox_up(self._backend, schemas)

    def sandbox_reset(self, sandbox_db: str, schemas: list[str]) -> SandboxUpOutput:
        return _SqlServerSandboxCore.sandbox_reset(self._backend, sandbox_db, schemas)

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        return _SqlServerSandboxCore.sandbox_down(self._backend, sandbox_db)

    def sandbox_status(
        self,
        sandbox_db: str,
        schemas: list[str] | None = None,
    ) -> SandboxStatusOutput:
        return _SqlServerSandboxCore.sandbox_status(self._backend, sandbox_db, schemas)


class _SqlServerExecutionService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        return _SqlServerSandboxCore.execute_scenario(self._backend, sandbox_db, scenario)

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        return _SqlServerSandboxCore.execute_select(self._backend, sandbox_db, sql, fixtures)

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return _SqlServerSandboxCore.compare_two_sql(
            self._backend, sandbox_db, sql_a, sql_b, fixtures,
        )


class SqlServerSandbox(_SqlServerSandboxCore):
    """Thin public facade for SQL Server sandbox operations."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._lifecycle = _SqlServerLifecycleService(self)
        self._execution = _SqlServerExecutionService(self)

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
    "SqlServerSandbox",
    "_detect_remote_exec_target",
    "_get_identity_columns",
    "_get_not_null_defaults",
    "_import_pyodbc",
    "_quote_identifier",
    "_split_identifier_parts",
    "_validate_fixtures",
    "_validate_identifier",
    "_validate_readonly_sql",
    "_validate_sandbox_db_name",
]
