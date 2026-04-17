"""SQL Server sandbox SQL comparison service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.sandbox.sql_server_services import _SqlServerSandboxCore

if TYPE_CHECKING:
    from shared.sandbox.sql_server import SqlServerSandbox


class SqlServerComparisonService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

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
