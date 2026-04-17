"""SQL Server sandbox fixture and view materialization service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.sandbox.sql_server_services import _SqlServerSandboxCore

if TYPE_CHECKING:
    from shared.sandbox.sql_server import SqlServerSandbox


class SqlServerFixtureService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def seed_fixtures(
        self,
        cursor: Any,
        sandbox_db: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        _SqlServerSandboxCore._seed_fixtures(
            self._backend, cursor, sandbox_db, fixtures,
        )

    def ensure_view_tables(
        self,
        sandbox_db: str,
        given: list[dict[str, Any]],
    ) -> list[str]:
        return _SqlServerSandboxCore._ensure_view_tables(
            self._backend, sandbox_db, given,
        )
