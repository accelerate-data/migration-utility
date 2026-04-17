"""Oracle sandbox fixture and view materialization service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.sandbox.oracle_services import _OracleSandboxCore

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox


class OracleFixtureService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def seed_fixtures(
        self,
        cursor: Any,
        sandbox_schema: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        _OracleSandboxCore._seed_fixtures(
            self._backend, cursor, sandbox_schema, fixtures,
        )

    def ensure_view_tables(
        self,
        sandbox_db: str,
        given: list[dict[str, Any]],
    ) -> list[str]:
        return _OracleSandboxCore._ensure_view_tables(
            self._backend, sandbox_db, given,
        )
