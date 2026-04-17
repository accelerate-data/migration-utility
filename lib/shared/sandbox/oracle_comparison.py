"""Oracle sandbox SQL comparison service."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.sandbox.oracle_services import _OracleSandboxCore

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox


class OracleComparisonService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

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
