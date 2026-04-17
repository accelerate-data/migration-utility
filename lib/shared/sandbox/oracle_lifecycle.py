"""Oracle sandbox lifecycle service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from shared.output_models.sandbox import (
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
)
from shared.sandbox.oracle_services import _OracleSandboxCore

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox


class OracleLifecycleService:
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
