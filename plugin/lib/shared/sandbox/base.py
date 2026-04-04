"""Abstract base for sandbox backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class SandboxBackend(ABC):
    """Interface that every technology-specific sandbox must implement."""

    @classmethod
    @abstractmethod
    def from_env(cls, manifest: dict[str, Any]) -> SandboxBackend:
        """Create an instance from environment variables and manifest config."""

    @abstractmethod
    def sandbox_up(
        self,
        run_id: str,
        schemas: list[str],
    ) -> dict[str, Any]:
        """Create the sandbox database and clone schema from the source."""

    @abstractmethod
    def sandbox_down(self, run_id: str) -> dict[str, Any]:
        """Drop the sandbox database."""

    @abstractmethod
    def execute_scenario(
        self,
        run_id: str,
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
        """Run one test scenario: insert fixtures, exec proc, capture output."""

    @abstractmethod
    def sandbox_status(self, run_id: str) -> dict[str, Any]:
        """Check whether the sandbox database exists and is accessible."""

    @staticmethod
    def sandbox_db_name(run_id: str) -> str:
        """Deterministic sandbox database name."""
        return f"__test_{run_id.replace('-', '_')}"
