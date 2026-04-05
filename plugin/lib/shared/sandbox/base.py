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
        schemas: list[str],
    ) -> dict[str, Any]:
        """Create the sandbox database and clone schema from the source."""

    @abstractmethod
    def sandbox_down(self, sandbox_db: str) -> dict[str, Any]:
        """Drop the sandbox database."""

    @abstractmethod
    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
        """Run one test scenario: insert fixtures, exec proc, capture output."""

    @abstractmethod
    def sandbox_status(self, sandbox_db: str) -> dict[str, Any]:
        """Check whether the sandbox database exists and is accessible."""

    @abstractmethod
    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Seed fixtures, run two SELECT statements, return symmetric diff.

        Within a single transaction (rolled back at the end):
        1. Seed fixture rows into tables
        2. Execute sql_a (extracted core SELECT) → capture rows_a
        3. Execute sql_b (refactored CTE SELECT) → capture rows_b
        4. Compute symmetric difference (rows_a vs rows_b)

        Both sql_a and sql_b must be pure SELECT/WITH statements.

        Returns::

            {
                "status": "ok" | "error",
                "equivalent": bool,
                "a_count": int,
                "b_count": int,
                "a_minus_b": list[dict],  # in A but not B
                "b_minus_a": list[dict],  # in B but not A
                "errors": list[dict],
            }
        """
