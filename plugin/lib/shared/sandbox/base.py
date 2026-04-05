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
    def compare_sql(
        self,
        sandbox_db: str,
        procedure: str,
        target_table: str,
        refactored_sql: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Seed fixtures, run original proc and refactored SQL, return diff.

        Within a single transaction (rolled back at the end):
        1. Seed fixture rows into tables
        2. EXEC procedure → SELECT * FROM target_table → capture rows_original
        3. DELETE FROM target_table (wipe proc output)
        4. Execute refactored CTE SELECT → capture rows_refactored
        5. Compute symmetric difference (original vs refactored)

        Returns::

            {
                "status": "ok" | "error",
                "equivalent": bool,
                "original_count": int,
                "refactored_count": int,
                "a_minus_b": list[dict],  # in original but not refactored
                "b_minus_a": list[dict],  # in refactored but not original
                "errors": list[dict],
            }
        """
