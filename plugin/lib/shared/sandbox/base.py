"""Abstract base for sandbox backends."""

from __future__ import annotations

import re
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shared.output_models import (
        SandboxDownOutput,
        SandboxStatusOutput,
        SandboxUpOutput,
        TestHarnessExecuteOutput,
    )


def serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ensure all values in a result set are JSON-serializable.

    Primitive types (int, float, str, bool, None) pass through unchanged.
    Decimal values are coerced to str to preserve exact precision.
    bytes/memoryview are hex-encoded.
    LOB-like objects (oracledb LOB — has a read() method) are read and
    treated as str (CLOB) or bytes→hex (BLOB).
    All other non-primitive types (datetime, etc.) are coerced to str.
    """
    out: list[dict[str, Any]] = []
    for row in rows:
        serialized: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, (int, float, str, bool, type(None))):
                serialized[k] = v
            elif isinstance(v, Decimal):
                serialized[k] = str(v)
            elif isinstance(v, (bytes, memoryview)):
                serialized[k] = bytes(v).hex()
            elif hasattr(v, "read"):
                content = v.read()
                if isinstance(content, bytes):
                    serialized[k] = content.hex()
                else:
                    serialized[k] = str(content)
            else:
                serialized[k] = str(v)
        out.append(serialized)
    return out


def validate_fixture_rows(table: str, rows: list[dict[str, Any]]) -> None:
    """Validate that all fixture rows have the same keys as row 0.

    Raises ValueError if any row has a different key set.
    """
    if not rows:
        return
    columns = set(rows[0].keys())
    for i, row in enumerate(rows[1:], start=1):
        if set(row.keys()) != columns:
            raise ValueError(
                f"Fixture row {i} for table {table!r} has "
                f"different keys than row 0"
            )


def generate_sandbox_name() -> str:
    """Generate a random sandbox database/schema name."""
    return f"__test_{uuid.uuid4().hex[:12]}"


def validate_fixtures(
    fixtures: list[dict[str, Any]],
    validate_name: Callable[[str], None],
) -> None:
    """Validate fixture structure: table names, column names, row consistency.

    ``validate_name`` is the backend-specific identifier validator
    (e.g. ``_validate_identifier`` for SQL Server, ``_validate_oracle_identifier``
    for Oracle).
    """
    for fixture in fixtures:
        validate_name(fixture["table"])
        rows = fixture.get("rows", [])
        if rows:
            for col_name in rows[0].keys():
                validate_name(col_name)
            validate_fixture_rows(fixture["table"], rows)


def validate_readonly_sql(sql: str, write_re: re.Pattern[str]) -> None:
    """Reject SQL that contains write operations.

    ``write_re`` is the backend-specific compiled regex of write keywords.
    Raises ValueError if the SQL is empty or contains write operations.
    """
    if not sql or not sql.strip():
        raise ValueError("SQL is empty")
    match = write_re.search(sql)
    if match:
        keyword = match.group(1)
        raise ValueError(
            f"SQL contains write operation '{keyword}'. "
            "Only SELECT/WITH statements are allowed."
        )


def capture_rows(cursor: Any) -> list[dict[str, Any]]:
    """Read all rows from the current cursor result set as dicts."""
    from shared.db_connect import cursor_to_dicts

    return cursor_to_dicts(cursor)


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
    ) -> SandboxUpOutput:
        """Create the sandbox database and clone schema from the source."""

    @abstractmethod
    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        """Drop the sandbox database."""

    @abstractmethod
    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        """Run one test scenario: insert fixtures, exec proc, capture output."""

    @abstractmethod
    def sandbox_status(self, sandbox_db: str) -> SandboxStatusOutput:
        """Check whether the sandbox database exists and is accessible."""

    @abstractmethod
    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        """Seed fixtures, run a SELECT, return result rows.

        Within a single transaction (rolled back at the end):
        1. Seed fixture rows into tables
        2. Execute *sql* (a pure SELECT/WITH statement)
        3. Capture and return result rows
        """

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
