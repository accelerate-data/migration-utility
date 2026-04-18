"""Base interfaces for technology-specific DB operations."""

from __future__ import annotations

import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from shared.runtime_config_models import RuntimeRole


@dataclass(frozen=True)
class ColumnSpec:
    """One physical column to materialize on a target relation."""

    name: str
    source_type: str
    nullable: bool = True


class DatabaseOperations(ABC):
    """Technology-specific primitive operations exposed to orchestration."""

    fixture_script_relpath: str

    def __init__(self, role: RuntimeRole, project_root: Path | None = None) -> None:
        self.role = role
        self.project_root = project_root

    @classmethod
    def from_role(
        cls,
        role: RuntimeRole,
        *,
        project_root: Path | None = None,
    ) -> "DatabaseOperations":
        return cls(role, project_root=project_root)

    def fixture_script_path(self, repo_root: Path) -> Path:
        return repo_root / self.fixture_script_relpath

    @abstractmethod
    def environment_name(self) -> str:
        """Return the concrete environment identifier for this role."""

    @abstractmethod
    def materialize_migration_test_env(self) -> dict[str, str]:
        """Build environment variables for the canonical materialize.sh script."""

    @abstractmethod
    def ensure_source_schema(self, schema_name: str) -> None:
        """Ensure the configured physical source schema exists."""

    @abstractmethod
    def list_source_tables(self, schema_name: str) -> set[str]:
        """Return existing table names in the physical source schema."""

    @abstractmethod
    def create_source_table(
        self,
        schema_name: str,
        table_name: str,
        columns: list[ColumnSpec],
    ) -> None:
        """Create one physical source table for dbt source() resolution."""

    @abstractmethod
    def read_table_rows(
        self,
        schema_name: str,
        table_name: str,
        columns: list[str] | None = None,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        """Read table rows for dbt seed CSV export."""

    @abstractmethod
    def fetch_source_rows(
        self,
        schema_name: str,
        table_name: str,
        *,
        limit: int,
        predicate: str | None = None,
        columns: list[str] | None = None,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        """Read capped source rows for target-side source replication."""

    @abstractmethod
    def truncate_table(self, schema_name: str, table_name: str) -> None:
        """Delete all rows from one physical table."""

    @abstractmethod
    def insert_rows(
        self,
        schema_name: str,
        table_name: str,
        columns: list[str],
        rows: list[tuple[object, ...]],
    ) -> int:
        """Insert rows into one physical table and return the inserted row count."""

    def _read_secret(self, env_var_name: str | None) -> str | None:
        if not env_var_name:
            return None
        return os.environ.get(env_var_name)

    _SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_ $#]*$")

    def _validate_identifier(self, name: str) -> None:
        """Raise ``ValueError`` if *name* is not a safe SQL identifier."""
        if not self._SAFE_IDENTIFIER_RE.match(name):
            raise ValueError(f"Unsafe SQL identifier: {name!r}")

    def _base_type_token(self, source_type: str) -> str:
        """Return the leading SQL type token without precision/length suffixes."""
        return source_type.upper().strip().split("(", 1)[0].strip()
