"""Base interfaces for technology-specific DB operations."""

from __future__ import annotations

import os
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

    def __init__(self, role: RuntimeRole) -> None:
        self.role = role

    @classmethod
    def from_role(cls, role: RuntimeRole) -> "DatabaseOperations":
        return cls(role)

    def fixture_script_path(self, repo_root: Path) -> Path:
        return repo_root / self.fixture_script_relpath

    @abstractmethod
    def environment_name(self) -> str:
        """Return the concrete environment identifier for this role."""

    @abstractmethod
    def materialize_migration_test_env(self) -> dict[str, str]:
        """Build environment variables for materialize-migration-test.sh."""

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

    def _read_secret(self, env_var_name: str | None) -> str | None:
        if not env_var_name:
            return None
        return os.environ.get(env_var_name)
