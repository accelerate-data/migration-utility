"""Data structures and exceptions for DDL loading."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shared.name_resolver import normalize


# ── Errors ────────────────────────────────────────────────────────────────────


class DdlParseError(Exception):
    """Raised when sqlglot cannot parse a DDL block or extract references."""


class CatalogNotFoundError(Exception):
    """Raised when the catalog/ directory is missing from the DDL path."""

    def __init__(self, ddl_path: Any) -> None:
        super().__init__(f"No catalog/ directory found in {ddl_path}")
        self.ddl_path = ddl_path


class CatalogFileMissingError(Exception):
    """Raised when a specific catalog JSON file is missing."""

    def __init__(self, object_type: str, fqn: str) -> None:
        super().__init__(f"No catalog file for {object_type} {fqn}")
        self.object_type = object_type
        self.fqn = fqn


class ProfileMissingError(Exception):
    """Raised when a table catalog file has no profile section."""

    def __init__(self, table_fqn: str) -> None:
        super().__init__(f"Table {table_fqn} has no profile section — run profiler first")
        self.table_fqn = table_fqn


class ObjectNotFoundError(Exception):
    """Raised when a named DDL object is not found in the catalog."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Object not found: {name}")
        self.name = name


# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class ObjectRefs:
    """References extracted from a DDL entry's AST."""

    writes_to: list[str] = field(default_factory=list)
    reads_from: list[str] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)
    uses_functions: list[str] = field(default_factory=list)
    needs_llm: bool = False
    write_operations: dict[str, list[str]] = field(default_factory=dict)
    statements: list[dict[str, str]] = field(default_factory=list)


@dataclass
class DdlEntry:
    raw_ddl: str
    ast: Any | None
    parse_error: str | None = None


@dataclass
class DdlCatalog:
    """In-memory catalog of DDL objects loaded from a directory.

    Keys in each mapping are normalized names (lowercase, bracket-free,
    schema-qualified) as produced by name_resolver.normalize().
    """

    tables: dict[str, DdlEntry] = field(default_factory=dict)
    procedures: dict[str, DdlEntry] = field(default_factory=dict)
    views: dict[str, DdlEntry] = field(default_factory=dict)
    functions: dict[str, DdlEntry] = field(default_factory=dict)

    def get_table(self, name: str) -> DdlEntry | None:
        return self.tables.get(normalize(name))

    def get_procedure(self, name: str) -> DdlEntry | None:
        return self.procedures.get(normalize(name))

    def get_view(self, name: str) -> DdlEntry | None:
        return self.views.get(normalize(name))

    def get_function(self, name: str) -> DdlEntry | None:
        return self.functions.get(normalize(name))
