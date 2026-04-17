"""Data structures and exceptions for standalone DDL MCP loading."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ddl_mcp_support.name_resolver import normalize


class DdlParseError(Exception):
    """Raised when sqlglot cannot parse a DDL block or extract references."""


@dataclass
class ObjectRefs:
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
    unsupported_syntax_nodes: list[str] | None = None
    long_truncation: bool = False


@dataclass
class DdlCatalog:
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
