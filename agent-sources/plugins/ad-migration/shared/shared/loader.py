"""DDL directory loader.

Reads a directory containing GO-delimited SQL files and builds a DdlCatalog
mapping normalized names to raw DDL blocks and parsed sqlglot ASTs.

Expected directory layout:
    tables.sql      CREATE TABLE statements, GO-separated
    procedures.sql  CREATE PROCEDURE statements, GO-separated
    views.sql       CREATE VIEW statements, GO-separated (optional)
    functions.sql   CREATE FUNCTION statements, GO-separated (optional)
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot

from shared.name_resolver import normalize

_GO_RE = re.compile(r"(?:^|\n)\s*GO\b", re.IGNORECASE)

_OBJECT_NAME_RE = re.compile(
    r"CREATE\s+(?:OR\s+ALTER\s+)?"
    r"(?:TABLE|PROCEDURE|PROC|VIEW|FUNCTION)\s+"
    r"((?:\[?\w+\]?\.){0,3}\[?\w+\]?)",
    re.IGNORECASE,
)


def _split_blocks(sql: str) -> list[str]:
    """Split GO-delimited SQL into individual object blocks."""
    blocks = _GO_RE.split(sql)
    return [b.strip() for b in blocks if b.strip()]


def _extract_name(block: str) -> str | None:
    m = _OBJECT_NAME_RE.search(block)
    return m.group(1) if m else None


def _parse_block(block: str, dialect: str = "tsql") -> Any | None:
    """Parse a single DDL block with sqlglot; return AST or None on failure."""
    try:
        return sqlglot.parse_one(block, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    except Exception:  # noqa: BLE001
        return None


@dataclass
class DdlEntry:
    raw_ddl: str
    ast: Any | None


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


def _load_file(path: Path, dialect: str = "tsql") -> dict[str, DdlEntry]:
    if not path.exists():
        return {}
    blocks = _split_blocks(path.read_text(encoding="utf-8"))
    result: dict[str, DdlEntry] = {}
    for block in blocks:
        raw_name = _extract_name(block)
        if not raw_name:
            print(f"loader: could not extract name from block in {path.name}", file=sys.stderr)
            continue
        key = normalize(raw_name)
        ast = _parse_block(block, dialect=dialect)
        result[key] = DdlEntry(raw_ddl=block, ast=ast)
    return result


def load_directory(ddl_path: Path | str, dialect: str = "tsql") -> DdlCatalog:
    """Read a DDL artifacts directory and return a populated DdlCatalog.

    Args:
        ddl_path: Path to directory containing tables.sql, procedures.sql, etc.
        dialect:  sqlglot dialect for parsing (default: "tsql").

    Raises:
        FileNotFoundError: if ddl_path does not exist.
    """
    path = Path(ddl_path)
    if not path.exists():
        raise FileNotFoundError(f"DDL path does not exist: {path}")

    return DdlCatalog(
        tables=_load_file(path / "tables.sql", dialect=dialect),
        procedures=_load_file(path / "procedures.sql", dialect=dialect),
        views=_load_file(path / "views.sql", dialect=dialect),
        functions=_load_file(path / "functions.sql", dialect=dialect),
    )
