"""DDL directory loader.

Reads a directory containing GO-delimited SQL files and builds a DdlCatalog
mapping normalized names to raw DDL blocks and parsed sqlglot ASTs.

Expected directory layout:
    tables.sql      CREATE TABLE statements, GO-separated
    procedures.sql  CREATE PROCEDURE statements, GO-separated
    views.sql       CREATE VIEW statements, GO-separated (optional)
    functions.sql   CREATE FUNCTION statements, GO-separated (optional)

Parse errors
------------
`_parse_block` raises `DdlParseError` when sqlglot cannot produce a structured
AST (falls back to a raw `Command` node).  `load_directory` catches these
per-block, records the error in `DdlEntry.parse_error`, and continues.

`extract_refs` raises `DdlParseError` when called on an entry whose AST is
absent *or* whose body contains internal `Command` nodes (partially unparsed
statements).  Callers — including `index_directory` — catch this and record
`parse_error` in the catalog output.  No regex fallback is used.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlglot
import sqlglot.expressions as exp

from shared.name_resolver import normalize

_GO_RE = re.compile(r"(?:^|\n)\s*GO\b", re.IGNORECASE)

_OBJECT_NAME_RE = re.compile(
    r"CREATE\s+(?:OR\s+ALTER\s+)?"
    r"(?:TABLE|PROCEDURE|PROC|VIEW|FUNCTION)\s+"
    r"((?:\[?\w+\]?\.){0,3}\[?\w+\]?)",
    re.IGNORECASE,
)

# Object type keyword → catalog bucket name
_TYPE_MAP = {
    "table": "tables",
    "procedure": "procedures",
    "proc": "procedures",
    "view": "views",
    "function": "functions",
}

_TYPE_KEYWORD_RE = re.compile(
    r"CREATE\s+(?:OR\s+ALTER\s+)?(TABLE|PROCEDURE|PROC|VIEW|FUNCTION)\b",
    re.IGNORECASE,
)

# catalog.json format version
_CATALOG_SCHEMA_VERSION = "1.0"


# ── Errors ────────────────────────────────────────────────────────────────────


class DdlParseError(Exception):
    """Raised when sqlglot cannot parse a DDL block or extract references."""


# ── Data structures ───────────────────────────────────────────────────────────


@dataclass
class ObjectRefs:
    """References extracted from a DDL entry's AST."""

    writes_to: list[str] = field(default_factory=list)
    reads_from: list[str] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)


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


# ── Parsing helpers ───────────────────────────────────────────────────────────


def _split_blocks(sql: str) -> list[str]:
    """Split GO-delimited SQL into individual object blocks."""
    blocks = _GO_RE.split(sql)
    return [b.strip() for b in blocks if b.strip()]


def _extract_name(block: str) -> str | None:
    m = _OBJECT_NAME_RE.search(block)
    return m.group(1) if m else None


def _extract_type_bucket(block: str) -> str | None:
    m = _TYPE_KEYWORD_RE.search(block)
    if m:
        return _TYPE_MAP.get(m.group(1).lower())
    return None


def _parse_block(block: str, dialect: str = "tsql") -> Any:
    """Parse a single DDL block with sqlglot and return the AST.

    Raises:
        DdlParseError: if sqlglot falls back to a `Command` node (could not
            parse the block as structured DDL).
    """
    try:
        result = sqlglot.parse_one(block, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    except Exception as exc:
        name = _extract_name(block) or "<unknown>"
        raise DdlParseError(f"sqlglot raised an exception parsing '{name}': {exc}") from exc

    if result is None or isinstance(result, exp.Command):
        name = _extract_name(block) or "<unknown>"
        raise DdlParseError(
            f"sqlglot could not parse DDL block for '{name}' (fell back to Command)"
        )
    return result


# ── Reference extraction ──────────────────────────────────────────────────────


def _is_real_table(table: exp.Table) -> bool:
    """Return True only for schema-qualified, single-database table references.

    Filters out:
    - Temp tables: sqlglot strips '#' so they appear with db='' and catalog=''
    - Unqualified bare names (db=''): likely CTEs, aliases, or temp tables
    - Cross-database references (catalog!=''): 3- or 4-part names
    """
    return bool(table.db) and not table.catalog


def _table_fqn(table: exp.Table) -> str:
    """Return a normalized FQN for a real table reference."""
    return normalize(f"{table.db}.{table.name}")


def extract_refs(entry: DdlEntry) -> ObjectRefs:
    """Extract write/read/call references from a DDL entry using AST only.

    Raises:
        DdlParseError: if the entry has no AST (parse failed) or if the body
            contains internal Command nodes (partially unparsed statements).
            No regex fallback is used — callers must handle partial failures.
    """
    if entry.ast is None:
        raise DdlParseError("Cannot extract refs: DDL block failed to parse (ast is None)")

    internal_commands = list(entry.ast.find_all(exp.Command))
    if internal_commands:
        sample = str(internal_commands[0])[:80]
        raise DdlParseError(
            f"Cannot extract refs: body contains unparsed statement(s) "
            f"(sqlglot Command node): {sample!r}"
        )

    writes_to: set[str] = set()
    reads_from: set[str] = set()

    # writes_to: INSERT targets
    for node in entry.ast.find_all(exp.Insert):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            writes_to.add(_table_fqn(target))

    # writes_to: UPDATE targets
    for node in entry.ast.find_all(exp.Update):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            writes_to.add(_table_fqn(target))

    # writes_to: DELETE targets
    for node in entry.ast.find_all(exp.Delete):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            writes_to.add(_table_fqn(target))

    # writes_to: MERGE targets
    for node in entry.ast.find_all(exp.Merge):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            writes_to.add(_table_fqn(target))

    # reads_from: FROM and JOIN sources (exclude write targets)
    for node in entry.ast.find_all(exp.From):
        table = node.find(exp.Table)
        if table and _is_real_table(table):
            reads_from.add(_table_fqn(table))

    for node in entry.ast.find_all(exp.Join):
        table = node.find(exp.Table)
        if table and _is_real_table(table):
            reads_from.add(_table_fqn(table))

    # calls: EXEC targets — sqlglot EXEC always produces an internal Command,
    # so calls cannot be extracted via AST. This is a known limitation; callers
    # that need call-graph analysis must handle parse_error entries separately.

    return ObjectRefs(
        writes_to=sorted(writes_to),
        reads_from=sorted(reads_from - writes_to),
        calls=[],
    )


# ── Directory loading ─────────────────────────────────────────────────────────


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
        try:
            ast = _parse_block(block, dialect=dialect)
            result[key] = DdlEntry(raw_ddl=block, ast=ast)
        except DdlParseError as exc:
            print(f"loader: parse error for '{key}' in {path.name}: {exc}", file=sys.stderr)
            result[key] = DdlEntry(raw_ddl=block, ast=None, parse_error=str(exc))
    return result


def load_directory(ddl_path: Path | str, dialect: str = "tsql") -> DdlCatalog:
    """Read a DDL artifacts directory and return a populated DdlCatalog.

    Parse errors are surfaced via DdlEntry.parse_error — loading continues
    for all remaining objects.

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


# ── On-disk index ─────────────────────────────────────────────────────────────


def index_directory(
    ddl_path: Path | str,
    output_dir: Path | str,
    dialect: str = "tsql",
) -> None:
    """Split a DDL directory into per-object files and write a catalog.json.

    Creates:
        output_dir/tables/         one .sql file per table
        output_dir/procedures/     one .sql file per procedure
        output_dir/views/          one .sql file per view
        output_dir/functions/      one .sql file per function
        output_dir/catalog.json    reference graph

    Objects that fail to parse still get their .sql file written (raw DDL
    preserved) but appear with writes_to/reads_from/calls=[] and a non-null
    parse_error in catalog.json.

    Args:
        ddl_path:   Source DDL artifacts directory.
        output_dir: Destination directory (created if absent).
        dialect:    sqlglot dialect for parsing (default: "tsql").

    Raises:
        FileNotFoundError: if ddl_path does not exist.
    """
    catalog = load_directory(ddl_path, dialect=dialect)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    bucket_map = {
        "tables": catalog.tables,
        "procedures": catalog.procedures,
        "views": catalog.views,
        "functions": catalog.functions,
    }

    objects: dict[str, dict] = {}

    for bucket, entries in bucket_map.items():
        if not entries:
            continue
        bucket_dir = out / bucket
        bucket_dir.mkdir(exist_ok=True)
        for name, entry in entries.items():
            file_name = f"{name}.sql"
            (bucket_dir / file_name).write_text(entry.raw_ddl, encoding="utf-8")

            # Determine singular type label for catalog
            type_label = bucket.rstrip("s")  # "procedures" → "procedure"

            refs_dict: dict = {"writes_to": [], "reads_from": [], "calls": []}
            parse_error: str | None = entry.parse_error
            if parse_error is None:
                try:
                    refs = extract_refs(entry)
                    refs_dict = {
                        "writes_to": refs.writes_to,
                        "reads_from": refs.reads_from,
                        "calls": refs.calls,
                    }
                except DdlParseError as exc:
                    parse_error = str(exc)

            objects[name] = {
                "type": type_label,
                "file": f"{bucket}/{file_name}",
                **refs_dict,
                "parse_error": parse_error,
            }

    catalog_doc = {
        "schema_version": _CATALOG_SCHEMA_VERSION,
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "source": str(Path(ddl_path).resolve()),
        "objects": objects,
    }
    (out / "catalog.json").write_text(
        json.dumps(catalog_doc, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def load_catalog(output_dir: Path | str) -> DdlCatalog:
    """Load a DdlCatalog from a pre-built index directory.

    Reads catalog.json and the individual .sql files. Does not re-parse with
    sqlglot — DdlEntry.ast is None for all entries.

    Args:
        output_dir: Directory previously written by index_directory().

    Raises:
        FileNotFoundError: if output_dir or catalog.json does not exist.
    """
    out = Path(output_dir)
    catalog_path = out / "catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(f"catalog.json not found in {out}")

    doc = json.loads(catalog_path.read_text(encoding="utf-8"))
    objects = doc.get("objects", {})

    result = DdlCatalog()
    bucket_attr = {
        "table": "tables",
        "procedure": "procedures",
        "view": "views",
        "function": "functions",
    }

    for name, obj in objects.items():
        file_path = out / obj["file"]
        raw_ddl = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
        entry = DdlEntry(
            raw_ddl=raw_ddl,
            ast=None,
            parse_error=obj.get("parse_error"),
        )
        attr = bucket_attr.get(obj.get("type", ""), "")
        if attr:
            getattr(result, attr)[name] = entry

    return result
