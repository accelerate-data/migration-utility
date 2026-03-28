"""DDL directory loader.

Reads a directory containing GO-delimited SQL files and builds a DdlCatalog
mapping normalized names to raw DDL blocks and parsed sqlglot ASTs.

Any .sql file in the directory is loaded.  Object types (table, procedure,
view, function) are auto-detected from CREATE statements — filenames are not
significant.  A single file may contain a mix of object types.

Parse errors
------------
`_parse_block` raises `DdlParseError` when sqlglot cannot produce a structured
AST (falls back to a raw `Command` node).  `_load_file` catches this per-block
and stores the entry with `ast=None` and `parse_error` populated — the
remaining blocks in the file continue loading.  No silent fallback is used.

`extract_refs` raises `DdlParseError` when called on an entry whose AST is
absent *or* whose body contains internal `Command` nodes (partially unparsed
statements).  No regex fallback is used.
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


_EXEC_RE = re.compile(r"\b(?:EXEC|EXECUTE)\b", re.IGNORECASE)


@dataclass
class ObjectRefs:
    """References extracted from a DDL entry's AST."""

    writes_to: list[str] = field(default_factory=list)
    reads_from: list[str] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)
    has_exec: bool = False


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


_BODY_RE = re.compile(
    r"\bAS\s+BEGIN\b(.*)\bEND\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_CMD_CONTROL_FLOW_RE = re.compile(
    r"^\s*(?:BEGIN\s+TRY|END\s+TRY|BEGIN\s+CATCH|END\s+CATCH"
    r"|BEGIN|END|ELSE"
    r"|WHILE\b[^\n;]*"
    r"|IF\b[^\n;]*"
    r")\s*",
    re.IGNORECASE,
)

_MAX_PARSE_DEPTH = 5


def _extract_sql_from_command(cmd: exp.Command) -> str | None:
    """Extract SQL text from a Command node, stripping control flow prefixes.

    Command nodes contain unparsed text where sqlglot gave up. The text
    often starts with BEGIN/END/ELSE/IF/WHILE — strip those to reveal
    the DML underneath, then return it for re-parsing.

    Returns None if no SQL can be extracted (the Command is pure control flow).
    """
    text = cmd.sql()
    prev = None
    while text != prev:
        prev = text
        text = _CMD_CONTROL_FLOW_RE.sub("", text, count=1)
    text = text.strip()
    return text if text else None


def _recursive_parse(sql: str, dialect: str, depth: int = 0) -> list[Any]:
    """Parse SQL text, recursively re-parsing Command nodes.

    sqlglot cannot parse T-SQL control flow (IF, BEGIN/END, ELSE, WHILE,
    TRY/CATCH) and produces Command nodes for those blocks. This function
    extracts the SQL text from Command nodes, strips the control flow
    prefix, and re-parses — recovering the DML statements inside nested
    blocks.
    """
    if depth > _MAX_PARSE_DEPTH:
        return []
    stmts = sqlglot.parse(sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    results: list[Any] = []
    for s in stmts:
        if s is None:
            continue
        if isinstance(s, exp.Command):
            inner_sql = _extract_sql_from_command(s)
            if not inner_sql or inner_sql == s.sql():
                continue
            results.extend(_recursive_parse(inner_sql, dialect, depth + 1))
        elif isinstance(s, exp.If):
            # sqlglot parses IF conditions but loses the body statements.
            # Re-parse the full IF text with control flow stripped.
            if_sql = _extract_sql_from_command(
                exp.Command(this=s.sql())
            )
            if if_sql:
                results.extend(_recursive_parse(if_sql, dialect, depth + 1))
        else:
            results.append(s)
    return results


_STRIP_CONTROL_FLOW_RE = re.compile(
    r"""
      \bIF\s+(?:NOT\s+)?EXISTS\s*\([^)]*\)\s*(?:BEGIN\b)?\s*  # IF [NOT] EXISTS (...) [BEGIN]
    | \bIF\b[^;]*?\bBEGIN\b                                    # IF <condition> BEGIN
    | \bIF\b\s+@@\w+\s*[<>=!]+\s*\S+\s*                       # IF @@var = value
    | \bWHILE\s+(?:NOT\s+)?EXISTS\s*\([^)]*\)\s*(?:BEGIN\b)?\s*  # WHILE [NOT] EXISTS (...) [BEGIN]
    | \bWHILE\b[^;]*?\bBEGIN\b                                 # WHILE <condition> BEGIN
    | \bELSE\s+BEGIN\b                                          # ELSE BEGIN
    | \bELSE\b                                                  # ELSE
    | \bBEGIN\s+TRY\b                                           # BEGIN TRY
    | \bEND\s+TRY\b                                             # END TRY
    | \bBEGIN\s+CATCH\b                                          # BEGIN CATCH
    | \bEND\s+CATCH\b                                            # END CATCH
    | \bBEGIN\b                                                  # standalone BEGIN
    | \bEND\b\s*;?                                               # standalone END
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _parse_body_statements(raw_ddl: str, dialect: str = "tsql") -> list[Any]:
    """Extract the procedure/function body and parse each statement individually.

    Uses a two-pass strategy:
    1. Recursive parse: preserves structure, re-parses Command nodes by
       stripping control flow prefixes.
    2. Fallback: strips all control flow from the raw body and parses the
       remaining DML — catches statements that sqlglot drops inside
       IF <condition> BEGIN...END blocks.

    The union of both passes gives maximum coverage.

    Returns an empty list if no AS BEGIN...END block is found.
    """
    m = _BODY_RE.search(raw_ddl)
    if not m:
        return []
    body = m.group(1).strip()
    if not body:
        return []

    # Pass 1: recursive Command re-parsing
    pass1 = _recursive_parse(body, dialect)

    # Pass 2: strip all control flow, parse flat DML
    stripped = _STRIP_CONTROL_FLOW_RE.sub("", body).strip()
    pass2 = [
        s for s in sqlglot.parse(stripped, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
        if s is not None and not isinstance(s, exp.Command)
    ] if stripped else []

    # Merge: use pass1 as base, add any statements from pass2 not already found
    return _merge_statement_lists(pass1, pass2)


def _stmt_key(s: Any) -> str | None:
    """Return a dedup key for a statement, or None if it can't be serialized."""
    try:
        return s.sql()
    except (ValueError, TypeError):
        return None


def _merge_statement_lists(primary: list[Any], secondary: list[Any]) -> list[Any]:
    """Merge two statement lists, deduplicating by SQL text."""
    seen: set[str] = set()
    for s in primary:
        key = _stmt_key(s)
        if key:
            seen.add(key)
    merged = list(primary)
    for s in secondary:
        key = _stmt_key(s)
        if key and key not in seen:
            merged.append(s)
            seen.add(key)
    return merged


def _collect_refs_from_statements(statements: list[Any]) -> ObjectRefs:
    """Walk a list of parsed statements and collect write/read references."""
    writes_to: set[str] = set()
    reads_from: set[str] = set()

    for stmt in statements:
        if stmt is None or isinstance(stmt, exp.Command):
            continue

        # writes_to: INSERT targets
        for node in stmt.find_all(exp.Insert):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                writes_to.add(_table_fqn(target))

        # writes_to: UPDATE targets
        for node in stmt.find_all(exp.Update):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                writes_to.add(_table_fqn(target))

        # writes_to: DELETE targets (use node.this — node.find(Table) may
        # return a TOP pseudo-table from DELETE TOP (N) syntax)
        for node in stmt.find_all(exp.Delete):
            target = node.this
            if isinstance(target, exp.Table) and _is_real_table(target):
                writes_to.add(_table_fqn(target))

        # writes_to: MERGE targets + reads_from: MERGE USING sources
        for node in stmt.find_all(exp.Merge):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                writes_to.add(_table_fqn(target))
            using = node.args.get("using")
            if isinstance(using, exp.Table) and _is_real_table(using):
                reads_from.add(_table_fqn(using))

        # writes_to: TRUNCATE targets
        for node in stmt.find_all(exp.TruncateTable):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                writes_to.add(_table_fqn(target))

        # writes_to: SELECT INTO targets
        for node in stmt.find_all(exp.Into):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                writes_to.add(_table_fqn(target))

        # reads_from: FROM and JOIN sources
        for node in stmt.find_all(exp.From):
            table = node.find(exp.Table)
            if table and _is_real_table(table):
                reads_from.add(_table_fqn(table))

        for node in stmt.find_all(exp.Join):
            table = node.find(exp.Table)
            if table and _is_real_table(table):
                reads_from.add(_table_fqn(table))

    return ObjectRefs(
        writes_to=sorted(writes_to),
        reads_from=sorted(reads_from - writes_to),
        calls=[],
    )


def extract_refs(entry: DdlEntry) -> ObjectRefs:
    """Extract write/read/call references from a DDL entry.

    For procedures, views, and functions: extracts the body between
    AS BEGIN...END and parses each statement individually, since sqlglot's
    CREATE PROCEDURE AST only captures the first body statement.

    For tables and other CREATE statements: walks the original AST directly.

    Sets has_exec=True if the raw DDL contains EXEC/EXECUTE statements,
    signalling that the proc requires Claude-assisted analysis.

    Raises:
        DdlParseError: if the entry has no AST and no raw_ddl to fall back on.
    """
    has_exec = bool(_EXEC_RE.search(entry.raw_ddl))

    # For entries with AS BEGIN...END bodies, parse the body statements
    body_stmts = _parse_body_statements(entry.raw_ddl)
    if body_stmts:
        refs = _collect_refs_from_statements(body_stmts)
        refs.has_exec = has_exec
        return refs

    # Fallback: walk the original AST (tables, simple views without BEGIN/END)
    if entry.ast is None:
        raise DdlParseError("Cannot extract refs: DDL block failed to parse (ast is None)")

    refs = _collect_refs_from_statements([entry.ast])
    refs.has_exec = has_exec
    return refs


# ── Directory loading ─────────────────────────────────────────────────────────


def _load_file(path: Path, catalog: DdlCatalog, dialect: str = "tsql") -> None:
    """Parse a .sql file and route each block into the correct catalog bucket."""
    if not path.exists():
        return
    blocks = _split_blocks(path.read_text(encoding="utf-8"))
    for block in blocks:
        raw_name = _extract_name(block)
        if not raw_name:
            print(f"loader: could not extract name from block in {path.name}", file=sys.stderr)
            continue
        bucket_name = _extract_type_bucket(block)
        if not bucket_name:
            print(f"loader: could not determine type for '{raw_name}' in {path.name}", file=sys.stderr)
            continue
        key = normalize(raw_name)
        try:
            ast = _parse_block(block, dialect=dialect)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=ast)
        except DdlParseError as exc:
            print(f"loader: parse failed for '{raw_name}', storing with error: {exc}", file=sys.stderr)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=None, parse_error=str(exc))


def load_directory(ddl_path: Path | str, dialect: str = "tsql") -> DdlCatalog:
    """Read all .sql files in a DDL directory and return a populated DdlCatalog.

    Object types are auto-detected from CREATE statements — filenames are not
    significant.  Any .sql file may contain any mix of tables, procedures,
    views, and functions separated by GO delimiters.

    Args:
        ddl_path: Path to directory containing .sql files.
        dialect:  sqlglot dialect for parsing (default: "tsql").

    Raises:
        FileNotFoundError: if ddl_path does not exist.
    """
    path = Path(ddl_path)
    if not path.exists():
        raise FileNotFoundError(f"DDL path does not exist: {path}")

    catalog = DdlCatalog()
    for sql_file in sorted(path.glob("*.sql")):
        _load_file(sql_file, catalog, dialect=dialect)
    return catalog


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
