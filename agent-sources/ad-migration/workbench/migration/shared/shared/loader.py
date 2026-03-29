"""DDL directory loader.

Reads a directory containing GO-delimited SQL files and builds a DdlCatalog
mapping normalized names to raw DDL blocks and parsed sqlglot ASTs.

Any .sql file in the directory is loaded.  Object types (table, procedure,
view, function) are auto-detected from CREATE statements — filenames are not
significant.  A single file may contain a mix of object types.

Two-tier analysis
-----------------
Procedure bodies are parsed with a single sqlglot pass.  Statements that
sqlglot can fully parse produce deterministic refs (writes_to, reads_from,
write_operations).  Statements wrapped in T-SQL control flow (IF/ELSE,
TRY/CATCH, BEGIN/END) or containing EXEC/dynamic SQL are flagged via
``needs_llm=True`` on the returned ``ObjectRefs``, signalling that the LLM
should read the raw DDL to complete analysis.

Parse errors
------------
`_parse_block` raises `DdlParseError` when sqlglot cannot produce a structured
AST (falls back to a raw `Command` node).  `_load_file` catches this per-block
and stores the entry with `ast=None` and `parse_error` populated — the
remaining blocks in the file continue loading.  No silent fallback is used.
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

_SEMICOLON_RE = re.compile(r";\s*(?:\n|$)")

_DELIMITER_MAP: dict[str, re.Pattern[str]] = {
    "tsql": _GO_RE,
    "snowflake": _SEMICOLON_RE,
    "spark": _SEMICOLON_RE,
}


def _read_manifest(ddl_path: Path) -> dict[str, str]:
    """Read manifest.json from ddl_path if present. Returns dialect, defaulting to tsql."""
    manifest_file = Path(ddl_path) / "manifest.json"
    if manifest_file.exists():
        import json as _json
        try:
            with manifest_file.open() as f:
                m = _json.load(f)
        except _json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json in {ddl_path} is not valid JSON: {exc}") from exc
        return {"dialect": m.get("dialect", "tsql")}
    return {"dialect": "tsql"}


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


_MIGRATE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
_SKIP_TYPES = (exp.TruncateTable, exp.Drop)


def classify_statement(stmt: Any) -> str:
    """Classify a parsed statement as migrate, skip, or claude.

    - migrate: core DML that becomes the dbt model (INSERT, UPDATE, DELETE, MERGE, SELECT INTO)
    - skip: operational DDL or session config (SET, TRUNCATE, DROP/CREATE INDEX, DECLARE)
    - claude: EXEC/dynamic SQL that needs Claude to resolve
    """
    if stmt is None:
        return "skip"
    if isinstance(stmt, exp.Command):
        return "claude"
    if isinstance(stmt, _MIGRATE_TYPES):
        return "migrate"
    if isinstance(stmt, exp.Select) and stmt.args.get("into"):
        return "migrate"
    if isinstance(stmt, _SKIP_TYPES):
        return "skip"
    if isinstance(stmt, exp.Set):
        return "skip"
    if isinstance(stmt, exp.Create):
        # CREATE PROCEDURE/VIEW/FUNCTION = migrate; CREATE INDEX/PARTITION = skip
        kind = stmt.args.get("kind", "")
        if kind and kind.upper() in ("PROCEDURE", "VIEW", "FUNCTION"):
            return "migrate"
        return "skip"
    return "skip"


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


# ── Parsing helpers ───────────────────────────────────────────────────────────


def _split_blocks(sql: str, delimiter_re: re.Pattern[str] = _GO_RE) -> list[str]:
    """Split SQL into individual object blocks using the given delimiter pattern."""
    blocks = delimiter_re.split(sql)
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

def _parse_body_statements(raw_ddl: str, dialect: str = "tsql") -> tuple[list[Any], bool]:
    """Parse procedure body with a single sqlglot pass.

    Returns (statements, needs_llm):
      - statements: successfully parsed AST nodes (partial — may miss
        control-flow-wrapped DML)
      - needs_llm: True if any statement was unparseable (Command/If nodes
        found), signalling that the LLM should read the raw DDL
    """
    m = _BODY_RE.search(raw_ddl)
    if not m:
        return [], False
    body = m.group(1).strip()
    if not body:
        return [], False

    stmts = sqlglot.parse(body, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    parsed: list[Any] = []
    needs_llm = False
    for s in stmts:
        if s is None:
            continue
        if isinstance(s, (exp.Command, exp.If)):
            needs_llm = True
            # Keep the node so tests can inspect its text, but don't
            # feed it into ref collection (Command/If are not walkable).
            parsed.append(s)
        else:
            parsed.append(s)
    return parsed, needs_llm


# Node type → operation name, used for write_operations mapping
_WRITE_NODE_TYPES: list[tuple[type, str]] = [
    (exp.Insert, "INSERT"),
    (exp.Update, "UPDATE"),
    (exp.Delete, "DELETE"),
    (exp.Merge, "MERGE"),
    (exp.TruncateTable, "TRUNCATE"),
    (exp.Into, "SELECT_INTO"),
]


def _collect_refs_from_statements(statements: list[Any], dialect: str = "tsql") -> ObjectRefs:
    """Walk a list of parsed statements and collect write/read/function references."""
    writes_to: set[str] = set()
    reads_from: set[str] = set()
    uses_functions: set[str] = set()
    write_ops: dict[str, list[str]] = {}
    stmt_list: list[dict[str, str]] = []

    def _add_write(fqn: str, op: str) -> None:
        writes_to.add(fqn)
        if fqn not in write_ops:
            write_ops[fqn] = []
        if op not in write_ops[fqn]:
            write_ops[fqn].append(op)

    for stmt in statements:
        # Classify and record every statement
        action = classify_statement(stmt)
        try:
            sql_text = stmt.sql(dialect=dialect)[:200] if stmt else ""
        except (ValueError, TypeError):
            sql_text = str(stmt)[:200] if stmt else ""
        stmt_list.append({
            "type": type(stmt).__name__ if stmt else "None",
            "action": action,
            "sql": sql_text,
        })

        if stmt is None or isinstance(stmt, exp.Command):
            continue

        # writes_to: INSERT targets
        for node in stmt.find_all(exp.Insert):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                _add_write(_table_fqn(target), "INSERT")

        # writes_to: UPDATE targets
        for node in stmt.find_all(exp.Update):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                _add_write(_table_fqn(target), "UPDATE")

        # writes_to: DELETE targets (use node.this — node.find(Table) may
        # return a TOP pseudo-table from DELETE TOP (N) syntax)
        for node in stmt.find_all(exp.Delete):
            target = node.this
            if isinstance(target, exp.Table) and _is_real_table(target):
                _add_write(_table_fqn(target), "DELETE")

        # writes_to: MERGE targets + reads_from: MERGE USING sources
        for node in stmt.find_all(exp.Merge):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                _add_write(_table_fqn(target), "MERGE")
            using = node.args.get("using")
            if isinstance(using, exp.Table) and _is_real_table(using):
                reads_from.add(_table_fqn(using))

        # writes_to: TRUNCATE targets
        for node in stmt.find_all(exp.TruncateTable):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                _add_write(_table_fqn(target), "TRUNCATE")

        # writes_to: SELECT INTO targets
        for node in stmt.find_all(exp.Into):
            target = node.find(exp.Table)
            if target and _is_real_table(target):
                _add_write(_table_fqn(target), "SELECT_INTO")

        # reads_from: FROM and JOIN sources
        for node in stmt.find_all(exp.From):
            table = node.find(exp.Table)
            if table and _is_real_table(table):
                reads_from.add(_table_fqn(table))

        for node in stmt.find_all(exp.Join):
            table = node.find(exp.Table)
            if table and _is_real_table(table):
                reads_from.add(_table_fqn(table))

        # uses_functions: schema-qualified function calls (Dot → Anonymous)
        for node in stmt.find_all(exp.Dot):
            left = node.args.get("this")
            right = node.args.get("expression")
            if isinstance(right, exp.Anonymous) and left is not None:
                schema = left.sql(dialect=dialect).strip()
                func_name = right.name
                if schema and func_name:
                    uses_functions.add(normalize(f"{schema}.{func_name}"))

    return ObjectRefs(
        writes_to=sorted(writes_to),
        reads_from=sorted(reads_from - writes_to),
        calls=[],
        uses_functions=sorted(uses_functions),
        write_operations=write_ops,
        statements=stmt_list,
    )


_EXEC_LINE_RE = re.compile(
    r"^\s*(?:EXEC(?:UTE)?)\b\s*(.+?);\s*$",
    re.IGNORECASE | re.MULTILINE,
)


def _add_exec_statements(raw_ddl: str, refs: ObjectRefs) -> None:
    """Scan raw DDL for EXEC statements and add them to refs.statements."""
    for m in _EXEC_LINE_RE.finditer(raw_ddl):
        exec_text = f"EXEC {m.group(1).strip()}"
        refs.statements.append({
            "type": "Command",
            "action": "claude",
            "sql": exec_text[:200],
        })


def extract_refs(entry: DdlEntry, dialect: str = "tsql") -> ObjectRefs:
    """Extract write/read/call references from a DDL entry.

    For procedures, views, and functions: extracts the body between
    AS BEGIN...END and parses each statement individually, since sqlglot's
    CREATE PROCEDURE AST only captures the first body statement.

    For tables and other CREATE statements: walks the original AST directly.

    Sets needs_llm=True when the procedure contains unparseable control
    flow (Command/If nodes) or EXEC/dynamic SQL — signalling that the
    LLM should read the raw DDL to complete analysis.

    Raises:
        DdlParseError: if the entry has no AST and no raw_ddl to fall back on.
    """
    has_exec = bool(_EXEC_RE.search(entry.raw_ddl))

    # For entries with AS BEGIN...END bodies, parse the body statements
    body_stmts, body_needs_llm = _parse_body_statements(entry.raw_ddl, dialect=dialect)
    if body_stmts:
        refs = _collect_refs_from_statements(body_stmts, dialect=dialect)
        refs.needs_llm = body_needs_llm or has_exec
        if has_exec:
            _add_exec_statements(entry.raw_ddl, refs)
        return refs

    # Fallback: walk the original AST (tables, simple views without BEGIN/END)
    if entry.ast is None:
        raise DdlParseError("Cannot extract refs: DDL block failed to parse (ast is None)")

    refs = _collect_refs_from_statements([entry.ast], dialect=dialect)
    refs.needs_llm = has_exec
    if has_exec:
        _add_exec_statements(entry.raw_ddl, refs)
    return refs


# ── Directory loading ─────────────────────────────────────────────────────────


def _load_file(
    path: Path,
    catalog: DdlCatalog,
    dialect: str = "tsql",
    delimiter_re: re.Pattern[str] = _GO_RE,
) -> None:
    """Parse a .sql file and route each block into the correct catalog bucket."""
    if not path.exists():
        return
    blocks = _split_blocks(path.read_text(encoding="utf-8"), delimiter_re=delimiter_re)
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

    If a manifest.json is present in ddl_path, the dialect declared there
    overrides the dialect parameter.

    Args:
        ddl_path: Path to directory containing .sql files.
        dialect:  sqlglot dialect for parsing (default: "tsql").

    Raises:
        FileNotFoundError: if ddl_path does not exist.
    """
    path = Path(ddl_path)
    if not path.exists():
        raise FileNotFoundError(f"DDL path does not exist: {path}")

    _manifest = _read_manifest(path)
    dialect = _manifest["dialect"]
    delimiter_re = _DELIMITER_MAP.get(dialect, _GO_RE)

    catalog = DdlCatalog()
    for sql_file in sorted(path.glob("*.sql")):
        _load_file(sql_file, catalog, dialect=dialect, delimiter_re=delimiter_re)
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
    _manifest = _read_manifest(Path(ddl_path))
    dialect = _manifest["dialect"]
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
