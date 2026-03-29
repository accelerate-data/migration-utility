"""SQL block parsing and AST reference extraction.

Splits GO-delimited DDL into blocks, parses each with sqlglot, and extracts
write/read/function references from procedure bodies.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

import sqlglot
import sqlglot.expressions as exp

from shared.loader_data import DdlEntry, DdlParseError, ObjectRefs
from shared.name_resolver import normalize


# ── Constants ─────────────────────────────────────────────────────────────────

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

_EXEC_RE = re.compile(r"\b(?:EXEC|EXECUTE)\b", re.IGNORECASE)

_MIGRATE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
_SKIP_TYPES = (exp.TruncateTable, exp.Drop)

_BODY_RE = re.compile(
    r"\bAS\s+BEGIN\b(.*)\bEND\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_EXEC_LINE_RE = re.compile(
    r"^\s*(?:EXEC(?:UTE)?)\b\s*(.+?);\s*$",
    re.IGNORECASE | re.MULTILINE,
)


# ── Block splitting + parsing ────────────────────────────────────────────────


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


# ── Statement classification ─────────────────────────────────────────────────


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


# ── Reference extraction ─────────────────────────────────────────────────────


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


def _collect_write_refs(
    stmt: Any, add_write: Callable[[str, str], None], reads_from: set[str],
) -> None:
    """Extract write targets (INSERT/UPDATE/DELETE/MERGE/TRUNCATE/SELECT INTO) from a statement."""
    for node in stmt.find_all(exp.Insert):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            add_write(_table_fqn(target), "INSERT")

    for node in stmt.find_all(exp.Update):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            add_write(_table_fqn(target), "UPDATE")

    # DELETE: use node.this — node.find(Table) may return a TOP pseudo-table
    for node in stmt.find_all(exp.Delete):
        target = node.this
        if isinstance(target, exp.Table) and _is_real_table(target):
            add_write(_table_fqn(target), "DELETE")

    for node in stmt.find_all(exp.Merge):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            add_write(_table_fqn(target), "MERGE")
        using = node.args.get("using")
        if isinstance(using, exp.Table) and _is_real_table(using):
            reads_from.add(_table_fqn(using))

    for node in stmt.find_all(exp.TruncateTable):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            add_write(_table_fqn(target), "TRUNCATE")

    for node in stmt.find_all(exp.Into):
        target = node.find(exp.Table)
        if target and _is_real_table(target):
            add_write(_table_fqn(target), "SELECT_INTO")


def _collect_read_refs(stmt: Any, reads_from: set[str]) -> None:
    """Extract read sources (FROM/JOIN) from a statement."""
    for node in stmt.find_all(exp.From):
        table = node.find(exp.Table)
        if table and _is_real_table(table):
            reads_from.add(_table_fqn(table))

    for node in stmt.find_all(exp.Join):
        table = node.find(exp.Table)
        if table and _is_real_table(table):
            reads_from.add(_table_fqn(table))


def _collect_function_refs(stmt: Any, uses_functions: set[str], dialect: str) -> None:
    """Extract schema-qualified function calls (Dot → Anonymous) from a statement."""
    for node in stmt.find_all(exp.Dot):
        left = node.args.get("this")
        right = node.args.get("expression")
        if isinstance(right, exp.Anonymous) and left is not None:
            schema = left.sql(dialect=dialect).strip()
            func_name = right.name
            if schema and func_name:
                uses_functions.add(normalize(f"{schema}.{func_name}"))


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

        _collect_write_refs(stmt, _add_write, reads_from)
        _collect_read_refs(stmt, reads_from)
        _collect_function_refs(stmt, uses_functions, dialect)

    return ObjectRefs(
        writes_to=sorted(writes_to),
        reads_from=sorted(reads_from - writes_to),
        calls=[],
        uses_functions=sorted(uses_functions),
        write_operations=write_ops,
        statements=stmt_list,
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
