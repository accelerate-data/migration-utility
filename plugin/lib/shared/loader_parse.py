"""SQL block parsing and AST reference extraction.

Splits GO-delimited DDL into blocks, parses each with sqlglot, and extracts
write/read/function references from procedure bodies.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from typing import Any

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp

from shared.block_segmenter import (
    BlockNode,
    IfNode,
    SegmentNode,
    SegmenterError,
    StatementNode,
    TryCatchNode,
    WhileNode,
    segment_sql,
)
from shared.catalog import scan_routing_flags
from shared.loader_data import DdlEntry, DdlParseError, ObjectRefs
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

GO_RE = re.compile(r"(?:^|\n)\s*GO\b", re.IGNORECASE)

_OBJECT_NAME_RE = re.compile(
    r"CREATE\s+"
    r"(?:OR\s+(?:ALTER|REPLACE)\s+)?"              # OR ALTER (T-SQL) | OR REPLACE (Oracle)
    r"(?:(?:FORCE|EDITIONABLE|NONEDITIONABLE)\s+)*" # Oracle DBMS_METADATA modifiers
    r"(?:TABLE|PROCEDURE|PROC|VIEW|FUNCTION)\s+"
    r'((?:"?\[?\w+\]?"?\.){0,3}"?\[?\w+\]?"?)',    # [bracket] or "quoted" schema.name
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
    r"CREATE\s+"
    r"(?:OR\s+(?:ALTER|REPLACE)\s+)?"
    r"(?:(?:FORCE|EDITIONABLE|NONEDITIONABLE)\s+)*"
    r"(TABLE|PROCEDURE|PROC|VIEW|FUNCTION)\b",
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


def split_blocks(sql: str, delimiter_re: re.Pattern[str] = GO_RE) -> list[str]:
    """Split SQL into individual object blocks using the given delimiter pattern."""
    blocks = delimiter_re.split(sql)
    return [b.strip() for b in blocks if b.strip()]


def extract_name(block: str) -> str | None:
    m = _OBJECT_NAME_RE.search(block)
    return m.group(1) if m else None


def extract_type_bucket(block: str) -> str | None:
    m = _TYPE_KEYWORD_RE.search(block)
    if m:
        return _TYPE_MAP.get(m.group(1).lower())
    return None


def parse_block(block: str, dialect: str = "tsql") -> Any:
    """Parse a single DDL block with sqlglot and return the AST.

    Raises:
        DdlParseError: if sqlglot falls back to a `Command` node (could not
            parse the block as structured DDL).
    """
    try:
        result = sqlglot.parse_one(block, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
    except (sqlglot.errors.ParseError, sqlglot.errors.UnsupportedError, ValueError) as exc:
        name = extract_name(block) or "<unknown>"
        raise DdlParseError(f"sqlglot raised an exception parsing '{name}': {exc}") from exc

    if result is None or isinstance(result, exp.Command):
        name = extract_name(block) or "<unknown>"
        raise DdlParseError(
            f"sqlglot could not parse DDL block for '{name}' (fell back to Command)"
        )
    return result


# ── Statement classification ─────────────────────────────────────────────────


def classify_statement(stmt: Any) -> str:
    """Classify a parsed statement as migrate, skip, or needs_llm.

    - migrate: core DML that becomes the dbt model (INSERT, UPDATE, DELETE, MERGE, SELECT INTO)
    - skip: operational DDL or session config (SET, TRUNCATE, DROP/CREATE INDEX, DECLARE)
    - needs_llm: EXEC/dynamic SQL that needs LLM to resolve
    """
    if stmt is None:
        return "skip"
    if isinstance(stmt, exp.Command):
        if str(stmt.args.get("this", "")).upper() == "PRINT":
            return "skip"
        return "needs_llm"
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


def parse_body_statements(raw_ddl: str, dialect: str = "tsql") -> tuple[list[Any], bool]:
    """Parse procedure body, flattening recoverable control-flow branches.

    Returns (statements, needs_llm):
      - statements: successfully parsed AST nodes from recoverable leaf SQL
      - needs_llm: True if any leaf statement remains unresolved and requires
        LLM analysis
    """
    m = _BODY_RE.search(raw_ddl)
    if not m:
        return [], False
    body = m.group(1).strip()
    if not body:
        return [], False

    try:
        nodes = segment_sql(body)
    except SegmenterError as exc:
        logger.debug("event=segment_fallback reason=segmenter_error error=%s", exc)
        return [], True

    parsed: list[Any] = []
    needs_llm = False
    for leaf_sql in _flatten_leaf_sql(nodes):
        routing = scan_routing_flags(leaf_sql)
        if routing["needs_llm"] or "static_exec" in routing["routing_reasons"] or "dynamic_sql_literal" in routing["routing_reasons"]:
            if routing["needs_llm"]:
                needs_llm = True
            continue

        stmts = sqlglot.parse(leaf_sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN)
        for stmt in stmts:
            if stmt is None:
                continue
            if isinstance(stmt, exp.Command):
                needs_llm = True
                continue
            parsed.append(stmt)

    return parsed, needs_llm


def _flatten_leaf_sql(nodes: list[SegmentNode]) -> list[str]:
    leaf_sql: list[str] = []
    for node in nodes:
        if isinstance(node, StatementNode):
            if node.sql.strip():
                leaf_sql.append(node.sql.strip())
        elif isinstance(node, BlockNode):
            leaf_sql.extend(_flatten_leaf_sql(node.children))
        elif isinstance(node, IfNode):
            leaf_sql.extend(_flatten_leaf_sql(node.true_branch))
            leaf_sql.extend(_flatten_leaf_sql(node.false_branch))
        elif isinstance(node, WhileNode):
            leaf_sql.extend(_flatten_leaf_sql(node.body))
        elif isinstance(node, TryCatchNode):
            leaf_sql.extend(_flatten_leaf_sql(node.try_branch))
            leaf_sql.extend(_flatten_leaf_sql(node.catch_branch))
    return leaf_sql


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
        target = node.this
        if isinstance(target, exp.Table) and not _is_real_table(target):
            from_clause = node.args.get("from")
            if isinstance(from_clause, exp.From) and isinstance(from_clause.this, exp.Table):
                target = from_clause.this
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


def collect_refs_from_statements(statements: list[Any], dialect: str = "tsql") -> ObjectRefs:
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
            "action": "needs_llm",
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
    routing = scan_routing_flags(entry.raw_ddl)
    has_exec = bool(_EXEC_RE.search(entry.raw_ddl))

    # For entries with AS BEGIN...END bodies, parse the body statements
    body_stmts, body_needs_llm = parse_body_statements(entry.raw_ddl, dialect=dialect)
    if body_stmts:
        refs = collect_refs_from_statements(body_stmts, dialect=dialect)
        refs.needs_llm = body_needs_llm or routing["needs_llm"]
        if has_exec:
            _add_exec_statements(entry.raw_ddl, refs)
        return refs

    # Fallback: walk the original AST (tables, simple views without BEGIN/END)
    if entry.ast is None:
        raise DdlParseError("Cannot extract refs: DDL block failed to parse (ast is None)")

    refs = collect_refs_from_statements([entry.ast], dialect=dialect)
    refs.needs_llm = routing["needs_llm"]
    if has_exec:
        _add_exec_statements(entry.raw_ddl, refs)
    return refs
