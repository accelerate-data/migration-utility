"""SQL block parsing and AST reference extraction for standalone DDL MCP support."""
from __future__ import annotations

import re
from typing import Any

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp

from ddl_mcp_support.block_segmenter import (
    BlockNode,
    IfNode,
    SegmentNode,
    SegmenterError,
    StatementNode,
    TryCatchNode,
    WhileNode,
    segment_sql,
)
from ddl_mcp_support.loader_data import DdlEntry, DdlParseError, ObjectRefs
from ddl_mcp_support.name_resolver import normalize
from ddl_mcp_support.routing import scan_routing_flags


GO_RE = re.compile(r"(?:^|\n)\s*GO\b", re.IGNORECASE)
_SEMICOLON_RE = re.compile(r";\s*(?:\n|$)")
_OBJECT_NAME_RE = re.compile(
    r"CREATE\s+"
    r"(?:OR\s+(?:ALTER|REPLACE)\s+)?"
    r"(?:(?:FORCE|EDITIONABLE|NONEDITIONABLE)\s+)*"
    r"(?:TABLE|PROCEDURE|PROC|VIEW|FUNCTION)\s+"
    r'((?:"?\[?\w+\]?"?\.){0,3}"?\[?\w+\]?"?)',
    re.IGNORECASE,
)
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
_BODY_RE = re.compile(
    r"\bAS\s+BEGIN\b(.*)\bEND\b(?:\s*;)?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_EXEC_LINE_RE = re.compile(
    r"^\s*(?:EXEC(?:UTE)?)\b\s*(.+?);\s*$",
    re.IGNORECASE | re.MULTILINE,
)

_MIGRATE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
_SKIP_TYPES = (exp.TruncateTable, exp.Drop)


def split_blocks(sql: str, delimiter_re: re.Pattern[str] = GO_RE) -> list[str]:
    return [b.strip() for b in delimiter_re.split(sql) if b.strip()]


def extract_name(block: str) -> str | None:
    m = _OBJECT_NAME_RE.search(block)
    return m.group(1) if m else None


def extract_type_bucket(block: str) -> str | None:
    m = _TYPE_KEYWORD_RE.search(block)
    if m:
        return _TYPE_MAP.get(m.group(1).lower())
    return None


def parse_block(block: str, dialect: str = "tsql") -> Any:
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


def _is_real_table(table: exp.Table) -> bool:
    return bool(table.db) and not table.catalog


def _table_fqn(table: exp.Table) -> str:
    return normalize(f"{table.db}.{table.name}")


def _strip_trailing_sql_comments(sql: str) -> str:
    end = len(sql)
    while True:
        while end > 0 and sql[end - 1].isspace():
            end -= 1

        if end >= 2 and sql[end - 2:end] == "*/":
            start = sql.rfind("/*", 0, end - 2)
            if start == -1:
                break
            end = start
            continue

        line_start = sql.rfind("\n", 0, end) + 1
        line = sql[line_start:end]
        line_comment_offset = line.find("--")
        comment_start = line_start + line_comment_offset if line_comment_offset != -1 else -1
        if comment_start != -1:
            prefix = sql[:comment_start]
            if prefix and not prefix[-1].isspace() and prefix[-1] != ";":
                break
            end = comment_start
            continue

        break

    return sql[:end].rstrip()


def parse_body_statements(raw_ddl: str, dialect: str = "tsql") -> tuple[list[Any], bool]:
    m = _BODY_RE.search(_strip_trailing_sql_comments(raw_ddl))
    if not m:
        return [], False
    body = m.group(1).strip()
    if not body:
        return [], False

    try:
        nodes = segment_sql(body)
    except SegmenterError:
        return [], True

    parsed: list[Any] = []
    needs_llm = False
    for leaf_sql in _flatten_leaf_sql(nodes):
        routing = scan_routing_flags(leaf_sql)
        routing_reasons = routing["routing_reasons"]
        if (
            routing["needs_llm"]
            or "static_exec" in routing_reasons
            or "dynamic_sql_literal" in routing_reasons
        ):
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


def classify_statement(stmt: Any) -> str:
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
        kind = stmt.args.get("kind", "")
        if kind and kind.upper() in ("PROCEDURE", "VIEW", "FUNCTION"):
            return "migrate"
        return "skip"
    return "skip"


def _collect_write_refs(stmt: Any, add_write: Any, reads_from: set[str]) -> None:
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
    for node in stmt.find_all(exp.From):
        table = node.find(exp.Table)
        if table and _is_real_table(table):
            reads_from.add(_table_fqn(table))

    for node in stmt.find_all(exp.Join):
        table = node.find(exp.Table)
        if table and _is_real_table(table):
            reads_from.add(_table_fqn(table))


def _collect_function_refs(stmt: Any, uses_functions: set[str], dialect: str) -> None:
    for node in stmt.find_all(exp.Dot):
        left = node.args.get("this")
        right = node.args.get("expression")
        if isinstance(right, exp.Anonymous) and left is not None:
            schema = left.sql(dialect=dialect).strip()
            func_name = right.name
            if schema and func_name:
                uses_functions.add(normalize(f"{schema}.{func_name}"))


def collect_refs_from_statements(statements: list[Any], dialect: str = "tsql") -> ObjectRefs:
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
        stmt_list.append(
            {
                "type": type(stmt).__name__ if stmt else "None",
                "action": action,
                "sql": sql_text,
            }
        )

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
    for m in _EXEC_LINE_RE.finditer(raw_ddl):
        exec_text = f"EXEC {m.group(1).strip()}"
        refs.statements.append(
            {
                "type": "Command",
                "action": "needs_llm",
                "sql": exec_text[:200],
            }
        )


def extract_refs(entry: DdlEntry, dialect: str = "tsql") -> ObjectRefs:
    routing = scan_routing_flags(entry.raw_ddl)
    has_exec = bool(_EXEC_RE.search(entry.raw_ddl))

    body_stmts, body_needs_llm = parse_body_statements(entry.raw_ddl, dialect=dialect)
    if body_stmts:
        refs = collect_refs_from_statements(body_stmts, dialect=dialect)
        refs.needs_llm = body_needs_llm or bool(routing["needs_llm"])
        if has_exec:
            _add_exec_statements(entry.raw_ddl, refs)
        return refs

    if entry.ast is None:
        raise DdlParseError("Cannot extract refs: DDL block failed to parse (ast is None)")

    refs = collect_refs_from_statements([entry.ast], dialect=dialect)
    refs.needs_llm = bool(routing["needs_llm"])
    if has_exec:
        _add_exec_statements(entry.raw_ddl, refs)
    return refs
