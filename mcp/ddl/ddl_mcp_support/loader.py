"""Standalone DDL loader support for the DDL MCP package."""
from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp

from ddl_mcp_support.name_resolver import normalize

logger = logging.getLogger(__name__)

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
_CONTROL_FLOW_REASONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bIF\b", re.IGNORECASE), "if_else"),
    (re.compile(r"\bWHILE\b", re.IGNORECASE), "while_loop"),
    (re.compile(r"\bBEGIN\s+TRY\b", re.IGNORECASE), "try_catch"),
)
_SELECT_INTO_RE = re.compile(
    r"^(?!.*\bINSERT\b).*\bINTO\s+[\[\w#@]",
    re.IGNORECASE | re.MULTILINE,
)
_TRUNCATE_RE = re.compile(r"\bTRUNCATE\b", re.IGNORECASE)
_STATIC_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?(?!sp_executesql\b)(?!\()"
    r"(?:\[[^\]]+\]|\w+)(?:\s*\.\s*(?:\[[^\]]+\]|\w+)){0,3}",
    re.IGNORECASE,
)
DYNAMIC_EXEC_RE = re.compile(r"\bEXEC(?:UTE)?\s*\(", re.IGNORECASE)
_SP_EXECUTESQL_RE = re.compile(r"\bEXEC(?:UTE)?\s+sp_executesql\b", re.IGNORECASE)
_SP_EXECUTESQL_LITERAL_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+sp_executesql\s+N?'(?:''|[^'])*'",
    re.IGNORECASE | re.DOTALL,
)
_SP_EXECUTESQL_VARIABLE_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+sp_executesql\s+@",
    re.IGNORECASE,
)
_CROSS_DB_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?"
    r"(?!sp_executesql\b)"
    r"(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)"
    r"(?!\s*\.)",
    re.IGNORECASE,
)
_LINKED_SERVER_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:@\w+\s*=\s*)?"
    r"(?!sp_executesql\b)"
    r"(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)\s*\.\s*(?:\[[^\]]+\]|\w+)",
    re.IGNORECASE,
)
_DELIMITER_MAP: dict[str, re.Pattern[str]] = {
    "tsql": GO_RE,
    "snowflake": _SEMICOLON_RE,
    "spark": _SEMICOLON_RE,
}
_STATEMENT_STARTERS = {
    "BEGIN",
    "WITH",
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "TRUNCATE",
    "DROP",
    "CREATE",
    "ALTER",
    "DECLARE",
    "SET",
    "RETURN",
    "PRINT",
    "EXEC",
    "EXECUTE",
    "IF",
    "WHILE",
}
_MIGRATE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge)
_SKIP_TYPES = (exp.TruncateTable, exp.Drop)


class DdlParseError(Exception):
    """Raised when sqlglot cannot parse a DDL block or extract references."""


class SegmenterError(ValueError):
    """Raised when the segmenter cannot classify a control-flow structure."""


class SegmenterLimitError(SegmenterError):
    """Raised when recursion or node-count guardrails are exceeded."""


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


@dataclass(frozen=True)
class StatementNode:
    sql: str


@dataclass(frozen=True)
class BlockNode:
    children: list["SegmentNode"]


@dataclass(frozen=True)
class IfNode:
    condition_sql: str
    true_branch: list["SegmentNode"]
    false_branch: list["SegmentNode"]


@dataclass(frozen=True)
class WhileNode:
    condition_sql: str
    body: list["SegmentNode"]


@dataclass(frozen=True)
class TryCatchNode:
    try_branch: list["SegmentNode"]
    catch_branch: list["SegmentNode"]


SegmentNode = StatementNode | BlockNode | IfNode | WhileNode | TryCatchNode


def mask_tsql(sql: str, *, mask_bracketed_identifiers: bool = True) -> str:
    chars = list(sql)
    i = 0
    while i < len(chars):
        ch = chars[i]
        nxt = chars[i + 1] if i + 1 < len(chars) else ""

        if ch == "'":
            chars[i] = " "
            i += 1
            while i < len(chars):
                if chars[i] == "'":
                    chars[i] = " "
                    if i + 1 < len(chars) and chars[i + 1] == "'":
                        chars[i + 1] = " "
                        i += 2
                        continue
                    i += 1
                    break
                chars[i] = " "
                i += 1
            continue

        if mask_bracketed_identifiers and ch == "[":
            chars[i] = " "
            i += 1
            while i < len(chars):
                chars[i] = " "
                if sql[i] == "]":
                    i += 1
                    break
                i += 1
            continue

        if ch == "-" and nxt == "-":
            chars[i] = chars[i + 1] = " "
            i += 2
            while i < len(chars) and chars[i] != "\n":
                chars[i] = " "
                i += 1
            continue

        if ch == "/" and nxt == "*":
            chars[i] = chars[i + 1] = " "
            i += 2
            while i < len(chars):
                chars[i] = " "
                if i + 1 < len(chars) and sql[i] == "*" and sql[i + 1] == "/":
                    chars[i + 1] = " "
                    i += 2
                    break
                i += 1
            continue

        i += 1

    return "".join(chars)


def scan_routing_flags(definition: str) -> dict[str, bool | str | list[str]]:
    masked = mask_tsql(definition)
    reasons: list[str] = []

    for pattern, reason in _CONTROL_FLOW_REASONS:
        if pattern.search(masked):
            reasons.append(reason)

    has_dynamic_exec = bool(DYNAMIC_EXEC_RE.search(masked))
    has_sp_executesql_literal = bool(_SP_EXECUTESQL_LITERAL_RE.search(masked))
    has_sp_executesql_variable = bool(_SP_EXECUTESQL_VARIABLE_RE.search(masked))
    has_static_exec = bool(_STATIC_EXEC_RE.search(definition))
    has_select_into = bool(_SELECT_INTO_RE.search(masked))
    has_truncate = bool(_TRUNCATE_RE.search(masked))
    has_linked_server_exec = bool(_LINKED_SERVER_EXEC_RE.search(definition))
    has_cross_db_exec = bool(_CROSS_DB_EXEC_RE.search(definition)) and not has_linked_server_exec

    if has_dynamic_exec or has_sp_executesql_variable:
        reasons.append("dynamic_sql_variable")
    elif has_sp_executesql_literal or _SP_EXECUTESQL_RE.search(masked):
        reasons.append("dynamic_sql_literal")

    if has_static_exec:
        reasons.append("static_exec")
    if has_cross_db_exec:
        reasons.append("cross_db_exec")
    if has_linked_server_exec:
        reasons.append("linked_server_exec")

    routing_reasons = sorted(set(reasons))
    needs_llm = "dynamic_sql_variable" in routing_reasons
    needs_enrich = has_static_exec or has_select_into or has_truncate

    if needs_llm:
        mode = "llm_required"
    elif "dynamic_sql_literal" in routing_reasons:
        mode = "dynamic_sql_literal"
    elif "static_exec" in routing_reasons and needs_enrich:
        mode = "call_graph_enrich"
    elif any(reason in routing_reasons for reason in ("if_else", "while_loop", "try_catch")):
        mode = "control_flow_fallback"
    else:
        mode = "deterministic"

    return {
        "needs_llm": needs_llm,
        "needs_enrich": needs_enrich,
        "mode": mode,
        "routing_reasons": routing_reasons,
    }


class _Parser:
    def __init__(self, sql: str, *, max_depth: int, max_nodes: int) -> None:
        self.sql = sql
        self.masked = mask_tsql(sql)
        self.upper = self.masked.upper()
        self.max_depth = max_depth
        self.max_nodes = max_nodes
        self.node_count = 0

    def parse(self) -> list[SegmentNode]:
        nodes, pos = self._parse_segments(0, end_keywords=(), depth=0)
        pos = self._skip_ws(pos)
        if pos != len(self.sql):
            raise SegmenterError(f"Unexpected trailing SQL at index {pos}")
        return nodes

    def _parse_segments(
        self, pos: int, *, end_keywords: tuple[str, ...], depth: int,
    ) -> tuple[list[SegmentNode], int]:
        if depth > self.max_depth:
            raise SegmenterLimitError("maximum control-flow nesting depth exceeded")

        nodes: list[SegmentNode] = []
        while True:
            pos = self._skip_ws(pos)
            if pos >= len(self.sql):
                return nodes, pos
            if any(self._match_keyword(pos, keyword) for keyword in end_keywords):
                return nodes, pos
            unit_nodes, pos = self._parse_unit(pos, depth=depth)
            nodes.extend(unit_nodes)

    def _parse_unit(self, pos: int, *, depth: int) -> tuple[list[SegmentNode], int]:
        return self._parse_node(pos, depth=depth, stop_keywords=())

    def _parse_if(self, pos: int, *, depth: int) -> tuple[IfNode, int]:
        branch_start = self._find_branch_start(pos + len("IF"))
        condition_sql = self.sql[pos + len("IF"):branch_start].strip()
        true_branch, next_pos = self._parse_branch(branch_start, depth=depth + 1)
        next_pos = self._skip_ws(next_pos)
        false_branch: list[SegmentNode] = []
        if self._match_keyword(next_pos, "ELSE"):
            false_branch, next_pos = self._parse_branch(next_pos + len("ELSE"), depth=depth + 1)
        return self._record(IfNode(condition_sql=condition_sql, true_branch=true_branch, false_branch=false_branch)), next_pos

    def _parse_while(self, pos: int, *, depth: int) -> tuple[WhileNode, int]:
        branch_start = self._find_branch_start(pos + len("WHILE"))
        condition_sql = self.sql[pos + len("WHILE"):branch_start].strip()
        body, next_pos = self._parse_branch(branch_start, depth=depth + 1)
        return self._record(WhileNode(condition_sql=condition_sql, body=body)), next_pos

    def _parse_try_catch(self, pos: int, *, depth: int) -> tuple[TryCatchNode, int]:
        try_body_start = self._skip_ws(pos + len("BEGIN TRY"))
        try_branch, next_pos = self._parse_segments(try_body_start, end_keywords=("END TRY",), depth=depth + 1)
        if not self._match_phrase(next_pos, ("END", "TRY")):
            raise SegmenterError("BEGIN TRY block missing END TRY")
        catch_start = self._skip_ws(next_pos + len("END TRY"))
        if not self._match_phrase(catch_start, ("BEGIN", "CATCH")):
            raise SegmenterError("BEGIN TRY block missing BEGIN CATCH")
        catch_body_start = self._skip_ws(catch_start + len("BEGIN CATCH"))
        catch_branch, end_pos = self._parse_segments(catch_body_start, end_keywords=("END CATCH",), depth=depth + 1)
        if not self._match_phrase(end_pos, ("END", "CATCH")):
            raise SegmenterError("BEGIN CATCH block missing END CATCH")
        return self._record(TryCatchNode(try_branch=try_branch, catch_branch=catch_branch)), end_pos + len("END CATCH")

    def _parse_begin_block(self, pos: int, *, depth: int) -> tuple[BlockNode, int]:
        body_start = self._skip_ws(pos + len("BEGIN"))
        children, end_pos = self._parse_segments(body_start, end_keywords=("END",), depth=depth + 1)
        if not self._match_keyword(end_pos, "END"):
            raise SegmenterError("BEGIN block missing END")
        return self._record(BlockNode(children=children)), end_pos + len("END")

    def _parse_branch(self, pos: int, *, depth: int) -> tuple[list[SegmentNode], int]:
        return self._parse_node(pos, depth=depth, stop_keywords=("ELSE", "END"))

    def _parse_node(
        self, pos: int, *, depth: int, stop_keywords: tuple[str, ...],
    ) -> tuple[list[SegmentNode], int]:
        pos = self._skip_ws(pos)
        if self._match_phrase(pos, ("BEGIN", "TRY")):
            node, end = self._parse_try_catch(pos, depth=depth)
            return [node], end
        if self._match_keyword(pos, "IF"):
            node, end = self._parse_if(pos, depth=depth)
            return [node], end
        if self._match_keyword(pos, "WHILE"):
            node, end = self._parse_while(pos, depth=depth)
            return [node], end
        if self._match_keyword(pos, "BEGIN"):
            node, end = self._parse_begin_block(pos, depth=depth)
            return [node], end
        node, end = self._parse_statement(pos, stop_keywords=stop_keywords)
        return [node], end

    def _parse_statement(
        self, pos: int, *, stop_keywords: tuple[str, ...],
    ) -> tuple[StatementNode, int]:
        i = pos
        paren_depth = 0
        case_depth = 0
        while i < len(self.sql):
            ch = self.masked[i]
            if ch == "(":
                paren_depth += 1
                i += 1
                continue
            if ch == ")" and paren_depth > 0:
                paren_depth -= 1
                i += 1
                continue
            if paren_depth == 0:
                word = self._read_word(i)
                if word == "CASE":
                    case_depth += 1
                    i += len("CASE")
                    continue
                if word == "END" and case_depth > 0:
                    case_depth -= 1
                    i += len("END")
                    continue
                if self.masked[i] == ";":
                    text = self.sql[pos:i].strip()
                    return self._record(StatementNode(sql=text)), i + 1
                if case_depth == 0 and any(self._match_keyword(i, keyword) for keyword in stop_keywords):
                    break
            i += 1
        text = self.sql[pos:i].strip()
        if not text:
            raise SegmenterError(f"Empty statement at index {pos}")
        return self._record(StatementNode(sql=text)), i

    def _find_branch_start(self, pos: int) -> int:
        i = self._skip_ws(pos)
        paren_depth = 0
        while i < len(self.sql):
            ch = self.masked[i]
            if ch == "(":
                paren_depth += 1
                i += 1
                continue
            if ch == ")" and paren_depth > 0:
                paren_depth -= 1
                i += 1
                continue
            if paren_depth == 0:
                word = self._read_word(i)
                if word and word in _STATEMENT_STARTERS:
                    return i
            i += 1
        raise SegmenterError(f"Could not find control-flow branch start at index {pos}")

    def _read_word(self, pos: int) -> str:
        pos = self._skip_ws(pos)
        start = pos
        while pos < len(self.upper) and (self.upper[pos].isalnum() or self.upper[pos] == "_"):
            pos += 1
        return self.upper[start:pos]

    def _skip_ws(self, pos: int) -> int:
        while pos < len(self.sql) and self.masked[pos].isspace():
            pos += 1
        while pos < len(self.sql) and self.masked[pos] == ";":
            pos += 1
            while pos < len(self.sql) and self.masked[pos].isspace():
                pos += 1
        return pos

    def _match_keyword(self, pos: int, keyword: str) -> bool:
        pos = self._skip_ws(pos)
        end = pos + len(keyword)
        if self.upper[pos:end] != keyword:
            return False
        before_ok = pos == 0 or not (self.upper[pos - 1].isalnum() or self.upper[pos - 1] == "_")
        after_ok = end >= len(self.upper) or not (self.upper[end].isalnum() or self.upper[end] == "_")
        return before_ok and after_ok

    def _match_phrase(self, pos: int, words: tuple[str, ...]) -> bool:
        pos = self._skip_ws(pos)
        for word in words:
            if not self._match_keyword(pos, word):
                return False
            pos = self._skip_ws(pos + len(word))
        return True

    def _record(self, node: SegmentNode) -> SegmentNode:
        self.node_count += 1
        if self.node_count > self.max_nodes:
            raise SegmenterLimitError("maximum segmenter node count exceeded")
        return node


def segment_sql(sql: str, *, max_depth: int = 20, max_nodes: int = 500) -> list[SegmentNode]:
    return _Parser(sql, max_depth=max_depth, max_nodes=max_nodes).parse()


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


def read_manifest(project_root: Path) -> dict[str, Any]:
    manifest_file = Path(project_root) / "manifest.json"
    if manifest_file.exists():
        try:
            with manifest_file.open(encoding="utf-8") as f:
                m = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f"manifest.json in {project_root} is not valid JSON: {exc}") from exc
        m["dialect"] = m.get("dialect", "tsql")
        return m
    return {"dialect": "tsql"}


def _resolve_ddl_dir(project_root: Path) -> Path:
    raw = os.environ.get("DDL_DIR", "").strip()
    return Path(raw) if raw else project_root / "ddl"


def _load_file(
    path: Path,
    catalog: DdlCatalog,
    dialect: str = "tsql",
    delimiter_re: re.Pattern[str] = GO_RE,
) -> None:
    if not path.exists():
        return
    blocks = split_blocks(path.read_text(encoding="utf-8"), delimiter_re=delimiter_re)
    for block in blocks:
        raw_name = extract_name(block)
        if not raw_name:
            logger.warning("event=load_file operation=extract_name file=%s reason=no_name_found", path.name)
            continue
        bucket_name = extract_type_bucket(block)
        if not bucket_name:
            logger.warning("event=load_file operation=extract_type file=%s object=%s reason=unknown_type", path.name, raw_name)
            continue
        key = normalize(raw_name)
        try:
            ast = parse_block(block, dialect=dialect)
            unsupported: list[str] = []
            for node in ast.walk():
                if isinstance(node, exp.Command):
                    unsupported.append(str(node)[:200])
            getattr(catalog, bucket_name)[key] = DdlEntry(
                raw_ddl=block,
                ast=ast,
                unsupported_syntax_nodes=unsupported if unsupported else None,
            )
        except DdlParseError as exc:
            logger.warning("event=load_file operation=parse_block file=%s object=%s error=%s", path.name, raw_name, exc)
            getattr(catalog, bucket_name)[key] = DdlEntry(raw_ddl=block, ast=None, parse_error=str(exc))


def load_directory(project_root: Path | str, dialect: str = "tsql") -> DdlCatalog:
    path = Path(project_root)
    if not path.exists():
        raise FileNotFoundError(f"Project root does not exist: {path}")

    ddl_dir = _resolve_ddl_dir(path)
    if not ddl_dir.is_dir():
        raise FileNotFoundError(f"ddl/ subdirectory does not exist: {ddl_dir}")

    manifest = read_manifest(path)
    dialect = manifest["dialect"]
    delimiter_re = _DELIMITER_MAP.get(dialect, GO_RE)

    catalog = DdlCatalog()
    for sql_file in sorted(ddl_dir.glob("*.sql")):
        _load_file(sql_file, catalog, dialect=dialect, delimiter_re=delimiter_re)
    return catalog
