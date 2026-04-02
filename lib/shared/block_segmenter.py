"""T-SQL-aware control-flow segmenter for procedure bodies.

This scanner only identifies control-flow block boundaries and leaf statement
slices. It does not try to parse T-SQL semantics.
"""

from __future__ import annotations

from dataclasses import dataclass


class SegmenterError(ValueError):
    """Raised when the segmenter cannot classify a control-flow structure."""


class SegmenterLimitError(SegmenterError):
    """Raised when recursion or node-count guardrails are exceeded."""


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


def _mask_tsql(sql: str) -> str:
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
        if ch == "[":
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
            while i < len(chars) - 1:
                if sql[i] == "*" and sql[i + 1] == "/":
                    chars[i] = chars[i + 1] = " "
                    i += 2
                    break
                chars[i] = " "
                i += 1
            continue
        i += 1
    return "".join(chars)


class _Parser:
    def __init__(self, sql: str, *, max_depth: int, max_nodes: int) -> None:
        self.sql = sql
        self.masked = _mask_tsql(sql)
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
        node, end = self._parse_statement(pos, stop_keywords=())
        return [node], end

    def _parse_if(self, pos: int, *, depth: int) -> tuple[IfNode, int]:
        branch_start = self._find_branch_start(pos + 2)
        condition_sql = self.sql[pos + 2:branch_start].strip()
        true_branch, next_pos = self._parse_branch(branch_start, depth=depth + 1)
        next_pos = self._skip_ws(next_pos)
        false_branch: list[SegmentNode] = []
        if self._match_keyword(next_pos, "ELSE"):
            false_branch, next_pos = self._parse_branch(next_pos + 4, depth=depth + 1)
        return self._record(IfNode(condition_sql=condition_sql, true_branch=true_branch, false_branch=false_branch)), next_pos

    def _parse_while(self, pos: int, *, depth: int) -> tuple[WhileNode, int]:
        branch_start = self._find_branch_start(pos + 5)
        condition_sql = self.sql[pos + 5:branch_start].strip()
        body, next_pos = self._parse_branch(branch_start, depth=depth + 1)
        return self._record(WhileNode(condition_sql=condition_sql, body=body)), next_pos

    def _parse_try_catch(self, pos: int, *, depth: int) -> tuple[TryCatchNode, int]:
        node, end = self._parse_try_catch_end(pos, depth=depth)
        return node, end

    def _parse_try_catch_end(self, pos: int, *, depth: int) -> tuple[TryCatchNode, int]:
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
        node, end = self._parse_statement(pos, stop_keywords=("ELSE", "END"))
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
    """Segment a procedure body into control-flow and statement nodes."""
    return _Parser(sql, max_depth=max_depth, max_nodes=max_nodes).parse()
