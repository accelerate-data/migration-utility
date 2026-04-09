"""routing.py — T-SQL routing flag detection for stored procedures.

Extracted from ``catalog.py`` to break the circular import chain:
``loader_parse`` → ``catalog`` → ``catalog_models``.

Import routing utilities from here instead of ``catalog``.
"""

from __future__ import annotations

import re

from shared.tsql_utils import mask_tsql

# ── Routing flag patterns ────────────────────────────────────────────────────

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

# Broader pattern that also catches EXEC @var and sp_executesql.
# Used by catalog_enrich.py to skip dynamic SQL during EXEC call extraction.
DYNAMIC_EXEC_BROAD_RE = re.compile(
    r"\bEXEC(?:UTE)?\s*[(@]|"
    r"\bsp_executesql\b",
    re.IGNORECASE,
)
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


def scan_routing_flags(definition: str) -> dict[str, bool]:
    """Scan a proc/view/function body and return routing summary fields.

    Returns the backward-compatible flags plus the canonical routing summary:
    ``{"needs_llm", "needs_enrich", "mode", "routing_reasons"}``.
    """
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
