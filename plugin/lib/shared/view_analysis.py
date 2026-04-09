"""view_analysis.py — SQL element extraction from view ASTs.

Analyzes CREATE VIEW statements using sqlglot to extract structural
elements (JOINs, CTEs, aggregations, window functions, etc.) for
downstream profiling and migration decisions.

Split from discover.py for module focus.
"""

from __future__ import annotations

from typing import Any

import sqlglot.errors
import sqlglot.expressions as exp

from shared.loader_data import DdlEntry


def _analyze_view_select(entry: DdlEntry) -> dict[str, Any]:
    """Extract SQL elements from a view's AST. Returns sql_elements and errors.

    Views are always deterministic SELECT statements — there is no needs_llm routing.
    If parse_error is set or the AST walk fails, sql_elements is null and errors contains
    a DDL_PARSE_ERROR entry. The skill falls back to raw_ddl in that case.
    """
    if entry.parse_error or entry.ast is None:
        return {
            "sql_elements": None,
            "errors": [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": entry.parse_error or "AST is None"}],
        }

    try:
        elements: list[dict[str, Any]] = []
        ast = entry.ast

        # JOINs — one element per unique (join_type, target) pair
        seen_joins: set[str] = set()
        for join in ast.find_all(exp.Join):
            join_type = "JOIN"
            if join.args.get("kind"):
                kind = str(join.args["kind"]).upper()
                side = str(join.args.get("side", "")).upper()
                join_type = f"{side} {kind}".strip() if side else kind
            table = join.this
            target = table.name if hasattr(table, "name") else str(table)
            if table.args.get("db"):
                target = f"{table.args['db']}.{target}"
            detail = f"{join_type} {target}"
            if detail not in seen_joins:
                seen_joins.add(detail)
                elements.append({"type": "join", "detail": detail})

        # CTEs (WITH clause) — use direct children to avoid counting nested subquery CTEs
        for cte_node in ast.find_all(exp.With):
            cte_count = len(cte_node.expressions)
            elements.append({"type": "cte", "detail": f"{cte_count} CTE(s)"})
            break  # only report the outermost WITH clause

        # GROUP BY
        for _ in ast.find_all(exp.Group):
            elements.append({"type": "group_by", "detail": "GROUP BY"})
            break

        # Aggregation functions
        agg_funcs: list[str] = []
        for agg in ast.find_all(exp.AggFunc):
            name = type(agg).__name__.upper()
            if name not in agg_funcs:
                agg_funcs.append(name)
        if agg_funcs:
            elements.append({"type": "aggregation", "detail": ", ".join(sorted(agg_funcs))})

        # Window functions (OVER)
        for _ in ast.find_all(exp.Window):
            elements.append({"type": "window_function", "detail": "OVER clause"})
            break

        # CASE expressions
        for _ in ast.find_all(exp.Case):
            elements.append({"type": "case", "detail": "CASE expression"})
            break

        # Subqueries — exclude CTE bodies (they are Subquery nodes but not inline subqueries)
        subquery_count = sum(
            1 for node in ast.find_all(exp.Subquery)
            if not isinstance(node.parent, exp.CTE)
        )
        if subquery_count:
            elements.append({"type": "subquery", "detail": f"{subquery_count} subquery(ies)"})

        return {"sql_elements": elements, "errors": []}

    except (sqlglot.errors.SqlglotError, AttributeError, TypeError) as exc:
        return {
            "sql_elements": None,
            "errors": [{"code": "DDL_PARSE_ERROR", "severity": "error", "message": str(exc)}],
        }
