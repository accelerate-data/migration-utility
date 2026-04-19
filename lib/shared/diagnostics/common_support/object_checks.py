"""Per-object cross-dialect diagnostic checks."""

from __future__ import annotations

import logging
from typing import Any

from shared.diagnostics import CatalogContext, DiagnosticResult, _THRESHOLDS, diagnostic

logger = logging.getLogger(__name__)


def _has_llm_recovery_statements(catalog_data: dict[str, Any]) -> bool:
    """Return True when persisted LLM statements prove parse recovery succeeded."""
    statements = catalog_data.get("statements")
    if not isinstance(statements, list):
        return False
    return any(
        isinstance(stmt, dict) and stmt.get("source") == "llm"
        for stmt in statements
    )


@diagnostic(
    code="PARSE_ERROR",
    objects=["view", "function", "procedure"],
    severity="error",
    pass_number=1,
)
def check_parse_error(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag objects whose DDL failed to parse."""
    if ctx.ddl_entry is None:
        return None
    err = ctx.ddl_entry.parse_error
    if not err:
        return None
    if ctx.object_type == "procedure" and _has_llm_recovery_statements(ctx.catalog_data):
        logger.info(
            "event=parse_error_suppressed component=diagnostics operation=check_parse_error object=%s status=success",
            ctx.fqn,
        )
        return None
    return DiagnosticResult(
        code="PARSE_ERROR",
        message=f"DDL failed to parse: {err[:200]}",
        severity="error",
        details={"parse_error": err},
    )


@diagnostic(
    code="UNSUPPORTED_SYNTAX",
    objects=["view", "function", "procedure"],
    severity="warning",
    pass_number=1,
)
def check_unsupported_syntax(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag objects with nested Command nodes (opaque subtrees) in the AST."""
    if ctx.ddl_entry is None:
        return None
    nodes = ctx.ddl_entry.unsupported_syntax_nodes
    if not nodes:
        return None
    return [
        DiagnosticResult(
            code="UNSUPPORTED_SYNTAX",
            message=f"Unsupported SQL construct: {text[:100]}",
            severity="warning",
            details={"command_text": text},
        )
        for text in nodes
    ]


@diagnostic(
    code="STALE_OBJECT",
    objects=["view", "function", "procedure"],
    severity="warning",
    pass_number=1,
)
def check_stale_object(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag objects marked stale after re-extraction."""
    if not ctx.catalog_data.get("stale"):
        return None
    details: dict[str, Any] = {}
    ddl_hash = ctx.catalog_data.get("ddl_hash")
    if ddl_hash:
        details["previous_ddl_hash"] = ddl_hash
    return DiagnosticResult(
        code="STALE_OBJECT",
        message="Object was present in a prior extraction but absent in the latest.",
        severity="warning",
        details=details if details else None,
    )


@diagnostic(
    code="MULTI_TABLE_WRITE",
    objects=["procedure"],
    severity="warning",
    pass_number=1,
)
def check_multi_table_write(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag procedures that write to more than one table."""
    refs = ctx.catalog_data.get("references", {})
    in_scope = refs.get("tables", {}).get("in_scope", [])
    updated_tables = [t for t in in_scope if t.get("is_updated")]
    if len(updated_tables) <= 1:
        return None
    table_fqns = [f"{t['schema']}.{t['name']}" for t in updated_tables]
    return DiagnosticResult(
        code="MULTI_TABLE_WRITE",
        message=f"Procedure writes to {len(updated_tables)} tables: {', '.join(table_fqns)}. Each table will require a separate dbt model.",
        severity="warning",
        details={"tables": table_fqns},
    )


@diagnostic(
    code="MULTI_TABLE_READ",
    objects=["function"],
    severity="warning",
    pass_number=1,
)
def check_multi_table_read(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag functions that read from many distinct tables."""
    threshold = _THRESHOLDS["MULTI_TABLE_READ_COUNT"]
    refs = ctx.catalog_data.get("references", {})
    in_scope = refs.get("tables", {}).get("in_scope", [])
    if len(in_scope) < threshold:
        return None
    table_fqns = [f"{t['schema']}.{t['name']}" for t in in_scope]
    return DiagnosticResult(
        code="MULTI_TABLE_READ",
        message=f"Function reads from {len(in_scope)} tables (threshold: {threshold}).",
        severity="warning",
        details={"table_count": len(in_scope), "tables": table_fqns},
    )
