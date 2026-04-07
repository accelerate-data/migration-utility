"""Oracle dialect-specific diagnostic checks."""

from __future__ import annotations

import logging
import re

from shared.diagnostics import CatalogContext, DiagnosticResult, diagnostic

logger = logging.getLogger(__name__)


# ── Pass 1: Oracle-specific checks ──────────────────────────────────────────


@diagnostic(
    code="LONG_TRUNCATION",
    objects=["view"],
    dialects=("oracle",),
    severity="error",
    pass_number=1,
)
def check_long_truncation(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag views whose DDL was truncated by the Oracle LONG column limit."""
    if not ctx.catalog_data.get("long_truncation", False):
        return None
    return DiagnosticResult(
        code="LONG_TRUNCATION",
        message="View DDL was truncated by Oracle LONG column limit (32,767 bytes). References may be incomplete.",
        severity="error",
        details={},
    )


@diagnostic(
    code="PACKAGE_MEMBER",
    objects=["view", "function", "procedure"],
    dialects=("oracle",),
    severity="warning",
    pass_number=1,
)
def check_package_member(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag references to Oracle package members, which cannot be extracted standalone."""
    if ctx.package_members is None:
        return None

    results: list[DiagnosticResult] = []
    refs = ctx.catalog_data.get("references", {})

    for bucket in ("tables", "views", "functions", "procedures", "materialized_views"):
        in_scope = refs.get(bucket, {}).get("in_scope", [])
        for entry in in_scope:
            ref_fqn = f"{entry['schema']}.{entry['name']}".lower()
            if ref_fqn in ctx.package_members:
                results.append(DiagnosticResult(
                    code="PACKAGE_MEMBER",
                    message=f"Reference to {ref_fqn} is a package member, which is not supported for extraction.",
                    severity="warning",
                    details={"package_member_fqn": ref_fqn},
                ))

    return results if results else None


@diagnostic(
    code="PIPELINED_FUNCTION",
    objects=["function"],
    dialects=("oracle",),
    severity="warning",
    pass_number=1,
)
def check_pipelined_function(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag Oracle PIPELINED table functions that require special migration handling."""
    if ctx.ddl_entry is None:
        return None
    if not ctx.ddl_entry.raw_ddl:
        return None
    if re.search(r'\bPIPELINED\b', ctx.ddl_entry.raw_ddl, re.IGNORECASE):
        return DiagnosticResult(
            code="PIPELINED_FUNCTION",
            message="Oracle PIPELINED table function detected. Migration requires special handling.",
            severity="warning",
            details={},
        )
    return None
