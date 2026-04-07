"""SQL Server dialect-specific diagnostic checks."""
from __future__ import annotations

import logging
import re
from typing import Any

from shared.diagnostics import CatalogContext, DiagnosticResult, diagnostic

logger = logging.getLogger(__name__)


# ── Pass 1: per-object checks ───────────────────────────────────────────────


@diagnostic(
    code="AMBIGUOUS_REFERENCE",
    objects=["view", "function", "procedure"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_ambiguous_reference(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag in_scope references marked as ambiguous by the DMF."""
    results: list[DiagnosticResult] = []
    refs = ctx.catalog_data.get("references", {})

    for bucket in ("tables", "views", "functions", "procedures", "materialized_views"):
        in_scope = refs.get(bucket, {}).get("in_scope", [])
        for entry in in_scope:
            if entry.get("is_ambiguous"):
                ref_fqn = f"{entry['schema']}.{entry['name']}"
                results.append(DiagnosticResult(
                    code="AMBIGUOUS_REFERENCE",
                    message=f"Reference {ref_fqn} is ambiguous (DMF could not resolve the target type).",
                    severity="warning",
                    details={"reference_fqn": ref_fqn, "reference_type": bucket},
                ))
    return results if results else None


@diagnostic(
    code="DMF_MISCLASSIFIED",
    objects=["view", "function", "procedure"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_dmf_misclassified(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag table references that are actually views or functions."""
    results: list[DiagnosticResult] = []
    refs = ctx.catalog_data.get("references", {})
    in_scope = refs.get("tables", {}).get("in_scope", [])

    for entry in in_scope:
        ref_fqn = f"{entry['schema']}.{entry['name']}".lower()
        if ref_fqn in ctx.known_fqns.get("views", set()):
            results.append(DiagnosticResult(
                code="DMF_MISCLASSIFIED",
                message=f"Reference {entry['schema']}.{entry['name']} is classified as a table but is likely a view.",
                severity="warning",
                details={"misclassified_fqn": ref_fqn, "assigned_bucket": "tables", "likely_bucket": "views"},
            ))
        elif ref_fqn in ctx.known_fqns.get("functions", set()):
            results.append(DiagnosticResult(
                code="DMF_MISCLASSIFIED",
                message=f"Reference {entry['schema']}.{entry['name']} is classified as a table but is likely a function.",
                severity="warning",
                details={"misclassified_fqn": ref_fqn, "assigned_bucket": "tables", "likely_bucket": "functions"},
            ))
    return results if results else None


@diagnostic(
    code="DMF_ERROR",
    objects=["view", "function", "procedure"],
    dialects=("tsql",),
    severity="error",
    pass_number=1,
)
def check_dmf_error(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag DMF errors collected during reference extraction."""
    dmf_errors = ctx.catalog_data.get("dmf_errors", [])
    if not dmf_errors:
        return None
    return [
        DiagnosticResult(
            code="DMF_ERROR",
            message=f"DMF reference extraction error: {error_string[:200]}",
            severity="error",
            details={"error": error_string},
        )
        for error_string in dmf_errors
    ]


@diagnostic(
    code="SUBTYPE_UNKNOWN",
    objects=["function"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_subtype_unknown(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag functions whose subtype (FN/IF/TF) could not be determined."""
    subtype = ctx.catalog_data.get("subtype")
    if subtype:
        return None
    return DiagnosticResult(
        code="SUBTYPE_UNKNOWN",
        message="Function subtype (FN/IF/TF) could not be determined.",
        severity="warning",
        details={},
    )


@diagnostic(
    code="CROSS_DB_EXEC",
    objects=["procedure"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_cross_db_exec(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag procedures that execute a cross-database call."""
    routing_reasons = ctx.catalog_data.get("routing_reasons", [])
    if "cross_db_exec" not in routing_reasons:
        return None
    return DiagnosticResult(
        code="CROSS_DB_EXEC",
        message="Procedure executes a cross-database call.",
        severity="warning",
        details={"routing_reason": "cross_db_exec"},
    )


@diagnostic(
    code="LINKED_SERVER_EXEC",
    objects=["procedure"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_linked_server_exec(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag procedures that execute a linked-server call."""
    routing_reasons = ctx.catalog_data.get("routing_reasons", [])
    if "linked_server_exec" not in routing_reasons:
        return None
    return DiagnosticResult(
        code="LINKED_SERVER_EXEC",
        message="Procedure executes a linked-server call.",
        severity="warning",
        details={"routing_reason": "linked_server_exec"},
    )


@diagnostic(
    code="GOTO_DETECTED",
    objects=["procedure"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_goto_detected(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag procedures that use GOTO control flow."""
    if ctx.ddl_entry is None:
        return None
    if not ctx.ddl_entry.raw_ddl:
        return None
    if re.search(r'\bGOTO\b', ctx.ddl_entry.raw_ddl, re.IGNORECASE):
        return DiagnosticResult(
            code="GOTO_DETECTED",
            message="Procedure uses GOTO control flow, which the segmenter cannot handle.",
            severity="warning",
            details={},
        )
    return None


@diagnostic(
    code="SEGMENTER_LIMIT",
    objects=["procedure"],
    dialects=("tsql",),
    severity="warning",
    pass_number=1,
)
def check_segmenter_limit(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag procedures where the segmenter hit a limit."""
    segmenter_error = ctx.catalog_data.get("segmenter_error")
    if not segmenter_error:
        return None
    return DiagnosticResult(
        code="SEGMENTER_LIMIT",
        message=f"Segmenter hit a limit: {segmenter_error[:200]}",
        severity="warning",
        details={"segmenter_error": segmenter_error},
    )
