"""Reference-focused cross-dialect diagnostic checks."""

from __future__ import annotations

from shared.diagnostics import CatalogContext, DiagnosticResult, diagnostic


@diagnostic(
    code="MISSING_REFERENCE",
    objects=["view", "function", "procedure"],
    severity="warning",
    pass_number=1,
)
def check_missing_reference(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag in_scope references pointing to FQNs with no catalog file."""
    results: list[DiagnosticResult] = []
    refs = ctx.catalog_data.get("references", {})

    for bucket in ("tables", "views", "functions", "procedures"):
        in_scope = refs.get(bucket, {}).get("in_scope", [])
        known = ctx.known_fqns.get(bucket, set())
        for entry in in_scope:
            ref_fqn = f"{entry['schema']}.{entry['name']}".lower()
            if ref_fqn not in known:
                # Skip refs that are known package members (PACKAGE_MEMBER check handles them)
                if ctx.package_members is not None and ref_fqn in ctx.package_members:
                    continue
                results.append(DiagnosticResult(
                    code="MISSING_REFERENCE",
                    message=f"Referenced {bucket.rstrip('s')} {entry['schema']}.{entry['name']} has no catalog entry.",
                    severity="warning",
                    details={"missing_fqn": f"{entry['schema']}.{entry['name']}", "reference_type": bucket.rstrip("s")},
                ))
    return results if results else None


@diagnostic(
    code="OUT_OF_SCOPE_REFERENCE",
    objects=["view", "function", "procedure"],
    severity="warning",
    pass_number=1,
)
def check_out_of_scope_reference(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag references classified as out-of-scope (cross-database or cross-server)."""
    results: list[DiagnosticResult] = []
    refs = ctx.catalog_data.get("references", {})

    for bucket in ("tables", "views", "functions", "procedures"):
        out_of_scope = refs.get(bucket, {}).get("out_of_scope", [])
        for entry in out_of_scope:
            parts = [entry.get("server", ""), entry.get("database", ""), entry.get("schema", ""), entry.get("name", "")]
            fqn = ".".join(p for p in parts if p)
            results.append(DiagnosticResult(
                code="OUT_OF_SCOPE_REFERENCE",
                message=f"Reference to external object {fqn} ({entry.get('reason', 'unknown')}).",
                severity="warning",
                details={"fqn": fqn, "reason": entry.get("reason", "unknown")},
            ))
    return results if results else None


@diagnostic(
    code="REMOTE_EXEC_UNSUPPORTED",
    objects=["procedure"],
    severity="error",
    pass_number=1,
)
def check_remote_exec_unsupported(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag cross-database or cross-server procedure EXEC targets as unsupported."""
    refs = ctx.catalog_data.get("references", {})
    out_of_scope = refs.get("procedures", {}).get("out_of_scope", [])
    results: list[DiagnosticResult] = []
    for entry in out_of_scope:
        reason = entry.get("reason", "unknown")
        if reason not in {"cross-database", "cross-server"}:
            continue
        parts = [entry.get("server", ""), entry.get("database", ""), entry.get("schema", ""), entry.get("name", "")]
        fqn = ".".join(p for p in parts if p)
        results.append(DiagnosticResult(
            code="REMOTE_EXEC_UNSUPPORTED",
            message=f"Procedure delegates to external procedure {fqn}, which is out of scope for direct migration.",
            severity="error",
            details={"fqn": fqn, "reason": reason},
        ))
    return results if results else None
