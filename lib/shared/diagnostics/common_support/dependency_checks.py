"""Dependency-graph cross-dialect diagnostic checks."""

from __future__ import annotations

from collections import deque

from shared.diagnostics import CatalogContext, DiagnosticResult, _THRESHOLDS, diagnostic
from shared.diagnostics.common_support.graph import _get_dep_fqns, _load_catalog_json
from shared.env_config import resolve_catalog_dir


@diagnostic(
    code="CIRCULAR_REFERENCE",
    objects=["procedure"],
    severity="error",
    pass_number=2,
)
def check_circular_reference(ctx: CatalogContext) -> DiagnosticResult | None:
    """Detect cycles in procedure EXEC call chains via BFS."""
    catalog_dir = resolve_catalog_dir(ctx.project_root)

    refs = ctx.catalog_data.get("references", {})
    proc_refs = refs.get("procedures", {}).get("in_scope", [])
    if not proc_refs:
        return None

    # BFS from this procedure through EXEC call chains
    start = ctx.fqn
    visited: set[str] = {start}
    queue: deque[tuple[str, list[str]]] = deque()

    # Seed with direct callees
    for ref in proc_refs:
        callee = f"{ref['schema']}.{ref['name']}".lower()
        if callee == start:
            return DiagnosticResult(
                code="CIRCULAR_REFERENCE",
                message=f"Procedure calls itself directly.",
                severity="error",
                details={"cycle": [start, start]},
            )
        queue.append((callee, [start, callee]))

    while queue:
        current, path = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        callee_data = _load_catalog_json(catalog_dir, "procedures", current)
        if callee_data is None:
            continue

        callee_refs = callee_data.get("references", {}).get("procedures", {}).get("in_scope", [])
        for ref in callee_refs:
            next_fqn = f"{ref['schema']}.{ref['name']}".lower()
            if next_fqn == start:
                return DiagnosticResult(
                    code="CIRCULAR_REFERENCE",
                    message=f"Circular EXEC chain detected: {' -> '.join(path + [start])}",
                    severity="error",
                    details={"cycle": path + [start]},
                )
            if next_fqn not in visited:
                queue.append((next_fqn, path + [next_fqn]))

    return None


@diagnostic(
    code="DEPENDENCY_HAS_ERROR",
    objects=["view", "function", "procedure"],
    severity="warning",
    pass_number=2,
)
def check_dependency_has_error(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag objects whose direct dependencies have error-level diagnostics (depth 1)."""
    if ctx.pass1_results is None:
        return None

    results: list[DiagnosticResult] = []
    for dep_fqn, bucket in _get_dep_fqns(ctx.catalog_data):
        dep_diags = ctx.pass1_results.get(dep_fqn, [])
        for diag in dep_diags:
            if diag.severity == "error":
                results.append(DiagnosticResult(
                    code="DEPENDENCY_HAS_ERROR",
                    message=f"Dependency {dep_fqn} has error: {diag.code}.",
                    severity="warning",
                    details={
                        "dependency_fqn": dep_fqn,
                        "dependency_type": bucket.rstrip("s"),
                        "error_code": diag.code,
                    },
                ))
    return results if results else None


@diagnostic(
    code="TRANSITIVE_SCOPE_LEAK",
    objects=["view", "function", "procedure"],
    severity="warning",
    pass_number=2,
)
def check_transitive_scope_leak(ctx: CatalogContext) -> list[DiagnosticResult] | None:
    """Flag objects whose direct dependencies have scope issues (depth 1)."""
    if ctx.pass1_results is None:
        return None

    leak_codes = {"MISSING_REFERENCE", "OUT_OF_SCOPE_REFERENCE"}
    results: list[DiagnosticResult] = []
    for dep_fqn, bucket in _get_dep_fqns(ctx.catalog_data):
        dep_diags = ctx.pass1_results.get(dep_fqn, [])
        for diag in dep_diags:
            if diag.code in leak_codes:
                leaked_ref = diag.details.get("missing_fqn") or diag.details.get("fqn", "unknown") if diag.details else "unknown"
                results.append(DiagnosticResult(
                    code="TRANSITIVE_SCOPE_LEAK",
                    message=f"Dependency {dep_fqn} has {diag.code}.",
                    severity="warning",
                    details={
                        "dependency_fqn": dep_fqn,
                        "leaked_reference": leaked_ref,
                        "leak_type": diag.code,
                    },
                ))
    return results if results else None


@diagnostic(
    code="NESTED_VIEW_CHAIN",
    objects=["view"],
    severity="warning",
    pass_number=2,
)
def check_nested_view_chain(ctx: CatalogContext) -> DiagnosticResult | None:
    """Flag views with deeply nested view-to-view chains (tables are leaves)."""
    threshold = _THRESHOLDS["NESTED_VIEW_CHAIN_DEPTH"]
    catalog_dir = resolve_catalog_dir(ctx.project_root)

    refs = ctx.catalog_data.get("references", {})
    view_refs = refs.get("views", {}).get("in_scope", [])
    if not view_refs:
        return None

    # BFS to find max depth from this view through view references
    # Tables are leaf nodes — stop traversal there
    max_depth = 0
    longest_chain: list[str] = [ctx.fqn]

    # (fqn, depth, chain)
    queue: deque[tuple[str, int, list[str]]] = deque()
    for ref in view_refs:
        ref_fqn = f"{ref['schema']}.{ref['name']}".lower()
        queue.append((ref_fqn, 1, [ctx.fqn, ref_fqn]))

    visited: set[str] = {ctx.fqn}

    while queue:
        current, depth, chain = queue.popleft()
        if current in visited:
            continue
        visited.add(current)

        if depth > max_depth:
            max_depth = depth
            longest_chain = chain

        child_data = _load_catalog_json(catalog_dir, "views", current)
        if child_data is None:
            continue

        child_view_refs = child_data.get("references", {}).get("views", {}).get("in_scope", [])
        for ref in child_view_refs:
            next_fqn = f"{ref['schema']}.{ref['name']}".lower()
            if next_fqn not in visited:
                queue.append((next_fqn, depth + 1, chain + [next_fqn]))

    if max_depth < threshold:
        return None

    return DiagnosticResult(
        code="NESTED_VIEW_CHAIN",
        message=f"View chain depth is {max_depth + 1} (threshold: {threshold}).",
        severity="warning",
        details={"depth": max_depth + 1, "chain": longest_chain},
    )
