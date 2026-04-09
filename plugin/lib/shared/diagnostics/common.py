"""Cross-dialect diagnostic checks.

Each function is registered via the ``@diagnostic`` decorator and runs
during ``run_diagnostics()``.  Dialect-specific checks live in separate
modules (e.g. ``sqlserver.py``, ``oracle.py``).
"""

from __future__ import annotations

import json
import logging
from collections import deque
from pathlib import Path
from typing import Any

from shared.diagnostics import (
    ALL_DIALECTS,
    CatalogContext,
    DiagnosticResult,
    _THRESHOLDS,
    diagnostic,
)
from shared.env_config import resolve_catalog_dir

logger = logging.getLogger(__name__)


# ── Pass 1: per-object checks ───────────────────────────────────────────────


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


# ── Pass 1: reference resolution checks ─────────────────────────────────────


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


# ── Pass 2: graph traversal checks ──────────────────────────────────────────


def _load_catalog_json(catalog_dir: Path, bucket: str, fqn: str) -> dict[str, Any] | None:
    """Load a catalog JSON file by bucket and FQN."""
    p = catalog_dir / bucket / f"{fqn}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


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


def _get_dep_fqns(catalog_data: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract all direct dependency (fqn, bucket) pairs from references.*.in_scope."""
    deps: list[tuple[str, str]] = []
    refs = catalog_data.get("references", {})
    for bucket in ("tables", "views", "functions", "procedures"):
        for entry in refs.get(bucket, {}).get("in_scope", []):
            fqn = f"{entry['schema']}.{entry['name']}".lower()
            deps.append((fqn, bucket))
    return deps


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
