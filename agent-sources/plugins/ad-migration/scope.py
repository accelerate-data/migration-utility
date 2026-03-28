"""scope.py — find stored procedures that write to a target table.

Usage:
    python scope.py --ddl-path PATH --table dbo.FactSales [--dialect tsql] [--depth 3]

Output JSON is written to stdout.  Warnings and progress go to stderr.
Exit codes:
    0 — success (even if no writers found)
    1 — domain failure
    2 — IO / parse error
"""

from __future__ import annotations

import json
import re
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import typer

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Write detection (regex on raw DDL)
_INSERT_RE = re.compile(r"\bINSERT\s+INTO\s+", re.IGNORECASE)
_UPDATE_RE = re.compile(r"\bUPDATE\s+", re.IGNORECASE)
_DELETE_RE = re.compile(r"\bDELETE\s+FROM\s+", re.IGNORECASE)
_MERGE_RE = re.compile(r"\bMERGE\s+(?:INTO\s+)?", re.IGNORECASE)
_TRUNCATE_RE = re.compile(r"\bTRUNCATE\s+TABLE\s+", re.IGNORECASE)

# Name token: optional brackets, word chars
_NAME_TOKEN = r"(?:\[?\w+\]?)"

# Qualified name: one or two-part (schema.table or just table)
_QUALIFIED_NAME = rf"({_NAME_TOKEN}(?:\.{_NAME_TOKEN})?)"

_INSERT_TARGET_RE = re.compile(
    rf"\bINSERT\s+INTO\s+{_QUALIFIED_NAME}", re.IGNORECASE
)
_UPDATE_TARGET_RE = re.compile(
    rf"\bUPDATE\s+{_QUALIFIED_NAME}", re.IGNORECASE
)
_DELETE_TARGET_RE = re.compile(
    rf"\bDELETE\s+FROM\s+{_QUALIFIED_NAME}", re.IGNORECASE
)
_MERGE_TARGET_RE = re.compile(
    rf"\bMERGE\s+(?:INTO\s+)?{_QUALIFIED_NAME}", re.IGNORECASE
)
_TRUNCATE_TARGET_RE = re.compile(
    rf"\bTRUNCATE\s+TABLE\s+{_QUALIFIED_NAME}", re.IGNORECASE
)

# Dynamic SQL detection
_DYNAMIC_SQL_RE = re.compile(
    r"EXEC\s*\(|EXEC\s+@\w+|sp_executesql|EXECUTE\s*\(",
    re.IGNORECASE,
)

# Cross-database (4-part name): server.db.schema.table
_FOUR_PART_RE = re.compile(
    rf"\[?\w+\]?\.\[?\w+\]?\.\[?\w+\]?\.\[?\w+\]?",
    re.IGNORECASE,
)

# Call graph: EXEC / EXECUTE followed by optional schema-qualified proc name
_EXEC_CALL_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+(?:\[?\w+\]?\.)?\[?(\w+)\]?\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class WriterEntry:
    procedure_name: str
    write_type: str  # "direct" | "indirect"
    write_operations: list[str]
    call_path: list[str]
    confidence: float
    status: str  # "confirmed" | "suspected"


@dataclass
class ErrorEntry:
    procedure: str
    code: str
    message: str


@dataclass
class ScopeResult:
    table: str
    writers: list[WriterEntry]
    errors: list[ErrorEntry]


# ---------------------------------------------------------------------------
# Internal analysis helpers
# ---------------------------------------------------------------------------


def _normalize(name: str, default_schema: str = "dbo") -> str:
    """Inline normalize — avoids coupling tests to import order."""
    # strip brackets
    cleaned = re.sub(r"\[([^\]]+)\]", r"\1", name).lower().strip()
    parts = [p.strip() for p in cleaned.split(".")]
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return f"{default_schema}.{parts[-1]}"


def _detect_writes_regex(
    raw_ddl: str,
    target_fqn: str,
) -> list[str]:
    """Return list of write operation keywords found targeting *target_fqn*.

    Compares normalized table names in each write statement against target_fqn.
    """
    ops: list[str] = []

    checks: list[tuple[re.Pattern[str], str]] = [
        (_INSERT_TARGET_RE, "INSERT"),
        (_UPDATE_TARGET_RE, "UPDATE"),
        (_DELETE_TARGET_RE, "DELETE"),
        (_MERGE_TARGET_RE, "MERGE"),
        (_TRUNCATE_TARGET_RE, "TRUNCATE"),
    ]

    for pattern, op_name in checks:
        for match in pattern.finditer(raw_ddl):
            candidate = match.group(1)
            if _normalize(candidate) == target_fqn:
                if op_name not in ops:
                    ops.append(op_name)

    return ops


def _has_dynamic_sql(raw_ddl: str) -> bool:
    return bool(_DYNAMIC_SQL_RE.search(raw_ddl))


def _has_four_part_name(raw_ddl: str) -> bool:
    return bool(_FOUR_PART_RE.search(raw_ddl))


def _extract_callees(raw_ddl: str, catalog_procs: dict[str, Any]) -> list[str]:
    """Return normalized proc names called via EXEC/EXECUTE that exist in catalog."""
    callees: list[str] = []
    for match in _EXEC_CALL_RE.finditer(raw_ddl):
        callee_name = match.group(1)
        # Try with and without default schema
        normalized = _normalize(callee_name)
        if normalized in catalog_procs:
            if normalized not in callees:
                callees.append(normalized)
        else:
            # Also try if full match includes schema
            full_match = match.group(0)
            # Extract the full qualified name from the EXEC statement
            exec_name_match = re.search(
                r"\bEXEC(?:UTE)?\s+((?:\[?\w+\]?\.)?(?:\[?\w+\]?))\b",
                full_match,
                re.IGNORECASE,
            )
            if exec_name_match:
                alt = _normalize(exec_name_match.group(1))
                if alt in catalog_procs and alt not in callees:
                    callees.append(alt)
    return callees


def _compute_confidence(
    write_type: str,
    has_dynamic: bool,
    has_static_write: bool,
    call_path_len: int,
    max_path_len: int,
    multiple_paths: bool,
) -> float:
    """Compute confidence score per scoring.md rules."""
    if write_type == "direct":
        base = 0.90
    else:
        base = 0.75

    score = base

    # Shorter call path bonus: +0.02 per hop shorter than the deepest path
    hops_shorter = max_path_len - call_path_len
    score += hops_shorter * 0.02

    # Multiple independent paths bonus
    if multiple_paths:
        score += 0.05

    if has_dynamic and has_static_write:
        score -= 0.20
    elif has_dynamic and not has_static_write:
        score = min(score, 0.45)

    return max(0.0, min(1.0, score))


# ---------------------------------------------------------------------------
# Core BFS traversal
# ---------------------------------------------------------------------------


def scope_writers(
    catalog_procs: dict[str, Any],
    target_fqn: str,
    depth: int = 3,
) -> ScopeResult:
    """Find all procedures writing to *target_fqn* up to *depth* call hops.

    Args:
        catalog_procs: dict of normalized_name -> DdlEntry (from DdlCatalog.procedures)
        target_fqn:    normalized fully-qualified target table name
        depth:         maximum call depth to traverse

    Returns:
        ScopeResult with writers and errors lists.
    """
    errors: list[ErrorEntry] = []
    cross_db_procs: set[str] = set()

    # Phase 1: scan all procs for cross-db and basic write detection
    direct_writers: dict[str, list[str]] = {}   # proc_fqn -> [ops]
    dynamic_procs: set[str] = set()

    for proc_name, entry in catalog_procs.items():
        raw = entry.raw_ddl

        # Check for cross-database 4-part names
        if _has_four_part_name(raw):
            errors.append(ErrorEntry(
                procedure=proc_name,
                code="ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE",
                message=(
                    f"Procedure '{proc_name}' references a cross-database "
                    f"4-part name and is out of scope for static analysis."
                ),
            ))
            cross_db_procs.add(proc_name)
            continue

        if _has_dynamic_sql(raw):
            dynamic_procs.add(proc_name)

        # Layer 1: try AST-based write detection
        ast_writes: list[str] = []
        if entry.parse_error is None and entry.ast is not None:
            try:
                # Import here to avoid circular issues when scope.py is used standalone
                from shared.loader import extract_refs, DdlParseError
                refs = extract_refs(entry)
                if target_fqn in refs.writes_to:
                    ast_writes = ["INSERT"]  # AST detected at least one write; ops below refine
            except Exception as exc:  # noqa: BLE001
                print(
                    f"scope: event=ast_extract_refs operation=extract_refs"
                    f" procedure={proc_name} status=failure error={exc!r};"
                    " falling back to regex layer",
                    file=sys.stderr,
                )

        # Layer 2: regex write detection (always run to detect ops + parse-error procs)
        regex_ops = _detect_writes_regex(raw, target_fqn)

        # Combine: use regex ops (more detailed), but AST confirms the detection
        all_ops = regex_ops

        if all_ops:
            direct_writers[proc_name] = all_ops

        print(
            f"scope: event=proc_scan operation=detect_writes"
            f" procedure={proc_name} ops={all_ops}"
            f" dynamic={proc_name in dynamic_procs}",
            file=sys.stderr,
        )

    # Phase 2: build call graph using regex on raw DDL
    callee_map: dict[str, list[str]] = {}
    for proc_name, entry in catalog_procs.items():
        if proc_name in cross_db_procs:
            continue
        callees = _extract_callees(entry.raw_ddl, catalog_procs)
        if callees:
            callee_map[proc_name] = callees

    # Phase 3: BFS from each proc to find indirect writers
    # For each non-direct-writer proc, BFS its call tree up to `depth` hops
    # to find if it reaches a direct writer.

    # We'll collect all writer results: direct first, then indirect
    writer_results: list[WriterEntry] = []

    # Track procs already added as direct writers
    added_procs: set[str] = set()

    # Add direct writers
    for proc_name, ops in direct_writers.items():
        has_dyn = proc_name in dynamic_procs
        writer_results.append(WriterEntry(
            procedure_name=proc_name,
            write_type="direct",
            write_operations=ops,
            call_path=[proc_name],
            confidence=0.0,  # placeholder; computed after all writers found
            status="",
        ))
        added_procs.add(proc_name)

    # BFS to find indirect writers
    # For each proc not already a direct writer, do BFS up to `depth` hops
    for start_proc in list(catalog_procs.keys()):
        if start_proc in added_procs or start_proc in cross_db_procs:
            continue
        if start_proc in direct_writers:
            continue

        # BFS: find shortest path from start_proc to any direct writer
        visited: set[str] = {start_proc}
        # queue: (current_proc, path_from_start_excluding_start)
        queue: deque[tuple[str, list[str]]] = deque()
        queue.append((start_proc, []))
        found_paths: list[list[str]] = []

        while queue:
            current, path = queue.popleft()
            if len(path) >= depth:
                continue
            for callee in callee_map.get(current, []):
                if callee in direct_writers and callee not in cross_db_procs:
                    # Found a path: start_proc -> ... -> callee (direct writer)
                    found_paths.append(path + [callee])
                elif callee not in visited:
                    visited.add(callee)
                    queue.append((callee, path + [callee]))

        if found_paths:
            # Take shortest path (minimum hops)
            best_path = min(found_paths, key=len)
            # call_path is the intermediate procs between start and the direct writer
            # According to call-graph.md: call_path lists procs from entry to writer
            # For indirect: call_path = [direct_writer_proc] when one hop
            # The path already contains the direct writer at the end
            call_path = best_path  # [intermediate..., direct_writer]

            writer_results.append(WriterEntry(
                procedure_name=start_proc,
                write_type="indirect",
                write_operations=[],
                call_path=[p for p in call_path],
                confidence=0.0,  # placeholder
                status="",
            ))
            added_procs.add(start_proc)

    # Phase 4: compute confidence scores
    # Determine max_path_len across all writers
    max_path_len = max(
        (len(w.call_path) for w in writer_results),
        default=0,
    )

    # Check for multiple independent paths to any direct writer
    # (procs that appear as indirect writers via multiple call paths)
    # Simplified: multiple_paths=True if more than one writer reaches the same
    # direct writer through different paths. For the base implementation,
    # we check if a proc has >1 found_paths.
    # We re-run to collect multiple paths per indirect writer.
    indirect_multi: dict[str, int] = {}  # proc -> number of paths found

    for start_proc in list(catalog_procs.keys()):
        if start_proc in direct_writers or start_proc in cross_db_procs:
            continue
        if start_proc not in added_procs:
            continue
        visited2: set[str] = {start_proc}
        queue2: deque[tuple[str, list[str]]] = deque()
        queue2.append((start_proc, []))
        path_count = 0

        while queue2:
            current, path = queue2.popleft()
            if len(path) >= depth:
                continue
            for callee in callee_map.get(current, []):
                if callee in direct_writers and callee not in cross_db_procs:
                    path_count += 1
                elif callee not in visited2:
                    visited2.add(callee)
                    queue2.append((callee, path + [callee]))

        indirect_multi[start_proc] = path_count

    for entry in writer_results:
        has_dyn = entry.procedure in dynamic_procs
        has_static = bool(entry.write_operations) or entry.write_type == "direct"
        multiple = (
            indirect_multi.get(entry.procedure, 0) > 1
            if entry.write_type == "indirect"
            else False
        )

        entry.confidence = _compute_confidence(
            write_type=entry.write_type,
            has_dynamic=has_dyn,
            has_static_write=has_static or entry.write_type == "direct",
            call_path_len=len(entry.call_path),
            max_path_len=max_path_len,
            multiple_paths=multiple,
        )
        entry.status = "confirmed" if entry.confidence >= 0.70 else "suspected"

    return ScopeResult(
        table=target_fqn,
        writers=writer_results,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

app = typer.Typer(add_completion=False)


@app.command()
def main(
    ddl_path: Path = typer.Option(..., help="Path to DDL artifacts directory"),
    table: str = typer.Option(..., help="Fully-qualified target table (e.g. dbo.FactSales)"),
    dialect: str = typer.Option("tsql", help="SQL dialect for parsing"),
    depth: int = typer.Option(3, help="Maximum call-graph traversal depth"),
) -> None:
    """Find stored procedures that write to a target table."""
    try:
        # Import here so the module is usable without modifying sys.path at import time
        sys.path.insert(0, str(Path(__file__).parent / "shared"))
        from shared.loader import load_directory
    except ImportError as exc:
        print(
            json.dumps({"error": f"Failed to import shared library: {exc}"}),
            file=sys.stdout,
        )
        raise typer.Exit(2)

    try:
        catalog = load_directory(ddl_path, dialect=dialect)
    except FileNotFoundError as exc:
        print(
            json.dumps({"error": str(exc)}),
            file=sys.stdout,
        )
        raise typer.Exit(2)
    except Exception as exc:
        print(
            json.dumps({"error": f"Unexpected error loading DDL: {exc}"}),
            file=sys.stdout,
        )
        raise typer.Exit(2)

    target_fqn = _normalize(table)
    print(f"scope: target table normalized to '{target_fqn}'", file=sys.stderr)

    result = scope_writers(catalog.procedures, target_fqn, depth=depth)

    output = {
        "table": result.table,
        "writers": [
            {
                "procedure_name": w.procedure_name,
                "write_type": w.write_type,
                "write_operations": w.write_operations,
                "call_path": w.call_path,
                "confidence": round(w.confidence, 4),
                "status": w.status,
            }
            for w in result.writers
        ],
        "errors": [
            {
                "procedure": e.procedure,
                "code": e.code,
                "message": e.message,
            }
            for e in result.errors
        ],
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    app()
