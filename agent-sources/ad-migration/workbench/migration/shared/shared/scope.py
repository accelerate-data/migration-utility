"""scope.py — find stored procedures that write to a target table.

Usage:
    uv run --project shared scope --ddl-path PATH --table dbo.FactSales [--dialect tsql] [--depth 3]

Output JSON is written to stdout.  Warnings and progress go to stderr.
Exit codes:
    0 — success (even if no writers found)
    1 — domain failure
    2 — IO / parse error

All write detection uses sqlglot AST analysis.  Procedures that cannot be
fully parsed (including those with EXEC / dynamic SQL) are reported as
PARSE_FAILED errors — no regex fallback is used.
"""

from __future__ import annotations

import json
import sys
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlglot.expressions as exp
import typer

from shared.loader import DdlEntry, DdlParseError, extract_refs
from shared.name_resolver import normalize

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
# AST analysis helpers
# ---------------------------------------------------------------------------

def _detect_writes_ast(entry: DdlEntry, target_fqn: str) -> list[str]:
    """Return write operation names targeting *target_fqn*.

    Uses extract_refs().write_operations which already maps each write
    target to its operation names via body statement parsing.

    Raises DdlParseError if the entry cannot be analysed.
    """
    refs = extract_refs(entry)
    return refs.write_operations.get(target_fqn, [])


def _has_cross_db_ref(entry: DdlEntry) -> bool:
    """Return True if any table reference in the AST has a catalog (3+ part name)."""
    if entry.ast is None:
        return False
    for tbl in entry.ast.find_all(exp.Table):
        if tbl.catalog:
            return True
    return False


def _compute_confidence(
    write_type: str,
    call_path_len: int,
    max_path_len: int,
    multiple_paths: bool,
) -> float:
    """Compute confidence score per scoring.md rules."""
    base = 0.90 if write_type == "direct" else 0.75
    score = base

    hops_shorter = max_path_len - call_path_len
    score += hops_shorter * 0.02

    if multiple_paths:
        score += 0.05

    return max(0.0, min(1.0, score))


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------


def scope_writers(
    catalog_procs: dict[str, Any],
    target_fqn: str,
    depth: int = 3,
) -> ScopeResult:
    """Find all procedures writing to *target_fqn* up to *depth* call hops.

    Args:
        catalog_procs: dict of normalized_name -> DdlEntry
        target_fqn:    normalized fully-qualified target table name
        depth:         maximum call depth to traverse

    Returns:
        ScopeResult with writers and errors lists.
    """
    errors: list[ErrorEntry] = []
    excluded: set[str] = set()

    # Phase 1: scan all procs — cross-db check, then AST write detection
    direct_writers: dict[str, list[str]] = {}

    for proc_name, entry in catalog_procs.items():
        # Cross-database check (AST-based)
        if _has_cross_db_ref(entry):
            errors.append(ErrorEntry(
                procedure=proc_name,
                code="ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE",
                message=(
                    f"Procedure '{proc_name}' references a cross-database "
                    f"name and is out of scope for static analysis."
                ),
            ))
            excluded.add(proc_name)
            continue

        # AST write detection — no regex fallback
        try:
            ops = _detect_writes_ast(entry, target_fqn)
        except DdlParseError as exc:
            errors.append(ErrorEntry(
                procedure=proc_name,
                code="PARSE_FAILED",
                message=str(exc),
            ))
            excluded.add(proc_name)
            print(
                f"scope: event=proc_scan operation=detect_writes"
                f" procedure={proc_name} status=parse_failed",
                file=sys.stderr,
            )
            continue

        if ops:
            direct_writers[proc_name] = ops

        print(
            f"scope: event=proc_scan operation=detect_writes"
            f" procedure={proc_name} ops={ops}",
            file=sys.stderr,
        )

    # Phase 2: build call graph from AST refs
    callee_map: dict[str, list[str]] = {}
    for proc_name, entry in catalog_procs.items():
        if proc_name in excluded:
            continue
        try:
            refs = extract_refs(entry)
            callees = [c for c in refs.calls if c in catalog_procs]
            if callees:
                callee_map[proc_name] = callees
        except DdlParseError:
            pass  # already reported in Phase 1

    # Phase 3: collect writers (direct + indirect via BFS)
    writer_results: list[WriterEntry] = []
    added_procs: set[str] = set()

    for proc_name, ops in direct_writers.items():
        writer_results.append(WriterEntry(
            procedure_name=proc_name,
            write_type="direct",
            write_operations=ops,
            call_path=[proc_name],
            confidence=0.0,
            status="",
        ))
        added_procs.add(proc_name)

    # BFS for indirect writers
    for start_proc in catalog_procs:
        if start_proc in added_procs or start_proc in excluded:
            continue

        visited: set[str] = {start_proc}
        queue: deque[tuple[str, list[str]]] = deque([(start_proc, [])])
        found_paths: list[list[str]] = []

        while queue:
            current, path = queue.popleft()
            if len(path) >= depth:
                continue
            for callee in callee_map.get(current, []):
                if callee in direct_writers and callee not in excluded:
                    found_paths.append(path + [callee])
                elif callee not in visited:
                    visited.add(callee)
                    queue.append((callee, path + [callee]))

        if found_paths:
            best_path = min(found_paths, key=len)
            writer_results.append(WriterEntry(
                procedure_name=start_proc,
                write_type="indirect",
                write_operations=[],
                call_path=list(best_path),
                confidence=0.0,
                status="",
            ))
            added_procs.add(start_proc)

    # Phase 4: compute confidence scores
    max_path_len = max(
        (len(w.call_path) for w in writer_results),
        default=0,
    )

    indirect_multi: dict[str, int] = {}
    for start_proc in catalog_procs:
        if start_proc in direct_writers or start_proc in excluded:
            continue
        if start_proc not in added_procs:
            continue
        visited2: set[str] = {start_proc}
        queue2: deque[tuple[str, list[str]]] = deque([(start_proc, [])])
        path_count = 0
        while queue2:
            current, path = queue2.popleft()
            if len(path) >= depth:
                continue
            for callee in callee_map.get(current, []):
                if callee in direct_writers and callee not in excluded:
                    path_count += 1
                elif callee not in visited2:
                    visited2.add(callee)
                    queue2.append((callee, path + [callee]))
        indirect_multi[start_proc] = path_count

    for w in writer_results:
        multiple = (
            indirect_multi.get(w.procedure_name, 0) > 1
            if w.write_type == "indirect"
            else False
        )
        w.confidence = _compute_confidence(
            write_type=w.write_type,
            call_path_len=len(w.call_path),
            max_path_len=max_path_len,
            multiple_paths=multiple,
        )
        w.status = "confirmed" if w.confidence >= 0.70 else "suspected"

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
    from shared.loader import load_directory

    try:
        catalog = load_directory(ddl_path, dialect=dialect)
    except FileNotFoundError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stdout)
        raise typer.Exit(2)
    except Exception as exc:
        print(json.dumps({"error": f"Unexpected error loading DDL: {exc}"}), file=sys.stdout)
        raise typer.Exit(2)

    target_fqn = normalize(table)
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
