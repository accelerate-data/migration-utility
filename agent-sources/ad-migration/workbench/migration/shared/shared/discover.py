"""discover.py — DDL object catalog and semantic reference finder.

Standalone CLI with three subcommands:

    list   List all objects of a given type in a DDL directory.
    show   Show details (columns/params/refs) for a single named object.
    refs   Find all procedures/views that reference a given object.

Auto-detects flat vs indexed format: if catalog.json exists in --ddl-path,
uses load_catalog(); otherwise uses load_directory().

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (object not found, etc.)
    2  IO or parse error
"""

from __future__ import annotations

import json
import sys
from collections import deque
from enum import Enum
from pathlib import Path
from typing import Any

import sqlglot.expressions as exp
import typer

from shared.loader import (  # noqa: E402
    DdlCatalog,
    DdlEntry,
    DdlParseError,
    extract_refs,
    load_catalog,
    load_directory,
)
from shared.name_resolver import normalize  # noqa: E402

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Helpers ───────────────────────────────────────────────────────────────────


class ObjectType(str, Enum):
    tables = "tables"
    procedures = "procedures"
    views = "views"
    functions = "functions"


def _load(ddl_path: Path, dialect: str) -> DdlCatalog:
    """Load a DdlCatalog from flat or indexed directory."""
    catalog_json = ddl_path / "catalog.json"
    if catalog_json.exists():
        print(f"discover: loading indexed catalog from {ddl_path}", file=sys.stderr)
        return load_catalog(ddl_path)
    print(f"discover: loading flat directory from {ddl_path}", file=sys.stderr)
    return load_directory(ddl_path, dialect=dialect)


def _emit(data: Any) -> None:
    """Write JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))


def _bucket(catalog: DdlCatalog, object_type: ObjectType) -> dict[str, DdlEntry]:
    return getattr(catalog, object_type.value)


def _singular(object_type: ObjectType) -> str:
    return object_type.value.rstrip("s")


def _find_entry(
    catalog: DdlCatalog, name: str
) -> tuple[str, str, DdlEntry] | None:
    """Find an entry by normalized name across all buckets.

    Returns (normalized_name, type_label, entry) or None.
    """
    norm = normalize(name)
    for type_label, bucket_name in [
        ("table", "tables"),
        ("procedure", "procedures"),
        ("view", "views"),
        ("function", "functions"),
    ]:
        bucket: dict[str, DdlEntry] = getattr(catalog, bucket_name)
        if norm in bucket:
            return norm, type_label, bucket[norm]
    return None


# ── Column / param extraction from AST ───────────────────────────────────────


def _extract_columns(entry: DdlEntry) -> list[dict[str, str]]:
    """Walk ColumnDef nodes in the AST and return column definitions.

    Returns an empty list if the AST is None or no ColumnDef nodes are found.
    """
    if entry.ast is None:
        return []
    columns: list[dict[str, str]] = []
    for col_def in entry.ast.find_all(exp.ColumnDef):
        col_name = col_def.name
        dtype_node = col_def.args.get("kind")
        sql_type = dtype_node.sql(dialect="tsql") if dtype_node is not None else ""
        columns.append({"name": col_name, "sql_type": sql_type})
    return columns


def _extract_params(entry: DdlEntry) -> list[dict[str, Any]]:
    """Extract procedure parameters from the AST.

    Walks ParameterizedTypedef / Parameter nodes attached to the Create node.
    Returns an empty list if the AST is None or no params are found.
    """
    if entry.ast is None:
        return []
    params: list[dict[str, Any]] = []
    for param in entry.ast.find_all(exp.Parameter):
        param_name = param.name
        dtype_node = param.args.get("kind")
        sql_type = dtype_node.sql(dialect="tsql") if dtype_node is not None else ""
        default_node = param.args.get("default")
        default_val = default_node.sql(dialect="tsql") if default_node is not None else None
        is_output = bool(param.args.get("output"))
        params.append(
            {
                "name": param_name,
                "sql_type": sql_type,
                "default": default_val,
                "is_output": is_output,
            }
        )
    return params


# ── Core logic (importable for testing) ───────────────────────────────────────


def run_list(ddl_path: Path, object_type: ObjectType, dialect: str) -> dict[str, Any]:
    """Return the list subcommand result dict."""
    catalog = _load(ddl_path, dialect)
    bucket = _bucket(catalog, object_type)
    objects = sorted(bucket.keys())
    return {"objects": objects}


def run_show(ddl_path: Path, name: str, dialect: str) -> dict[str, Any]:
    """Return the show subcommand result dict.

    Raises SystemExit(1) if the object is not found.
    """
    catalog = _load(ddl_path, dialect)
    found = _find_entry(catalog, name)
    if found is None:
        norm = normalize(name)
        print(f"discover: object not found: {norm}", file=sys.stderr)
        raise typer.Exit(code=1)

    norm, type_label, entry = found

    columns: list[dict] = []
    params: list[dict] = []
    refs_dict: dict | None = None
    parse_error: str | None = entry.parse_error

    if type_label == "table":
        columns = _extract_columns(entry)

    needs_llm = False
    classification: str | None = None

    statements: list[dict] | None = None

    if type_label == "procedure":
        params = _extract_params(entry)
        try:
            obj_refs = extract_refs(entry)
            refs_dict = {
                "reads_from": obj_refs.reads_from,
                "writes_to": obj_refs.writes_to,
                "write_operations": obj_refs.write_operations,
            }
            needs_llm = obj_refs.needs_llm
            statements = obj_refs.statements
        except DdlParseError as exc:
            parse_error = str(exc)
            refs_dict = None
            needs_llm = True

        if needs_llm or parse_error:
            classification = "claude_assisted"
        else:
            classification = "deterministic"

    elif type_label in ("view", "function"):
        try:
            obj_refs = extract_refs(entry)
            refs_dict = {
                "reads_from": obj_refs.reads_from,
                "writes_to": obj_refs.writes_to,
            }
        except DdlParseError as exc:
            parse_error = str(exc)
            refs_dict = None

    return {
        "name": norm,
        "type": type_label,
        "raw_ddl": entry.raw_ddl,
        "columns": columns,
        "params": params,
        "refs": refs_dict,
        "statements": statements,
        "needs_llm": needs_llm,
        "classification": classification,
        "parse_error": parse_error,
    }


def _compute_confidence(
    write_type: str,
    call_path_len: int,
    max_path_len: int,
    multiple_paths: bool,
) -> float:
    """Compute confidence score for a writer entry.

    Scoring rules:
      - Direct write base: 0.90
      - Indirect write base: 0.75
      - Shorter call path: +0.02 per hop saved vs deepest
      - Multiple independent paths: +0.05
    """
    base = 0.90 if write_type == "direct" else 0.75
    score = base + (max_path_len - call_path_len) * 0.02
    if multiple_paths:
        score += 0.05
    return max(0.0, min(1.0, score))


def run_refs(
    ddl_path: Path, name: str, dialect: str, depth: int = 3,
) -> dict[str, Any]:
    """Return the refs subcommand result dict.

    Finds all procedures/views that reference the target object, split
    into readers and writers.  Writers include both direct writers (AST
    detected) and indirect writers (via BFS call-graph traversal up to
    *depth* hops).  Each writer entry carries write_type, call_path,
    write_operations, confidence, and status.

    Procs with partial or missing refs (needs_llm / parse_error) are
    collected in llm_required for the LLM to complete.
    """
    catalog = _load(ddl_path, dialect)
    target = normalize(name)

    # ── Phase 1: scan all procs/views for direct refs ────────────────────
    readers: list[str] = []
    direct_writers: dict[str, list[str]] = {}  # proc_name → write_operations
    llm_required: list[str] = []
    excluded: set[str] = set()

    # Cache extract_refs results to avoid double-parsing
    refs_cache: dict[str, Any] = {}

    for bucket_name in ("procedures", "views"):
        bucket: dict[str, DdlEntry] = getattr(catalog, bucket_name)
        for caller_name, entry in bucket.items():
            if entry.parse_error is not None:
                llm_required.append(caller_name)
                excluded.add(caller_name)
                continue
            try:
                obj_refs = extract_refs(entry)
                refs_cache[caller_name] = obj_refs
            except DdlParseError as exc:
                print(
                    f"discover: skipping {caller_name} (extract_refs error: {str(exc)[:60]})",
                    file=sys.stderr,
                )
                llm_required.append(caller_name)
                excluded.add(caller_name)
                continue

            if obj_refs.needs_llm:
                # Check partial refs but always flag for LLM
                if target in obj_refs.reads_from:
                    readers.append(caller_name)
                if target in obj_refs.writes_to:
                    direct_writers[caller_name] = obj_refs.write_operations.get(target, [])
                llm_required.append(caller_name)
                excluded.add(caller_name)
                continue

            # Deterministic — full refs
            if target in obj_refs.reads_from:
                readers.append(caller_name)
            ops = obj_refs.write_operations.get(target, [])
            if ops:
                direct_writers[caller_name] = ops

    # ── Phase 2: build call graph (procedures only) ──────────────────────
    callee_map: dict[str, list[str]] = {}
    proc_names = set(catalog.procedures.keys())
    for proc_name, obj_refs in refs_cache.items():
        if proc_name in excluded:
            continue
        callees = [c for c in obj_refs.calls if c in proc_names]
        if callees:
            callee_map[proc_name] = callees

    # ── Phase 3: BFS for indirect writers ────────────────────────────────
    writer_entries: list[dict[str, Any]] = []
    added_procs: set[str] = set()

    for proc_name, ops in direct_writers.items():
        writer_entries.append({
            "procedure": proc_name,
            "write_type": "direct",
            "write_operations": ops,
            "call_path": [],
            "confidence": 0.0,
            "status": "",
        })
        added_procs.add(proc_name)

    for start_proc in proc_names:
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
            writer_entries.append({
                "procedure": start_proc,
                "write_type": "indirect",
                "write_operations": [],
                "call_path": list(best_path),
                "confidence": 0.0,
                "status": "",
            })
            added_procs.add(start_proc)

    # ── Phase 4: confidence scoring ──────────────────────────────────────
    max_path_len = max(
        (len(w["call_path"]) for w in writer_entries),
        default=0,
    )

    # Count multiple paths for indirect writers
    indirect_multi: dict[str, int] = {}
    for start_proc in proc_names:
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

    for w in writer_entries:
        multiple = (
            indirect_multi.get(w["procedure"], 0) > 1
            if w["write_type"] == "indirect"
            else False
        )
        w["confidence"] = round(_compute_confidence(
            write_type=w["write_type"],
            call_path_len=len(w["call_path"]),
            max_path_len=max_path_len,
            multiple_paths=multiple,
        ), 4)
        w["status"] = "confirmed" if w["confidence"] >= 0.70 else "suspected"

    # ── Build result ─────────────────────────────────────────────────────
    result: dict[str, Any] = {
        "name": target,
        "readers": sorted(readers),
        "writers": sorted(writer_entries, key=lambda w: w["procedure"]),
    }
    if llm_required:
        result["llm_required"] = sorted(set(llm_required))
    return result


# ── CLI commands ──────────────────────────────────────────────────────────────


@app.command(name="list")
def list_objects(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    type: ObjectType = typer.Option(..., help="Object type to list"),
    dialect: str = typer.Option("tsql", help="sqlglot dialect"),
) -> None:
    """List all objects of a given type in a DDL directory."""
    try:
        result = run_list(ddl_path, type, dialect)
    except (FileNotFoundError, DdlParseError) as exc:
        print(f"discover: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def show(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
    dialect: str = typer.Option("tsql", help="sqlglot dialect"),
) -> None:
    """Show details for a single named DDL object."""
    try:
        result = run_show(ddl_path, name, dialect)
    except (FileNotFoundError, DdlParseError) as exc:
        print(f"discover: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


@app.command()
def refs(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory"),
    name: str = typer.Option(..., help="Fully-qualified object name (schema.Name)"),
    dialect: str = typer.Option("tsql", help="sqlglot dialect"),
    depth: int = typer.Option(3, help="Maximum call-graph depth for indirect writers"),
) -> None:
    """Find all procedures/views that reference a given object."""
    try:
        result = run_refs(ddl_path, name, dialect, depth=depth)
    except (FileNotFoundError, DdlParseError) as exc:
        print(f"discover: {exc}", file=sys.stderr)
        raise typer.Exit(code=2) from exc
    _emit(result)


if __name__ == "__main__":
    app()
