"""Offline AST enrichment for catalog JSON files.

Reads DDL files and existing catalog/ JSON, runs sqlglot AST analysis,
and augments catalog entries with references the DMF missed (CTAS,
SELECT INTO, TRUNCATE, EXEC call chains). Tagged with detection: "ast_scan".
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections import deque
from pathlib import Path
from typing import Any

import typer

from shared.catalog import (
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    write_object_catalog,
    write_table_catalog,
)
from shared.loader import (
    DdlParseError,
    ObjectRefs,
    extract_refs,
    load_directory,
)
from shared.name_resolver import normalize

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)

# Regex to extract procedure calls from EXEC statements.
# Matches: EXEC schema.proc_name or EXECUTE [schema].[proc_name]
# Does NOT match: EXEC(@sql) or EXEC sp_executesql (dynamic SQL).
_EXEC_PROC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s+"
    r"((?:\[?\w+\]?\.)\[?\w+\]?)",
    re.IGNORECASE,
)

_DYNAMIC_EXEC_RE = re.compile(
    r"\bEXEC(?:UTE)?\s*[(@]|"
    r"\bsp_executesql\b",
    re.IGNORECASE,
)


def _extract_calls(raw_ddl: str) -> list[str]:
    """Extract normalized procedure calls from EXEC statements in raw DDL.

    Skips dynamic SQL patterns (EXEC(@var), sp_executesql).
    """
    calls: set[str] = set()
    for m in _EXEC_PROC_RE.finditer(raw_ddl):
        target = m.group(1).strip()
        # Skip if the match is actually part of a dynamic SQL pattern
        start = m.start()
        context = raw_ddl[max(0, start - 5):m.end() + 20]
        if _DYNAMIC_EXEC_RE.search(context):
            continue
        calls.add(normalize(target))
    return sorted(calls)


def _table_in_scope(
    in_scope_list: list[dict[str, Any]], table_fqn: str,
) -> bool:
    """Check if a table FQN is already in an in_scope list."""
    for entry in in_scope_list:
        entry_fqn = normalize(f"{entry['schema']}.{entry['name']}")
        if entry_fqn == table_fqn:
            return True
    return False


def _proc_in_scope(
    in_scope_list: list[dict[str, Any]], proc_fqn: str,
) -> bool:
    """Check if a procedure FQN is already in an in_scope list."""
    for entry in in_scope_list:
        entry_fqn = normalize(f"{entry['schema']}.{entry['name']}")
        if entry_fqn == proc_fqn:
            return True
    return False


def _fqn_parts(fqn: str) -> tuple[str, str]:
    """Split a normalized FQN into (schema, name)."""
    parts = fqn.split(".")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return "dbo", parts[-1]


def _make_ast_ref_entry(
    schema: str,
    name: str,
    *,
    is_updated: bool = False,
    is_selected: bool = False,
) -> dict[str, Any]:
    """Build a reference entry dict tagged with ast_scan detection."""
    return {
        "schema": schema,
        "name": name,
        "is_selected": is_selected,
        "is_updated": is_updated,
        "detection": "ast_scan",
    }


from shared.catalog import _empty_scoped


def _ensure_references(data: dict[str, Any]) -> dict[str, Any]:
    """Ensure a proc catalog dict has a full references structure."""
    if "references" not in data:
        data["references"] = {
            "tables": _empty_scoped(),
            "views": _empty_scoped(),
            "functions": _empty_scoped(),
            "procedures": _empty_scoped(),
        }
    refs = data["references"]
    for bucket in ("tables", "views", "functions", "procedures"):
        if bucket not in refs:
            refs[bucket] = _empty_scoped()
        if "in_scope" not in refs[bucket]:
            refs[bucket]["in_scope"] = []
        if "out_of_scope" not in refs[bucket]:
            refs[bucket]["out_of_scope"] = []
    return data


def _ensure_referenced_by(data: dict[str, Any]) -> dict[str, Any]:
    """Ensure a table catalog dict has a full referenced_by structure."""
    if "referenced_by" not in data:
        data["referenced_by"] = {
            "procedures": _empty_scoped(),
            "views": _empty_scoped(),
            "functions": _empty_scoped(),
        }
    ref_by = data["referenced_by"]
    for bucket in ("procedures", "views", "functions"):
        if bucket not in ref_by:
            ref_by[bucket] = _empty_scoped()
        if "in_scope" not in ref_by[bucket]:
            ref_by[bucket]["in_scope"] = []
        if "out_of_scope" not in ref_by[bucket]:
            ref_by[bucket]["out_of_scope"] = []
    return data


def enrich_catalog(ddl_path: Path, dialect: str = "tsql") -> dict[str, Any]:
    """Enrich catalog files with AST-derived references.

    Returns summary: {"tables_augmented": N, "procedures_augmented": N, "entries_added": N}
    """
    ddl_path = Path(ddl_path)

    if not has_catalog(ddl_path):
        logger.warning("event=enrich_catalog status=skip reason=no_catalog path=%s", ddl_path)
        return {"tables_augmented": 0, "procedures_augmented": 0, "entries_added": 0}

    # Phase 1: Load DDL catalog and extract AST refs for eligible procedures.
    # Skip procs marked needs_llm — sqlglot cannot parse them; discover show handles them.
    ddl_catalog = load_directory(ddl_path, dialect=dialect)

    ast_refs: dict[str, ObjectRefs] = {}  # proc_fqn → ObjectRefs
    ast_calls: dict[str, list[str]] = {}  # proc_fqn → list of called proc FQNs

    for proc_fqn, entry in ddl_catalog.procedures.items():
        proc_cat = load_proc_catalog(ddl_path, proc_fqn)
        if proc_cat and proc_cat.get("needs_llm"):
            logger.debug("event=enrich_skip proc=%s reason=needs_llm", proc_fqn)
            continue

        try:
            refs = extract_refs(entry)
            ast_refs[proc_fqn] = refs
        except DdlParseError as exc:
            logger.debug("event=enrich_skip proc=%s reason=parse_error error=%s", proc_fqn, exc)
            continue

        # Extract EXEC calls to other procedures
        calls = _extract_calls(entry.raw_ddl)
        # Filter to only known procedures
        known_calls = [c for c in calls if c in ddl_catalog.procedures]
        ast_calls[proc_fqn] = known_calls

    # Phase 2: Build direct writer map from AST
    # proc_fqn → set of table FQNs it writes to (per AST)
    direct_writers: dict[str, set[str]] = {}
    for proc_fqn, refs in ast_refs.items():
        if refs.writes_to:
            direct_writers[proc_fqn] = set(refs.writes_to)

    # Phase 3: BFS for indirect writers through call chains
    # For each proc, traverse its call graph to find tables written
    # by callees (directly or transitively).
    indirect_writers: dict[str, set[str]] = {}  # proc_fqn → set of table FQNs

    for proc_fqn in ast_refs:
        visited: set[str] = {proc_fqn}
        queue: deque[str] = deque(ast_calls.get(proc_fqn, []))
        indirect_tables: set[str] = set()

        while queue:
            callee = queue.popleft()
            if callee in visited:
                continue
            visited.add(callee)

            # Add callee's direct writes
            if callee in direct_writers:
                indirect_tables.update(direct_writers[callee])

            # Continue BFS through callee's calls
            for next_callee in ast_calls.get(callee, []):
                if next_callee not in visited:
                    queue.append(next_callee)

        # Only keep tables NOT already directly written by this proc
        own_writes = direct_writers.get(proc_fqn, set())
        new_indirect = indirect_tables - own_writes
        if new_indirect:
            indirect_writers[proc_fqn] = new_indirect

    # Phase 4: Compare AST findings with existing catalog and augment
    tables_augmented: set[str] = set()
    procedures_augmented: set[str] = set()
    entries_added = 0

    # Load all existing proc catalogs, augment, and write back
    all_writer_procs = set(direct_writers.keys()) | set(indirect_writers.keys())

    for proc_fqn in sorted(all_writer_procs):
        proc_data = load_proc_catalog(ddl_path, proc_fqn)
        if proc_data is None:
            # No catalog file for this proc — create one with AST refs
            proc_data = {}
        proc_data = _ensure_references(proc_data)

        tables_in_scope = proc_data["references"]["tables"]["in_scope"]
        proc_modified = False

        # Check direct writes from AST
        for table_fqn in sorted(direct_writers.get(proc_fqn, set())):
            if not _table_in_scope(tables_in_scope, table_fqn):
                schema, name = _fqn_parts(table_fqn)
                tables_in_scope.append(_make_ast_ref_entry(
                    schema, name, is_updated=True,
                ))
                proc_modified = True
                entries_added += 1
                logger.info(
                    "event=enrich_add_direct proc=%s table=%s detection=ast_scan",
                    proc_fqn, table_fqn,
                )

        # Check indirect writes from BFS
        for table_fqn in sorted(indirect_writers.get(proc_fqn, set())):
            if not _table_in_scope(tables_in_scope, table_fqn):
                schema, name = _fqn_parts(table_fqn)
                tables_in_scope.append(_make_ast_ref_entry(
                    schema, name, is_updated=True,
                ))
                proc_modified = True
                entries_added += 1
                logger.info(
                    "event=enrich_add_indirect proc=%s table=%s detection=ast_scan",
                    proc_fqn, table_fqn,
                )

        if proc_modified:
            procedures_augmented.add(proc_fqn)
            # Sort in_scope for deterministic output
            tables_in_scope.sort(
                key=lambda e: f"{e['schema']}.{e['name']}".lower(),
            )
            write_object_catalog(
                ddl_path, "procedures", proc_fqn,
                proc_data["references"],
                needs_llm=proc_data.get("needs_llm", False),
                needs_enrich=False,  # enrichment complete
            )

    # Phase 5: Flip — update table catalogs with new referenced_by entries
    for proc_fqn in sorted(procedures_augmented):
        proc_data = load_proc_catalog(ddl_path, proc_fqn)
        if proc_data is None:
            continue
        tables_in_scope = proc_data.get("references", {}).get("tables", {}).get("in_scope", [])

        for table_entry in tables_in_scope:
            if table_entry.get("detection") != "ast_scan":
                continue

            table_fqn = normalize(f"{table_entry['schema']}.{table_entry['name']}")
            table_data = load_table_catalog(ddl_path, table_fqn)
            if table_data is None:
                # Create a minimal table catalog entry
                table_data = {}
            table_data = _ensure_referenced_by(table_data)

            procs_in_scope = table_data["referenced_by"]["procedures"]["in_scope"]
            proc_schema, proc_name = _fqn_parts(proc_fqn)

            if not _proc_in_scope(procs_in_scope, proc_fqn):
                procs_in_scope.append(_make_ast_ref_entry(
                    proc_schema, proc_name, is_updated=True,
                ))
                procs_in_scope.sort(
                    key=lambda e: f"{e['schema']}.{e['name']}".lower(),
                )
                tables_augmented.add(table_fqn)

            # Write table catalog — preserve existing signals
            ref_by = table_data.pop("referenced_by", None)
            write_table_catalog(ddl_path, table_fqn, table_data, ref_by)

    summary = {
        "tables_augmented": len(tables_augmented),
        "procedures_augmented": len(procedures_augmented),
        "entries_added": entries_added,
    }
    logger.info("event=enrich_catalog_complete %s", summary)
    return summary


@app.command()
def main(
    ddl_path: Path = typer.Option(..., help="Path to DDL directory with catalog/"),
    dialect: str = typer.Option("tsql", help="SQL dialect"),
) -> None:
    """Augment catalog files with AST-derived references."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s: %(message)s",
        stream=sys.stderr,
    )
    result = enrich_catalog(ddl_path, dialect)
    json.dump(result, sys.stdout, indent=2)
    print(file=sys.stdout)  # trailing newline


if __name__ == "__main__":
    app()
