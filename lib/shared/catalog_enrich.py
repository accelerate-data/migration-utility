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
    ensure_referenced_by,
    ensure_references,
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    write_object_catalog,
    write_table_catalog,
)
from shared.loader import (
    DdlCatalog,
    DdlParseError,
    ObjectRefs,
    extract_refs,
    load_directory,
)
from shared.name_resolver import fqn_parts, normalize

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




def _scan_ast_refs(
    project_root: Path, ddl_catalog: DdlCatalog,
) -> tuple[dict[str, ObjectRefs], dict[str, list[str]]]:
    """Extract AST refs and EXEC calls for eligible procedures.

    Skips procs with ``needs_llm: true`` or ``needs_enrich: false``.
    Returns ``(ast_refs, ast_calls)``.
    """
    ast_refs: dict[str, ObjectRefs] = {}
    ast_calls: dict[str, list[str]] = {}

    for proc_fqn, entry in ddl_catalog.procedures.items():
        proc_cat = load_proc_catalog(project_root, proc_fqn)
        if proc_cat and proc_cat.get("needs_llm"):
            logger.debug("event=enrich_skip proc=%s reason=needs_llm", proc_fqn)
            continue
        if proc_cat and not proc_cat.get("needs_enrich", True):
            logger.debug("event=enrich_skip proc=%s reason=already_enriched", proc_fqn)
            continue

        try:
            refs = extract_refs(entry)
            ast_refs[proc_fqn] = refs
        except DdlParseError as exc:
            logger.debug("event=enrich_skip proc=%s reason=parse_error error=%s", proc_fqn, exc)
            continue

        calls = _extract_calls(entry.raw_ddl)
        ast_calls[proc_fqn] = [c for c in calls if c in ddl_catalog.procedures]

    return ast_refs, ast_calls


def _build_writer_maps(
    ast_refs: dict[str, ObjectRefs],
    ast_calls: dict[str, list[str]],
) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Build direct and BFS-indirect writer maps.

    Returns ``(direct_writers, indirect_writers)`` where each maps
    ``proc_fqn → set[table_fqn]``.
    """
    direct_writers: dict[str, set[str]] = {}
    for proc_fqn, refs in ast_refs.items():
        if refs.writes_to:
            direct_writers[proc_fqn] = set(refs.writes_to)

    indirect_writers: dict[str, set[str]] = {}
    for proc_fqn in ast_refs:
        visited: set[str] = {proc_fqn}
        queue: deque[str] = deque(ast_calls.get(proc_fqn, []))
        indirect_tables: set[str] = set()

        while queue:
            callee = queue.popleft()
            if callee in visited:
                continue
            visited.add(callee)
            if callee in direct_writers:
                indirect_tables.update(direct_writers[callee])
            for next_callee in ast_calls.get(callee, []):
                if next_callee not in visited:
                    queue.append(next_callee)

        new_indirect = indirect_tables - direct_writers.get(proc_fqn, set())
        if new_indirect:
            indirect_writers[proc_fqn] = new_indirect

    return direct_writers, indirect_writers


def _augment_proc_catalogs(
    project_root: Path,
    direct_writers: dict[str, set[str]],
    indirect_writers: dict[str, set[str]],
) -> tuple[set[str], int]:
    """Write AST-discovered refs into proc catalog files.

    Returns ``(procedures_augmented, entries_added)``.
    """
    procedures_augmented: set[str] = set()
    entries_added = 0
    all_writer_procs = set(direct_writers.keys()) | set(indirect_writers.keys())

    for proc_fqn in sorted(all_writer_procs):
        proc_data = load_proc_catalog(project_root, proc_fqn)
        if proc_data is None:
            proc_data = {}
        proc_data = ensure_references(proc_data)

        tables_in_scope = proc_data["references"]["tables"]["in_scope"]
        proc_modified = False

        for table_fqn in sorted(direct_writers.get(proc_fqn, set())):
            if not _table_in_scope(tables_in_scope, table_fqn):
                schema, name = fqn_parts(table_fqn)
                tables_in_scope.append(_make_ast_ref_entry(schema, name, is_updated=True))
                proc_modified = True
                entries_added += 1
                logger.info("event=enrich_add_direct proc=%s table=%s detection=ast_scan", proc_fqn, table_fqn)

        for table_fqn in sorted(indirect_writers.get(proc_fqn, set())):
            if not _table_in_scope(tables_in_scope, table_fqn):
                schema, name = fqn_parts(table_fqn)
                tables_in_scope.append(_make_ast_ref_entry(schema, name, is_updated=True))
                proc_modified = True
                entries_added += 1
                logger.info("event=enrich_add_indirect proc=%s table=%s detection=ast_scan", proc_fqn, table_fqn)

        if proc_modified:
            procedures_augmented.add(proc_fqn)
            tables_in_scope.sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
            write_object_catalog(
                project_root, "procedures", proc_fqn, proc_data["references"],
                needs_llm=proc_data.get("needs_llm", False), needs_enrich=False,
            )

    return procedures_augmented, entries_added


def _flip_to_table_catalogs(
    project_root: Path, procedures_augmented: set[str],
) -> set[str]:
    """Update table catalogs with reverse references from augmented procs.

    Returns set of table FQNs that were augmented.
    """
    tables_augmented: set[str] = set()

    for proc_fqn in sorted(procedures_augmented):
        proc_data = load_proc_catalog(project_root, proc_fqn)
        if proc_data is None:
            continue
        tables_in_scope = proc_data.get("references", {}).get("tables", {}).get("in_scope", [])

        for table_entry in tables_in_scope:
            if table_entry.get("detection") != "ast_scan":
                continue

            table_fqn = normalize(f"{table_entry['schema']}.{table_entry['name']}")
            table_data = load_table_catalog(project_root, table_fqn)
            if table_data is None:
                table_data = {}
            table_data = ensure_referenced_by(table_data)

            procs_in_scope = table_data["referenced_by"]["procedures"]["in_scope"]
            proc_schema, proc_name = fqn_parts(proc_fqn)

            if not _proc_in_scope(procs_in_scope, proc_fqn):
                procs_in_scope.append(_make_ast_ref_entry(proc_schema, proc_name, is_updated=True))
                procs_in_scope.sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
                tables_augmented.add(table_fqn)

            ref_by = table_data.pop("referenced_by", None)
            write_table_catalog(project_root, table_fqn, table_data, ref_by)

    return tables_augmented


def enrich_catalog(project_root: Path, dialect: str = "tsql") -> dict[str, Any]:
    """Enrich catalog files with AST-derived references.

    Args:
        project_root: Root artifacts directory containing ``ddl/``, ``catalog/``,
            and ``manifest.json``.
        dialect: SQL dialect for parsing (default: "tsql").

    Returns summary: {"tables_augmented": N, "procedures_augmented": N, "entries_added": N}
    """
    project_root = Path(project_root)

    if not has_catalog(project_root):
        logger.warning("event=enrich_catalog status=skip reason=no_catalog path=%s", project_root)
        return {"tables_augmented": 0, "procedures_augmented": 0, "entries_added": 0}

    ddl_catalog = load_directory(project_root, dialect=dialect)

    ast_refs, ast_calls = _scan_ast_refs(project_root, ddl_catalog)
    direct_writers, indirect_writers = _build_writer_maps(ast_refs, ast_calls)
    procedures_augmented, entries_added = _augment_proc_catalogs(project_root, direct_writers, indirect_writers)
    tables_augmented = _flip_to_table_catalogs(project_root, procedures_augmented)

    summary = {
        "tables_augmented": len(tables_augmented),
        "procedures_augmented": len(procedures_augmented),
        "entries_added": entries_added,
    }
    logger.info("event=enrich_catalog_complete %s", summary)
    return summary


@app.command()
def main(
    project_root: Path = typer.Option(..., "--project-root", help="Root artifacts directory containing ddl/, catalog/, and manifest.json"),
    dialect: str = typer.Option("tsql", help="SQL dialect"),
) -> None:
    """Augment catalog files with AST-derived references."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s: %(message)s",
        stream=sys.stderr,
    )
    result = enrich_catalog(project_root, dialect)
    json.dump(result, sys.stdout, indent=2)
    print(file=sys.stdout)  # trailing newline


if __name__ == "__main__":
    app()
