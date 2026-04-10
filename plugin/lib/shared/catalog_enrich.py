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
from typing import Any, Optional

import typer

from shared.output_models import CatalogEnrichOutput

from shared.catalog import (
    ensure_referenced_by,
    ensure_references,
    has_catalog,
    load_proc_catalog,
    load_table_catalog,
    write_json,
    write_table_catalog,
)
from shared.loader import (
    CatalogLoadError,
    DdlCatalog,
    DdlParseError,
    ObjectRefs,
    extract_refs,
    load_directory,
)
from shared.env_config import resolve_catalog_dir, resolve_project_root
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

from shared.routing import DYNAMIC_EXEC_BROAD_RE as _DYNAMIC_EXEC_RE


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


def _enrich_skip_reason(proc_cat: Any) -> str | None:
    """Return the reason enrichment should skip this procedure, if any."""
    if proc_cat is None:
        return None

    mode = proc_cat.mode if hasattr(proc_cat, "mode") else proc_cat.get("mode")
    if mode == "llm_required":
        return "needs_llm"
    return None


def _fqn_in_scope(
    in_scope_list: list[dict[str, Any]], fqn: str,
) -> bool:
    """Check if a normalized FQN is already in an in_scope list."""
    return any(
        normalize(f"{e['schema']}.{e['name']}") == fqn
        for e in in_scope_list
    )


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

    Skips only procs that require LLM routing.
    Returns ``(ast_refs, ast_calls)``.
    """
    ast_refs: dict[str, ObjectRefs] = {}
    ast_calls: dict[str, list[str]] = {}

    for proc_fqn, entry in ddl_catalog.procedures.items():
        try:
            proc_cat = load_proc_catalog(project_root, proc_fqn)
        except CatalogLoadError as exc:
            logger.warning("event=enrich_skip proc=%s reason=corrupt_catalog error=%s", proc_fqn, exc)
            continue
        skip_reason = _enrich_skip_reason(proc_cat)
        if skip_reason is not None:
            logger.debug("event=enrich_skip proc=%s reason=%s", proc_fqn, skip_reason)
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
        try:
            proc_cat = load_proc_catalog(project_root, proc_fqn)
        except CatalogLoadError as exc:
            logger.warning("event=enrich_skip proc=%s reason=corrupt_catalog error=%s", proc_fqn, exc)
            continue
        if proc_cat is None:
            logger.warning(
                "event=enrich_skip proc=%s reason=missing_catalog_on_write",
                proc_fqn,
            )
            continue
        else:
            proc_data = proc_cat.model_dump(exclude_none=True, by_alias=True)
        proc_data = ensure_references(proc_data)

        tables_in_scope = proc_data["references"]["tables"]["in_scope"]
        proc_modified = False

        for table_fqn in sorted(direct_writers.get(proc_fqn, set())):
            if not _fqn_in_scope(tables_in_scope, table_fqn):
                schema, name = fqn_parts(table_fqn)
                tables_in_scope.append(_make_ast_ref_entry(schema, name, is_updated=True))
                proc_modified = True
                entries_added += 1
                logger.info("event=enrich_add_direct proc=%s table=%s detection=ast_scan", proc_fqn, table_fqn)

        for table_fqn in sorted(indirect_writers.get(proc_fqn, set())):
            if not _fqn_in_scope(tables_in_scope, table_fqn):
                schema, name = fqn_parts(table_fqn)
                tables_in_scope.append(_make_ast_ref_entry(schema, name, is_updated=True))
                proc_modified = True
                entries_added += 1
                logger.info("event=enrich_add_indirect proc=%s table=%s detection=ast_scan", proc_fqn, table_fqn)

        if proc_modified:
            procedures_augmented.add(proc_fqn)
            tables_in_scope.sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
            proc_data["needs_enrich"] = False
            cat_path = resolve_catalog_dir(project_root) / "procedures" / f"{normalize(proc_fqn)}.json"
            write_json(cat_path, proc_data)

    return procedures_augmented, entries_added


def _flip_to_table_catalogs(
    project_root: Path, procedures_augmented: set[str],
) -> set[str]:
    """Update table catalogs with reverse references from augmented procs.

    Returns set of table FQNs that were augmented.
    """
    tables_augmented: set[str] = set()

    for proc_fqn in sorted(procedures_augmented):
        try:
            proc_cat = load_proc_catalog(project_root, proc_fqn)
        except CatalogLoadError as exc:
            logger.warning("event=enrich_skip proc=%s reason=corrupt_catalog error=%s", proc_fqn, exc)
            continue
        if proc_cat is None:
            continue
        proc_data = proc_cat.model_dump(exclude_none=True, by_alias=True)
        tables_in_scope = proc_data.get("references", {}).get("tables", {}).get("in_scope", [])

        for table_entry in tables_in_scope:
            if table_entry.get("detection") != "ast_scan":
                continue

            table_fqn = normalize(f"{table_entry['schema']}.{table_entry['name']}")
            try:
                table_cat = load_table_catalog(project_root, table_fqn)
            except CatalogLoadError as exc:
                logger.warning("event=enrich_skip table=%s reason=corrupt_catalog error=%s", table_fqn, exc)
                continue
            if table_cat is None:
                table_data: dict[str, Any] = {}
            else:
                table_data = table_cat.model_dump(exclude_none=True, by_alias=True)
            table_data = ensure_referenced_by(table_data)

            procs_in_scope = table_data["referenced_by"]["procedures"]["in_scope"]
            proc_schema, proc_name = fqn_parts(proc_fqn)

            if not _fqn_in_scope(procs_in_scope, proc_fqn):
                procs_in_scope.append(_make_ast_ref_entry(proc_schema, proc_name, is_updated=True))
                procs_in_scope.sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
                tables_augmented.add(table_fqn)

            ref_by = table_data.pop("referenced_by", None)
            write_table_catalog(project_root, table_fqn, table_data, ref_by)

    return tables_augmented


def enrich_catalog(project_root: Path, dialect: str = "tsql") -> CatalogEnrichOutput:
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
        return CatalogEnrichOutput(tables_augmented=0, procedures_augmented=0, entries_added=0)

    ddl_catalog = load_directory(project_root, dialect=dialect)

    ast_refs, ast_calls = _scan_ast_refs(project_root, ddl_catalog)
    direct_writers, indirect_writers = _build_writer_maps(ast_refs, ast_calls)
    procedures_augmented, entries_added = _augment_proc_catalogs(project_root, direct_writers, indirect_writers)
    tables_augmented = _flip_to_table_catalogs(project_root, procedures_augmented)

    summary = CatalogEnrichOutput(
        tables_augmented=len(tables_augmented),
        procedures_augmented=len(procedures_augmented),
        entries_added=entries_added,
    )
    logger.info("event=enrich_catalog_complete %s", summary)
    return summary


@app.command()
def main(
    project_root: Optional[Path] = typer.Option(None, "--project-root", help="Root artifacts directory containing ddl/, catalog/, and manifest.json (defaults to current working directory)"),
    dialect: Optional[str] = typer.Option(None, "--dialect", help="SQL dialect (default: read from manifest.json)"),
) -> None:
    """Augment catalog files with AST-derived references."""
    project_root = resolve_project_root(project_root)
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s: %(message)s",
        stream=sys.stderr,
    )
    if dialect is None:
        manifest_path = project_root / "manifest.json"
        if not manifest_path.exists():
            raise typer.BadParameter(
                "manifest.json not found and --dialect was not provided. "
                "Run write-manifest first or pass --dialect explicitly.",
                param_hint="--dialect",
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        dialect = manifest["dialect"]
    result = enrich_catalog(project_root, dialect)
    json.dump(result.model_dump(mode="json", exclude_none=True), sys.stdout, indent=2)
    print(file=sys.stdout)  # trailing newline


if __name__ == "__main__":
    app()
