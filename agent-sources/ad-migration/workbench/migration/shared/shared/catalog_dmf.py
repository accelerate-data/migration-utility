"""DMF result processing and catalog file orchestration.

Processes raw rows from ``sys.dm_sql_referenced_entities``, groups them into
per-object reference dicts with ``in_scope``/``out_of_scope`` scoping, flips
outbound references to build ``referenced_by`` for tables, and orchestrates
writing all catalog JSON files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from shared.catalog import (
    _empty_scoped,
    process_dmf_results,
    write_object_catalog,
    write_table_catalog,
    flip_references,
)
from shared.name_resolver import normalize


def _write_object_catalogs(
    ddl_path: Path,
    dmf_refs: dict[str, dict],
    object_type: str,
    rflags: dict[str, dict[str, bool]],
    pparams: dict[str, list[dict[str, Any]]],
    object_types: dict[str, str] | None,
) -> int:
    """Write catalog files for one object type (procs, views, or functions).

    Includes backfill for objects with no DMF refs.
    Returns count of files written.
    """
    def _empty_refs() -> dict[str, dict[str, list[dict[str, Any]]]]:
        return {"tables": _empty_scoped(), "views": _empty_scoped(),
                "functions": _empty_scoped(), "procedures": _empty_scoped()}

    count = 0
    for fqn, refs in dmf_refs.items():
        params = pparams.get(fqn) if object_type == "procedures" else None
        write_object_catalog(ddl_path, object_type, fqn, refs, **rflags.get(fqn, {}), params=params)
        count += 1

    for fqn, bucket in (object_types or {}).items():
        if bucket == object_type and fqn not in dmf_refs:
            params = pparams.get(fqn) if object_type == "procedures" else None
            write_object_catalog(ddl_path, object_type, fqn, _empty_refs(), **rflags.get(fqn, {}), params=params)
            count += 1

    return count


def _build_table_referenced_by(
    proc_refs: dict, view_refs: dict, func_refs: dict,
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Flip proc/view/func outbound refs and merge into per-table referenced_by."""
    table_referenced_by: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for referencing_type, refs_dict in [
        ("procedures", proc_refs), ("views", view_refs), ("functions", func_refs),
    ]:
        flipped = flip_references(refs_dict, referencing_type)
        for table_fqn, ref_by in flipped.items():
            if table_fqn not in table_referenced_by:
                table_referenced_by[table_fqn] = {
                    "procedures": _empty_scoped(), "views": _empty_scoped(), "functions": _empty_scoped(),
                }
            for bucket_name in ("procedures", "views", "functions"):
                table_referenced_by[table_fqn][bucket_name]["in_scope"].extend(
                    ref_by.get(bucket_name, {}).get("in_scope", [])
                )
    for ref_by in table_referenced_by.values():
        for scoped in ref_by.values():
            scoped["in_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
    return table_referenced_by


def write_catalog_files(
    ddl_path: Path,
    table_signals: dict[str, dict[str, Any]],
    proc_dmf_rows: list[dict[str, Any]],
    view_dmf_rows: list[dict[str, Any]],
    func_dmf_rows: list[dict[str, Any]],
    object_types: dict[str, str] | None = None,
    routing_flags: dict[str, dict[str, bool]] | None = None,
    database: str = "",
    proc_params: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, int]:
    """Process raw extraction data and write all catalog JSON files.

    Returns counts: ``{tables: N, procedures: N, views: N, functions: N}``.
    """
    rflags = routing_flags or {}
    pparams = proc_params or {}

    proc_refs = process_dmf_results(proc_dmf_rows, object_types, database=database)
    view_refs = process_dmf_results(view_dmf_rows, object_types, database=database)
    func_refs = process_dmf_results(func_dmf_rows, object_types, database=database)

    counts = {
        "procedures": _write_object_catalogs(ddl_path, proc_refs, "procedures", rflags, pparams, object_types),
        "views": _write_object_catalogs(ddl_path, view_refs, "views", rflags, pparams, object_types),
        "functions": _write_object_catalogs(ddl_path, func_refs, "functions", rflags, pparams, object_types),
    }

    table_referenced_by = _build_table_referenced_by(proc_refs, view_refs, func_refs)

    known_tables = {fqn for fqn, bucket in (object_types or {}).items() if bucket == "tables"}
    all_table_fqns = set(table_signals.keys()) | set(table_referenced_by.keys()) | known_tables
    counts["tables"] = 0
    for table_fqn in sorted(all_table_fqns):
        write_table_catalog(ddl_path, table_fqn, table_signals.get(table_fqn, {}), table_referenced_by.get(table_fqn))
        counts["tables"] += 1

    return counts
