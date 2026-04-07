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
    write_object_catalog,
    write_table_catalog,
)
from shared.dmf_processing import (
    empty_scoped,
    flip_references,
    process_dmf_results,
)
from shared.name_resolver import normalize


def _empty_refs() -> dict[str, dict[str, list[dict[str, Any]]]]:
    return {"tables": empty_scoped(), "views": empty_scoped(),
            "functions": empty_scoped(), "procedures": empty_scoped()}


def _write_object_catalogs(
    project_root: Path,
    dmf_refs: dict[str, dict],
    object_type: str,
    rflags: dict[str, dict[str, bool]],
    pparams: dict[str, list[dict[str, Any]]],
    object_types: dict[str, str] | None,
    *,
    write_filter: set[str] | None = None,
    hashes: dict[str, str] | None = None,
    view_definitions: dict[str, str] | None = None,
    view_columns: dict[str, list[dict[str, Any]]] | None = None,
    mv_fqns: set[str] | None = None,
    dmf_errors: dict[str, list[str]] | None = None,
    subtypes: dict[str, str] | None = None,
    segmenter_errors: dict[str, str] | None = None,
) -> int:
    """Write catalog files for one object type (procs, views, or functions).

    Includes backfill for objects with no DMF refs.
    When *write_filter* is set, only FQNs in the filter are written.
    *view_definitions* and *view_columns* are only applied when object_type == "views".
    Returns count of files written.
    """
    _hashes = hashes or {}
    _vdefs = view_definitions if object_type == "views" else None
    _vcols = view_columns if object_type == "views" else None
    _mv = mv_fqns or set()
    _dmf_errors = dmf_errors or {}
    _subtypes = subtypes or {}
    _seg_errors = segmenter_errors or {}

    count = 0
    for fqn, refs in dmf_refs.items():
        if write_filter is not None and fqn not in write_filter:
            continue
        params = pparams.get(fqn) if object_type == "procedures" else None
        write_object_catalog(
            project_root, object_type, fqn, refs,
            **rflags.get(fqn, {}), params=params, ddl_hash=_hashes.get(fqn),
            sql=_vdefs.get(fqn) if _vdefs else None,
            columns=_vcols.get(fqn) if _vcols else None,
            is_materialized_view=(object_type == "views" and fqn in _mv),
            dmf_errors=_dmf_errors.get(fqn),
            subtype=_subtypes.get(fqn),
            segmenter_error=_seg_errors.get(fqn),
        )
        count += 1

    for fqn, bucket in (object_types or {}).items():
        if bucket == object_type and fqn not in dmf_refs:
            if write_filter is not None and fqn not in write_filter:
                continue
            params = pparams.get(fqn) if object_type == "procedures" else None
            write_object_catalog(
                project_root, object_type, fqn, _empty_refs(),
                **rflags.get(fqn, {}), params=params, ddl_hash=_hashes.get(fqn),
                sql=_vdefs.get(fqn) if _vdefs else None,
                columns=_vcols.get(fqn) if _vcols else None,
                is_materialized_view=(object_type == "views" and fqn in _mv),
                dmf_errors=_dmf_errors.get(fqn),
                subtype=_subtypes.get(fqn),
                segmenter_error=_seg_errors.get(fqn),
            )
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
                    "procedures": empty_scoped(), "views": empty_scoped(), "functions": empty_scoped(),
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
    project_root: Path,
    table_signals: dict[str, dict[str, Any]],
    proc_dmf_rows: list[dict[str, Any]],
    view_dmf_rows: list[dict[str, Any]],
    func_dmf_rows: list[dict[str, Any]],
    object_types: dict[str, str] | None = None,
    routing_flags: dict[str, dict[str, bool]] | None = None,
    database: str = "",
    proc_params: dict[str, list[dict[str, Any]]] | None = None,
    write_filter: set[str] | None = None,
    hashes: dict[str, str] | None = None,
    view_definitions: dict[str, str] | None = None,
    view_columns: dict[str, list[dict[str, Any]]] | None = None,
    mv_fqns: set[str] | None = None,
    subtypes: dict[str, str] | None = None,
    segmenter_errors: dict[str, str] | None = None,
) -> dict[str, int]:
    """Process raw extraction data and write all catalog JSON files.

    When *write_filter* is set, only FQNs in the filter are written (changed +
    new objects for diff-aware reexport).  Pass ``None`` to write everything
    (backward-compatible default).

    *view_definitions* maps normalized view FQN → raw DDL string (from
    OBJECT_DEFINITION).  *view_columns* maps normalized view FQN → list of
    column dicts (from sys.columns).  Both are written into view catalog files
    as ``sql`` and ``columns`` fields when provided.

    Returns counts: ``{tables: N, procedures: N, views: N, functions: N}``.
    """
    rflags = routing_flags or {}
    pparams = proc_params or {}

    proc_refs, proc_dmf_errs = process_dmf_results(proc_dmf_rows, object_types, database=database)
    view_refs, view_dmf_errs = process_dmf_results(view_dmf_rows, object_types, database=database)
    func_refs, func_dmf_errs = process_dmf_results(func_dmf_rows, object_types, database=database)

    # Merge all DMF errors into a single dict for downstream consumers
    all_dmf_errors: dict[str, list[str]] = {}
    for errs in (proc_dmf_errs, view_dmf_errs, func_dmf_errs):
        for fqn, err_list in errs.items():
            all_dmf_errors.setdefault(fqn, []).extend(err_list)

    counts = {
        "procedures": _write_object_catalogs(
            project_root, proc_refs, "procedures", rflags, pparams, object_types,
            write_filter=write_filter, hashes=hashes,
            dmf_errors=all_dmf_errors, segmenter_errors=segmenter_errors,
        ),
        "views": _write_object_catalogs(
            project_root, view_refs, "views", rflags, pparams, object_types,
            write_filter=write_filter, hashes=hashes,
            view_definitions=view_definitions,
            view_columns=view_columns,
            mv_fqns=mv_fqns,
            dmf_errors=all_dmf_errors,
        ),
        "functions": _write_object_catalogs(
            project_root, func_refs, "functions", rflags, pparams, object_types,
            write_filter=write_filter, hashes=hashes,
            dmf_errors=all_dmf_errors, subtypes=subtypes,
        ),
    }

    table_referenced_by = _build_table_referenced_by(proc_refs, view_refs, func_refs)
    _hashes = hashes or {}

    known_tables = {fqn for fqn, bucket in (object_types or {}).items() if bucket == "tables"}
    all_table_fqns = set(table_signals.keys()) | set(table_referenced_by.keys()) | known_tables
    counts["tables"] = 0
    for table_fqn in sorted(all_table_fqns):
        if write_filter is not None and table_fqn not in write_filter:
            continue
        write_table_catalog(
            project_root, table_fqn,
            table_signals.get(table_fqn, {}),
            table_referenced_by.get(table_fqn),
            ddl_hash=_hashes.get(table_fqn),
        )
        counts["tables"] += 1

    return counts
