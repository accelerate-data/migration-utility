"""Catalog JSON file I/O for per-object metadata extracted from sys.* views.

Reads and writes the ``catalog/`` subdirectory that setup-ddl and export_ddl
produce alongside the flat ``.sql`` DDL files.  Each object gets its own JSON
file keyed by normalized ``schema.name``.

Layout::

    <ddl-output-dir>/
    └── catalog/
        ├── tables/<schema>.<table>.json
        ├── procedures/<schema>.<proc>.json
        ├── views/<schema>.<view>.json
        └── functions/<schema>.<function>.json

Table files carry catalog signals (PKs, FKs, identity, CDC, sensitivity) plus
``referenced_by`` (inbound references flipped from proc/view/function DMF data).
Proc/view/function files carry ``references`` (outbound references from the DMF).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from shared.name_resolver import normalize

# ── Routing flag patterns ────────────────────────────────────────────────────

_NEEDS_LLM_RE = re.compile(
    r"\bEXEC(?:UTE)?\s*\("    # EXEC(@sql) — dynamic execution
    r"|\bBEGIN\s+TRY\b"       # TRY/CATCH block
    r"|\bWHILE\b"             # WHILE loop
    r"|\bIF\b",               # IF/ELSE branch
    re.IGNORECASE,
)

_NEEDS_ENRICH_RE = re.compile(
    r"^(?!.*\bINSERT\b).*\bINTO\s+[\[\w#@]"                  # SELECT INTO (excludes INSERT INTO lines)
    r"|\bTRUNCATE\b"                                          # TRUNCATE TABLE
    r"|\bEXEC(?:UTE)?\s+(?!sp_executesql\b)(?![@(])[\[\w]",  # static EXEC (not dynamic, not sp_executesql)
    re.IGNORECASE | re.MULTILINE,
)


# ── Schemas (TypedDict-style, but plain dicts in practice) ──────────────────
#
# We define the shapes here as documentation.  At runtime everything is
# ``dict[str, Any]`` — no Pydantic overhead for what are pure serialisation
# containers.
#
# ReferenceEntry:
#   schema: str
#   name: str
#   is_selected: bool
#   is_updated: bool
#   is_insert_all: bool          (procs only)
#   columns: list[ColumnRef]     (optional)
#
# ColumnRef:
#   name: str
#   is_selected: bool
#   is_updated: bool
#
# TableCatalog:
#   primary_keys: list[PrimaryKey]
#   unique_indexes: list[UniqueIndex]
#   foreign_keys: list[ForeignKey]
#   auto_increment_columns: list[dict]   (column, mechanism, seed?, increment?)
#   change_capture: {enabled: bool, mechanism: str} | None
#   sensitivity_classifications: list[SensitivityEntry]
#   referenced_by: {procedures: [...], views: [...], functions: [...]}
#
# ProcCatalog / ViewCatalog / FunctionCatalog:
#   references: {tables: [...], views: [...], functions: [...], procedures: [...]}


# ── File naming ─────────────────────────────────────────────────────────────


def _catalog_dir(ddl_path: Path) -> Path:
    return ddl_path / "catalog"


def _object_path(ddl_path: Path, object_type: str, fqn: str) -> Path:
    """Return the catalog JSON path for a given object.

    *object_type* is one of ``tables``, ``procedures``, ``views``,
    ``functions``.  *fqn* is a normalised ``schema.name`` string.
    """
    return _catalog_dir(ddl_path) / object_type / f"{fqn}.json"


def has_catalog(ddl_path: Path) -> bool:
    """Return True if a catalog directory exists with at least one file."""
    d = _catalog_dir(ddl_path)
    if not d.is_dir():
        return False
    return any(d.rglob("*.json"))


# ── Loading ─────────────────────────────────────────────────────────────────


def load_table_catalog(ddl_path: Path, table_fqn: str) -> dict[str, Any] | None:
    """Load a single table catalog file, or ``None`` if absent."""
    p = _object_path(ddl_path, "tables", normalize(table_fqn))
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def load_proc_catalog(ddl_path: Path, proc_fqn: str) -> dict[str, Any] | None:
    """Load a single procedure catalog file, or ``None`` if absent."""
    p = _object_path(ddl_path, "procedures", normalize(proc_fqn))
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def load_view_catalog(ddl_path: Path, view_fqn: str) -> dict[str, Any] | None:
    """Load a single view catalog file, or ``None`` if absent."""
    p = _object_path(ddl_path, "views", normalize(view_fqn))
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def load_function_catalog(ddl_path: Path, func_fqn: str) -> dict[str, Any] | None:
    """Load a single function catalog file, or ``None`` if absent."""
    p = _object_path(ddl_path, "functions", normalize(func_fqn))
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


# ── Routing flag detection ──────────────────────────────────────────────────


def scan_routing_flags(definition: str) -> dict[str, bool]:
    """Scan a proc/view/function body and return routing flags.

    Returns ``{"needs_llm": bool, "needs_enrich": bool}``.

    ``needs_llm``: set when sqlglot cannot fully resolve the body — dynamic
    ``EXEC(@var)``, ``TRY/CATCH``, ``WHILE``, or ``IF/ELSE`` branching.

    ``needs_enrich``: set when DMF left gaps AST can fill — ``SELECT INTO``,
    ``TRUNCATE``, or static ``EXEC`` call chains.
    """
    return {
        "needs_llm": bool(_NEEDS_LLM_RE.search(definition)),
        "needs_enrich": bool(_NEEDS_ENRICH_RE.search(definition)),
    }


# ── Writing ─────────────────────────────────────────────────────────────────


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def write_table_catalog(
    ddl_path: Path,
    table_fqn: str,
    signals: dict[str, Any],
    referenced_by: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
) -> Path:
    """Write a table catalog file.  Returns the written path."""
    fqn = normalize(table_fqn)
    defaults: dict[str, Any] = {
        "columns": [],
        "primary_keys": [],
        "unique_indexes": [],
        "foreign_keys": [],
        "auto_increment_columns": [],
        "change_capture": None,
        "sensitivity_classifications": [],
    }
    data: dict[str, Any] = {**defaults, **signals}
    if referenced_by is not None:
        data["referenced_by"] = referenced_by
    else:
        data.setdefault("referenced_by", {
            "procedures": _empty_scoped(),
            "views": _empty_scoped(),
            "functions": _empty_scoped(),
        })
    p = _object_path(ddl_path, "tables", fqn)
    _write_json(p, data)
    return p


def write_object_catalog(
    ddl_path: Path,
    object_type: str,
    fqn: str,
    references: dict[str, list[dict[str, Any]]],
    *,
    needs_llm: bool = False,
    needs_enrich: bool = False,
    params: list[dict[str, Any]] | None = None,
) -> Path:
    """Write a proc/view/function catalog file.  Returns the written path."""
    norm = normalize(fqn)
    data: dict[str, Any] = {"references": references}
    if params is not None:
        data["params"] = params
    if needs_llm:
        data["needs_llm"] = True
    if needs_enrich:
        data["needs_enrich"] = True
    p = _object_path(ddl_path, object_type, norm)
    _write_json(p, data)
    return p


# ── DMF result processing ──────────────────────────────────────────────────


def _make_ref_entry(
    schema: str,
    name: str,
    *,
    is_selected: bool = False,
    is_updated: bool = False,
    is_insert_all: bool = False,
    is_caller_dependent: bool = False,
    is_ambiguous: bool = False,
    columns: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a single reference entry dict."""
    entry: dict[str, Any] = {
        "schema": schema,
        "name": name,
        "is_selected": is_selected,
        "is_updated": is_updated,
    }
    if is_insert_all:
        entry["is_insert_all"] = is_insert_all
    if is_caller_dependent:
        entry["is_caller_dependent"] = True
    if is_ambiguous:
        entry["is_ambiguous"] = True
    if columns:
        entry["columns"] = columns
    return entry


def _classify_referenced_type(class_desc: str) -> str | None:
    """Map ``referenced_class_desc`` from the DMF to a catalog bucket.

    Returns one of ``tables``, ``views``, ``functions``, ``procedures``
    or ``None`` for unsupported types (types, assemblies, etc.).
    """
    desc = class_desc.upper() if class_desc else ""
    if desc in ("OBJECT_OR_COLUMN",):
        # Ambiguous — caller must resolve from sys.objects or treat as table
        return None
    mapping = {
        "USER_TABLE": "tables",
        "VIEW": "views",
        "SQL_SCALAR_FUNCTION": "functions",
        "SQL_TABLE_VALUED_FUNCTION": "functions",
        "SQL_INLINE_TABLE_VALUED_FUNCTION": "functions",
        "SQL_STORED_PROCEDURE": "procedures",
        "AGGREGATE_FUNCTION": "functions",
    }
    return mapping.get(desc)


def _empty_scoped() -> dict[str, list[dict[str, Any]]]:
    """Return an empty scoped bucket: ``{"in_scope": [], "out_of_scope": []}``."""
    return {"in_scope": [], "out_of_scope": []}


def _group_dmf_rows(
    rows: list[dict[str, Any]],
    object_types: dict[str, str],
    database: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    """Group raw DMF rows by (referencing_fqn, entity_key).

    Each entity_key groups all rows for the same (referencing object,
    referenced entity, scope) combination.  Boolean flags are OR-accumulated
    across rows; column-level detail is merged.

    Returns ``{referencing_fqn: {entity_key: entry_data}}``.
    """
    db_lower = database.lower()
    grouped: dict[str, dict[str, dict[str, Any]]] = {}

    for row in rows:
        ref_schema = row.get("referencing_schema", "")
        ref_name = row.get("referencing_name", "")
        referencing_fqn = normalize(f"{ref_schema}.{ref_name}")

        tgt_schema = row.get("referenced_schema") or ""
        tgt_name = row.get("referenced_entity") or ""
        if not tgt_name:
            continue

        tgt_fqn = normalize(f"{tgt_schema}.{tgt_name}") if tgt_schema else normalize(tgt_name)
        minor_name = row.get("referenced_minor_name") or ""
        class_desc = row.get("referenced_class_desc") or ""

        # Scope classification
        ref_db = (row.get("referenced_database_name") or "").strip()
        ref_server = (row.get("referenced_server_name") or "").strip()
        is_out_of_scope = False
        out_reason: str | None = None
        if ref_server:
            is_out_of_scope = True
            out_reason = "cross_server_reference"
        elif ref_db and db_lower and ref_db.lower() != db_lower:
            is_out_of_scope = True
            out_reason = "cross_database_reference"

        # Bucket classification
        bucket = _classify_referenced_type(class_desc)
        if bucket is None:
            bucket = object_types.get(tgt_fqn)
        if bucket is None:
            bucket = "tables"

        if referencing_fqn not in grouped:
            grouped[referencing_fqn] = {}

        entity_key = f"{bucket}:{tgt_fqn}:{'out' if is_out_of_scope else 'in'}"
        if entity_key not in grouped[referencing_fqn]:
            entry_data: dict[str, Any] = {
                "bucket": bucket, "schema": tgt_schema, "name": tgt_name,
                "is_out_of_scope": is_out_of_scope,
            }
            if is_out_of_scope:
                entry_data.update(database=ref_db or database, server=ref_server or None, reason=out_reason)
            else:
                entry_data.update(is_selected=False, is_updated=False, is_insert_all=False,
                                  is_caller_dependent=False, is_ambiguous=False, columns={})
            grouped[referencing_fqn][entity_key] = entry_data

        entry = grouped[referencing_fqn][entity_key]

        # OR-accumulate flags and column detail for in-scope entries
        if not is_out_of_scope:
            entry["is_selected"] = entry["is_selected"] or bool(row.get("is_selected"))
            entry["is_updated"] = entry["is_updated"] or bool(row.get("is_updated"))
            entry["is_insert_all"] = entry["is_insert_all"] or bool(row.get("is_insert_all"))
            entry["is_caller_dependent"] = entry["is_caller_dependent"] or bool(row.get("is_caller_dependent"))
            entry["is_ambiguous"] = entry["is_ambiguous"] or bool(row.get("is_ambiguous"))
            if minor_name:
                if minor_name not in entry["columns"]:
                    entry["columns"][minor_name] = {"name": minor_name, "is_selected": False, "is_updated": False}
                col = entry["columns"][minor_name]
                col["is_selected"] = col["is_selected"] or bool(row.get("is_selected"))
                col["is_updated"] = col["is_updated"] or bool(row.get("is_updated"))

    return grouped


def _reshape_to_scoped_refs(
    grouped: dict[str, dict[str, dict[str, Any]]],
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Convert grouped intermediate structure to per-object scoped ref dicts.

    Returns ``{referencing_fqn: {tables: {in_scope: [...], out_of_scope: [...]}, ...}}``.
    """
    result: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for referencing_fqn, entities in grouped.items():
        refs: dict[str, dict[str, list[dict[str, Any]]]] = {
            "tables": _empty_scoped(), "views": _empty_scoped(),
            "functions": _empty_scoped(), "procedures": _empty_scoped(),
        }
        for entity_data in entities.values():
            b = entity_data["bucket"]
            if entity_data["is_out_of_scope"]:
                refs[b]["out_of_scope"].append({
                    "schema": entity_data["schema"], "name": entity_data["name"],
                    "database": entity_data.get("database", ""),
                    "server": entity_data.get("server"), "reason": entity_data["reason"],
                })
            else:
                columns = sorted(entity_data["columns"].values(), key=lambda c: c["name"])
                refs[b]["in_scope"].append(_make_ref_entry(
                    schema=entity_data["schema"], name=entity_data["name"],
                    is_selected=entity_data["is_selected"], is_updated=entity_data["is_updated"],
                    is_insert_all=entity_data["is_insert_all"],
                    is_caller_dependent=entity_data["is_caller_dependent"],
                    is_ambiguous=entity_data["is_ambiguous"],
                    columns=columns if columns else None,
                ))
        for scoped in refs.values():
            scoped["in_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
            scoped["out_of_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
        result[referencing_fqn] = refs
    return result


def process_dmf_results(
    rows: list[dict[str, Any]],
    object_types: dict[str, str] | None = None,
    database: str = "",
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Group raw DMF result rows into per-referencing-object reference dicts.

    Returns ``{referencing_fqn: {tables: {in_scope: [...], out_of_scope: [...]}, ...}}``.
    """
    grouped = _group_dmf_rows(rows, object_types or {}, database)
    return _reshape_to_scoped_refs(grouped)


def flip_references(
    proc_refs: dict[str, dict[str, dict[str, list[dict[str, Any]]]]],
    referencing_type: str,
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Build ``referenced_by`` dicts for tables by flipping outbound proc/view/function refs.

    *proc_refs* is the output of ``process_dmf_results()``:
    ``{referencing_fqn: {tables: {in_scope: [...], out_of_scope: [...]}, ...}}``.

    *referencing_type* is one of ``procedures``, ``views``, ``functions`` — the
    bucket name under which the referencing objects should appear in the
    ``referenced_by`` section of table files.

    Only ``in_scope`` entries are flipped (no table catalog file exists for
    out-of-scope references).

    Returns ``{table_fqn: {procedures: {in_scope: [...], out_of_scope: []}, ...}}``.
    """
    result: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}

    for referencing_fqn, refs in proc_refs.items():
        # Parse referencing_fqn back to schema.name (use last two parts)
        parts = referencing_fqn.split(".")
        ref_schema = parts[-2] if len(parts) >= 2 else "dbo"
        ref_name = parts[-1]

        for table_entry in refs.get("tables", {}).get("in_scope", []):
            table_fqn = normalize(f"{table_entry['schema']}.{table_entry['name']}")

            if table_fqn not in result:
                result[table_fqn] = {
                    "procedures": _empty_scoped(),
                    "views": _empty_scoped(),
                    "functions": _empty_scoped(),
                }

            flipped_entry = _make_ref_entry(
                schema=ref_schema,
                name=ref_name,
                is_selected=table_entry.get("is_selected", False),
                is_updated=table_entry.get("is_updated", False),
                is_insert_all=table_entry.get("is_insert_all", False),
                is_caller_dependent=table_entry.get("is_caller_dependent", False),
                is_ambiguous=table_entry.get("is_ambiguous", False),
                columns=table_entry.get("columns"),
            )
            result[table_fqn][referencing_type]["in_scope"].append(flipped_entry)

    # Sort each bucket for deterministic output
    for table_refs in result.values():
        for scoped in table_refs.values():
            scoped["in_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())

    return result


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
