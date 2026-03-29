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


def process_dmf_results(
    rows: list[dict[str, Any]],
    object_types: dict[str, str] | None = None,
    database: str = "",
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Group raw DMF result rows into per-referencing-object reference dicts.

    *rows* is a list of dicts with keys matching the DMF cursor output:
    ``referencing_schema``, ``referencing_name``, ``referenced_schema``,
    ``referenced_entity``, ``referenced_minor_name``, ``referenced_class_desc``,
    ``is_selected``, ``is_updated``, ``is_select_all``, ``is_insert_all``,
    ``is_all_columns_found``, ``is_caller_dependent``, ``is_ambiguous``,
    ``referenced_database_name``, ``referenced_server_name``.

    *object_types* is an optional ``{schema.name: type}`` mapping where type
    is one of ``tables``, ``views``, ``functions``, ``procedures``.  Used to
    resolve ``OBJECT_OR_COLUMN`` class descriptions.

    *database* is the current database name.  References whose
    ``referenced_database_name`` differs (or whose ``referenced_server_name``
    is non-empty) are classified as ``out_of_scope``.

    Returns ``{referencing_fqn: {tables: {in_scope: [...], out_of_scope: [...]}, ...}}``.
    """
    object_types = object_types or {}
    db_lower = database.lower()

    # Group rows by (referencing object, referenced entity)
    grouped: dict[str, dict[str, dict[str, Any]]] = {}

    for row in rows:
        ref_schema = row.get("referencing_schema", "")
        ref_name = row.get("referencing_name", "")
        referencing_fqn = normalize(f"{ref_schema}.{ref_name}")

        tgt_schema = row.get("referenced_schema") or ""
        tgt_name = row.get("referenced_entity") or ""
        if not tgt_name:
            continue  # skip column-only rows without entity

        tgt_fqn = normalize(f"{tgt_schema}.{tgt_name}") if tgt_schema else normalize(tgt_name)
        minor_name = row.get("referenced_minor_name") or ""
        class_desc = row.get("referenced_class_desc") or ""

        # Determine cross-database / cross-server scope
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

        bucket = _classify_referenced_type(class_desc)
        if bucket is None:
            # Try object_types lookup for OBJECT_OR_COLUMN
            bucket = object_types.get(tgt_fqn)
        if bucket is None:
            bucket = "tables"  # default assumption for OBJECT_OR_COLUMN

        if referencing_fqn not in grouped:
            grouped[referencing_fqn] = {}

        entity_key = f"{bucket}:{tgt_fqn}:{'out' if is_out_of_scope else 'in'}"
        if entity_key not in grouped[referencing_fqn]:
            entry_data: dict[str, Any] = {
                "bucket": bucket,
                "schema": tgt_schema,
                "name": tgt_name,
                "is_out_of_scope": is_out_of_scope,
            }
            if is_out_of_scope:
                entry_data["database"] = ref_db or database
                entry_data["server"] = ref_server or None
                entry_data["reason"] = out_reason
            else:
                entry_data.update({
                    "is_selected": False,
                    "is_updated": False,
                    "is_insert_all": False,
                    "is_caller_dependent": False,
                    "is_ambiguous": False,
                    "columns": {},
                })
            grouped[referencing_fqn][entity_key] = entry_data

        entry = grouped[referencing_fqn][entity_key]

        # Only accumulate detail for in-scope entries
        if not is_out_of_scope:
            entry["is_selected"] = entry["is_selected"] or bool(row.get("is_selected"))
            entry["is_updated"] = entry["is_updated"] or bool(row.get("is_updated"))
            entry["is_insert_all"] = entry["is_insert_all"] or bool(row.get("is_insert_all"))
            entry["is_caller_dependent"] = entry["is_caller_dependent"] or bool(row.get("is_caller_dependent"))
            entry["is_ambiguous"] = entry["is_ambiguous"] or bool(row.get("is_ambiguous"))

            # Column-level detail
            if minor_name:
                if minor_name not in entry["columns"]:
                    entry["columns"][minor_name] = {
                        "name": minor_name,
                        "is_selected": False,
                        "is_updated": False,
                    }
                col = entry["columns"][minor_name]
                col["is_selected"] = col["is_selected"] or bool(row.get("is_selected"))
                col["is_updated"] = col["is_updated"] or bool(row.get("is_updated"))

    # Reshape into per-object references dicts with scoped buckets
    result: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for referencing_fqn, entities in grouped.items():
        refs: dict[str, dict[str, list[dict[str, Any]]]] = {
            "tables": _empty_scoped(),
            "views": _empty_scoped(),
            "functions": _empty_scoped(),
            "procedures": _empty_scoped(),
        }
        for entity_data in entities.values():
            b = entity_data["bucket"]
            if entity_data["is_out_of_scope"]:
                out_entry: dict[str, Any] = {
                    "schema": entity_data["schema"],
                    "name": entity_data["name"],
                    "database": entity_data.get("database", ""),
                    "server": entity_data.get("server"),
                    "reason": entity_data["reason"],
                }
                refs[b]["out_of_scope"].append(out_entry)
            else:
                columns = sorted(entity_data["columns"].values(), key=lambda c: c["name"])
                ref_entry = _make_ref_entry(
                    schema=entity_data["schema"],
                    name=entity_data["name"],
                    is_selected=entity_data["is_selected"],
                    is_updated=entity_data["is_updated"],
                    is_insert_all=entity_data["is_insert_all"],
                    is_caller_dependent=entity_data["is_caller_dependent"],
                    is_ambiguous=entity_data["is_ambiguous"],
                    columns=columns if columns else None,
                )
                refs[b]["in_scope"].append(ref_entry)
        # Sort each bucket by schema.name for deterministic output
        for scoped in refs.values():
            scoped["in_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
            scoped["out_of_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())
        result[referencing_fqn] = refs
    return result


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

    *table_signals* maps ``table_fqn`` → catalog signal dict (PKs, FKs, etc.).
    Includes ``columns`` list when provided by export_ddl.
    *proc_dmf_rows*, *view_dmf_rows*, *func_dmf_rows* are raw DMF result rows.
    *object_types* resolves ambiguous OBJECT_OR_COLUMN references.
    *routing_flags* maps ``fqn`` → ``{"needs_llm": bool, "needs_enrich": bool}``
    from the body scan pass.
    *database* is the current database name, used to classify cross-database
    references as out-of-scope.
    *proc_params* maps ``fqn`` → list of parameter dicts from ``sys.parameters``.

    Returns counts: ``{tables: N, procedures: N, views: N, functions: N}``.
    """
    counts = {"tables": 0, "procedures": 0, "views": 0, "functions": 0}
    rflags = routing_flags or {}
    pparams = proc_params or {}

    # Process DMF results per object type
    proc_refs = process_dmf_results(proc_dmf_rows, object_types, database=database)
    view_refs = process_dmf_results(view_dmf_rows, object_types, database=database)
    func_refs = process_dmf_results(func_dmf_rows, object_types, database=database)

    def _empty_refs() -> dict[str, dict[str, list[dict[str, Any]]]]:
        return {"tables": _empty_scoped(), "views": _empty_scoped(), "functions": _empty_scoped(), "procedures": _empty_scoped()}

    def _flags(fqn: str) -> dict[str, bool]:
        return rflags.get(fqn, {})

    # Write proc/view/function catalog files
    for fqn, refs in proc_refs.items():
        write_object_catalog(ddl_path, "procedures", fqn, refs, **_flags(fqn), params=pparams.get(fqn))
        counts["procedures"] += 1

    for fqn, refs in view_refs.items():
        write_object_catalog(ddl_path, "views", fqn, refs, **_flags(fqn))
        counts["views"] += 1

    for fqn, refs in func_refs.items():
        write_object_catalog(ddl_path, "functions", fqn, refs, **_flags(fqn))
        counts["functions"] += 1

    # Write empty catalog files for objects that had no DMF refs
    for fqn, bucket in (object_types or {}).items():
        if bucket == "procedures" and fqn not in proc_refs:
            write_object_catalog(ddl_path, "procedures", fqn, _empty_refs(), **_flags(fqn), params=pparams.get(fqn))
            counts["procedures"] += 1
        elif bucket == "views" and fqn not in view_refs:
            write_object_catalog(ddl_path, "views", fqn, _empty_refs(), **_flags(fqn))
            counts["views"] += 1
        elif bucket == "functions" and fqn not in func_refs:
            write_object_catalog(ddl_path, "functions", fqn, _empty_refs(), **_flags(fqn))
            counts["functions"] += 1

    # Flip references to build referenced_by for tables
    table_referenced_by: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for referencing_type, refs_dict in [
        ("procedures", proc_refs),
        ("views", view_refs),
        ("functions", func_refs),
    ]:
        flipped = flip_references(refs_dict, referencing_type)
        for table_fqn, ref_by in flipped.items():
            if table_fqn not in table_referenced_by:
                table_referenced_by[table_fqn] = {
                    "procedures": _empty_scoped(),
                    "views": _empty_scoped(),
                    "functions": _empty_scoped(),
                }
            for bucket_name in ("procedures", "views", "functions"):
                table_referenced_by[table_fqn][bucket_name]["in_scope"].extend(
                    ref_by.get(bucket_name, {}).get("in_scope", [])
                )

    # Sort merged referenced_by
    for ref_by in table_referenced_by.values():
        for scoped in ref_by.values():
            scoped["in_scope"].sort(key=lambda e: f"{e['schema']}.{e['name']}".lower())

    # Write table catalog files
    known_tables = {fqn for fqn, bucket in (object_types or {}).items() if bucket == "tables"}
    all_table_fqns = set(table_signals.keys()) | set(table_referenced_by.keys()) | known_tables
    for table_fqn in sorted(all_table_fqns):
        signals = table_signals.get(table_fqn, {})
        ref_by = table_referenced_by.get(table_fqn)
        write_table_catalog(ddl_path, table_fqn, signals, ref_by)
        counts["tables"] += 1

    return counts
