"""DMF result processing — grouping, scoping, and reference flipping.

Processes raw rows from ``sys.dm_sql_referenced_entities``, groups them into
per-object reference dicts with ``in_scope``/``out_of_scope`` scoping, and
flips outbound references to build ``referenced_by`` for tables.
"""

from __future__ import annotations

from typing import Any

from shared.name_resolver import fqn_parts, normalize


# ── Shared helpers ────────────────────────────────────────────────────────────


def empty_scoped() -> dict[str, list[dict[str, Any]]]:
    """Return an empty scoped bucket: ``{"in_scope": [], "out_of_scope": []}``."""
    return {"in_scope": [], "out_of_scope": []}


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


# ── Grouping ──────────────────────────────────────────────────────────────────


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
            "tables": empty_scoped(), "views": empty_scoped(),
            "functions": empty_scoped(), "procedures": empty_scoped(),
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


# ── Public API ────────────────────────────────────────────────────────────────


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
        ref_schema, ref_name = fqn_parts(referencing_fqn)

        for table_entry in refs.get("tables", {}).get("in_scope", []):
            table_fqn = normalize(f"{table_entry['schema']}.{table_entry['name']}")

            if table_fqn not in result:
                result[table_fqn] = {
                    "procedures": empty_scoped(),
                    "views": empty_scoped(),
                    "functions": empty_scoped(),
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
