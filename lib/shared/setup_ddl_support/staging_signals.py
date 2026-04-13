"""Catalog-signal normalization helpers for setup-ddl."""

from __future__ import annotations

from typing import Any

from shared.name_resolver import normalize
from shared.sql_types import format_sql_type

TYPE_MAPPING = {
    "U": "tables",
    "V": "views",
    "P": "procedures",
    "FN": "functions",
    "IF": "functions",
    "TF": "functions",
}


def ensure_table_skeleton(signals: dict[str, dict[str, Any]], fqn: str) -> dict[str, Any]:
    if fqn not in signals:
        signals[fqn] = {
            "columns": [],
            "primary_keys": [],
            "unique_indexes": [],
            "foreign_keys": [],
            "auto_increment_columns": [],
            "change_capture": None,
            "sensitivity_classifications": [],
        }
    return signals[fqn]


def build_object_types_map(object_types_raw: list | dict) -> dict[str, str]:
    if isinstance(object_types_raw, dict):
        return object_types_raw
    result: dict[str, str] = {}
    if isinstance(object_types_raw, list):
        for row in object_types_raw:
            fqn = normalize(f"{row['schema_name']}.{row['name']}")
            bucket = TYPE_MAPPING.get(row.get("type", "").strip())
            if bucket:
                result[fqn] = bucket
    return result


def build_function_subtypes(object_types_raw: list | dict) -> dict[str, str]:
    func_types = {"FN", "IF", "TF"}
    result: dict[str, str] = {}
    if isinstance(object_types_raw, list):
        for row in object_types_raw:
            type_code = row.get("type", "").strip()
            if type_code in func_types:
                fqn = normalize(f"{row['schema_name']}.{row['name']}")
                result[fqn] = type_code
    return result


def apply_column_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        sig["columns"].append(
            {
                "name": row["column_name"],
                "sql_type": format_sql_type(
                    row["type_name"], row["max_length"], row["precision"], row["scale"]
                ),
                "is_nullable": bool(row.get("is_nullable")),
                "is_identity": bool(row.get("is_identity")),
            }
        )


def apply_pk_unique_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        if row.get("is_primary_key"):
            existing = next(
                (pk for pk in sig["primary_keys"] if pk["constraint_name"] == row["index_name"]),
                None,
            )
            if existing is None:
                sig["primary_keys"].append(
                    {"constraint_name": row["index_name"], "columns": [row["column_name"]]}
                )
            else:
                existing["columns"].append(row["column_name"])
        else:
            existing = next(
                (ui for ui in sig["unique_indexes"] if ui["index_name"] == row["index_name"]),
                None,
            )
            if existing is None:
                sig["unique_indexes"].append(
                    {"index_name": row["index_name"], "columns": [row["column_name"]]}
                )
            else:
                existing["columns"].append(row["column_name"])


def apply_fk_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        existing = next(
            (f for f in sig["foreign_keys"] if f["constraint_name"] == row["constraint_name"]),
            None,
        )
        if existing is None:
            sig["foreign_keys"].append(
                {
                    "constraint_name": row["constraint_name"],
                    "columns": [row["column_name"]],
                    "referenced_schema": row["ref_schema"],
                    "referenced_table": row["ref_table"],
                    "referenced_columns": [row["ref_column"]],
                }
            )
        else:
            existing["columns"].append(row["column_name"])
            existing["referenced_columns"].append(row["ref_column"])


def apply_identity_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        entry: dict[str, Any] = {"column": row["column_name"], "mechanism": "identity"}
        if "seed_value" in row:
            entry["seed"] = row["seed_value"]
        if "increment_value" in row:
            entry["increment"] = row["increment_value"]
        sig["auto_increment_columns"].append(entry)


def apply_change_capture_rows(
    signals: dict[str, dict[str, Any]], cdc_rows: list, ct_rows: list,
) -> None:
    for row in cdc_rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        sig["change_capture"] = {"enabled": True, "mechanism": "cdc"}
    for row in ct_rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        sig["change_capture"] = {"enabled": True, "mechanism": "change_tracking"}


def apply_sensitivity_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = ensure_table_skeleton(signals, fqn)
        sig["sensitivity_classifications"].append(
            {
                "column": row["column_name"],
                "label": row.get("label", ""),
                "information_type": row.get("information_type", ""),
            }
        )


def build_routing_flags(
    definitions_rows: list, scan_routing_flags: Any,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in definitions_rows:
        definition = row.get("definition")
        if definition:
            fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
            result[fqn] = scan_routing_flags(definition)
    return result


def build_proc_params(proc_params_rows: list) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in proc_params_rows:
        fqn = normalize(f"{row['schema_name']}.{row['proc_name']}")
        result.setdefault(fqn, []).append(
            {
                "name": row["param_name"],
                "sql_type": format_sql_type(
                    row["type_name"], row.get("max_length", 0), row.get("precision", 0), row.get("scale", 0)
                ),
                "is_output": bool(row.get("is_output")),
                "has_default": bool(row.get("has_default_value")),
            }
        )
    return result


def build_view_definitions_map(
    definitions_rows: list, object_types: dict[str, str],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in definitions_rows:
        definition = row.get("definition")
        if not definition:
            continue
        fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
        if object_types.get(fqn) == "views":
            result[fqn] = definition
    return result


def build_long_truncation_map(
    definitions_rows: list, object_types: dict[str, str],
) -> set[str]:
    result: set[str] = set()
    for row in definitions_rows:
        if not row.get("long_truncation"):
            continue
        fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
        if object_types.get(fqn) == "views":
            result.add(fqn)
    return result


def build_view_columns_map(view_columns_rows: list) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in view_columns_rows:
        fqn = normalize(f"{row['schema_name']}.{row['view_name']}")
        grouped.setdefault(fqn, []).append(
            {
                "_column_id": row.get("column_id", 0),
                "name": row["column_name"],
                "sql_type": format_sql_type(
                    row["type_name"], row["max_length"], row["precision"], row["scale"]
                ),
                "is_nullable": bool(row.get("is_nullable")),
            }
        )
    result: dict[str, list[dict[str, Any]]] = {}
    for fqn, cols in grouped.items():
        cols.sort(key=lambda c: c["_column_id"])
        result[fqn] = [{k: v for k, v in c.items() if k != "_column_id"} for c in cols]
    return result


def build_catalog_write_inputs(staging_inputs: dict[str, Any]) -> dict[str, Any]:
    from shared.routing import scan_routing_flags

    object_types = build_object_types_map(staging_inputs["object_types_raw"])
    table_signals: dict[str, dict[str, Any]] = {}
    apply_column_rows(table_signals, staging_inputs["table_columns_rows"])
    apply_pk_unique_rows(table_signals, staging_inputs["pk_unique_rows"])
    apply_fk_rows(table_signals, staging_inputs["fk_rows"])
    apply_identity_rows(table_signals, staging_inputs["identity_rows"])
    apply_change_capture_rows(table_signals, staging_inputs["cdc_rows"], staging_inputs["ct_rows"])
    apply_sensitivity_rows(table_signals, staging_inputs["sensitivity_rows"])

    return {
        "object_types": object_types,
        "table_signals": table_signals,
        "routing_flags": build_routing_flags(staging_inputs["definitions_rows"], scan_routing_flags),
        "proc_params": build_proc_params(staging_inputs["proc_params_rows"]),
        "view_definitions": build_view_definitions_map(staging_inputs["definitions_rows"], object_types),
        "view_columns": build_view_columns_map(staging_inputs["view_columns_rows"]),
        "function_subtypes": build_function_subtypes(staging_inputs["object_types_raw"]),
        "long_truncation_fqns": build_long_truncation_map(staging_inputs["definitions_rows"], object_types),
        "mv_fqns": {
            normalize(fqn) if "." in fqn else fqn for fqn in staging_inputs["mv_fqns_list"]
        },
    }
