"""CLI helpers for the setup-ddl skill.

Each subcommand accepts raw JSON (MCP query results saved by the agent) and
performs deterministic processing — assembling DDL files, writing catalog JSON,
or producing the extraction manifest.

Usage (via uv):
    uv run --project <shared> setup-ddl assemble-modules --input <json> --output-folder <dir> --type procedures
    uv run --project <shared> setup-ddl assemble-tables --input <json> --output-folder <dir>
    uv run --project <shared> setup-ddl write-catalog --staging-dir <dir> --output-folder <dir> --database <name>
    uv run --project <shared> setup-ddl write-manifest --output-folder <dir> --technology sql_server --database <name> --schemas bronze,silver
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import typer

from shared.name_resolver import normalize
from shared.sql_types import format_sql_type

app = typer.Typer(add_completion=False)

# ── Technology → dialect mapping ──────────────────────────────────────────────

_TECH_DIALECT = {
    "sql_server": "tsql",
    "fabric_warehouse": "tsql",
    "fabric_lakehouse": "spark",
    "snowflake": "snowflake",
}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _read_json(path: Path) -> Any:
    """Read and parse a JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def _read_json_optional(path: Path) -> Any:
    """Read a JSON file if it exists, else return empty list/dict."""
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


# ── assemble-modules ──────────────────────────────────────────────────────────


@app.command("assemble-modules")
def assemble_modules(
    input: Path = typer.Option(..., help="JSON file with [{schema_name, object_name, definition}]"),
    output_folder: Path = typer.Option(..., help="Output directory"),
    type: str = typer.Option(..., help="Object type: procedures, views, or functions"),
) -> None:
    """Assemble a GO-delimited .sql file from OBJECT_DEFINITION results."""
    if type not in ("procedures", "views", "functions"):
        typer.echo(f"Invalid type: {type}. Must be procedures, views, or functions.", err=True)
        raise typer.Exit(1)

    rows = _read_json(input)
    blocks: list[str] = []
    for row in rows:
        definition = row.get("definition")
        if definition:
            blocks.append(definition.strip())

    ddl_dir = output_folder / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / f"{type}.sql"
    out_path.write_text(
        "\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""),
        encoding="utf-8",
    )
    result = {"file": str(out_path), "count": len(blocks)}
    typer.echo(json.dumps(result))


# ── assemble-tables ───────────────────────────────────────────────────────────


@app.command("assemble-tables")
def assemble_tables(
    input: Path = typer.Option(..., help="JSON file with column metadata rows"),
    output_folder: Path = typer.Option(..., help="Output directory"),
) -> None:
    """Build CREATE TABLE statements from sys.columns metadata and write tables.sql."""
    rows = _read_json(input)

    # Group by (schema_name, table_name)
    tables: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row["schema_name"], row["table_name"])
        tables.setdefault(key, []).append(row)

    # Sort columns by column_id within each table
    for cols in tables.values():
        cols.sort(key=lambda r: r.get("column_id", 0))

    blocks: list[str] = []
    for (schema_name, table_name), cols in tables.items():
        col_defs: list[str] = []
        for c in cols:
            type_str = format_sql_type(
                c["type_name"], c["max_length"], c["precision"], c["scale"],
            )
            identity = ""
            if c.get("is_identity"):
                seed = c.get("seed_value", 1)
                inc = c.get("increment_value", 1)
                identity = f" IDENTITY({seed},{inc})"
            nullable = " NOT NULL" if not c.get("is_nullable") else " NULL"
            col_defs.append(f"    [{c['column_name']}] {type_str}{identity}{nullable}")

        ddl = f"CREATE TABLE [{schema_name}].[{table_name}] (\n" + ",\n".join(col_defs) + "\n)"
        blocks.append(ddl)

    ddl_dir = output_folder / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / "tables.sql"
    out_path.write_text(
        "\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""),
        encoding="utf-8",
    )
    result = {"file": str(out_path), "count": len(blocks)}
    typer.echo(json.dumps(result))


# ── write-catalog helpers ────────────────────────────────────────────────────

_TYPE_MAPPING = {"U": "tables", "V": "views", "P": "procedures",
                 "FN": "functions", "IF": "functions", "TF": "functions"}


def _ensure_table_skeleton(signals: dict[str, dict[str, Any]], fqn: str) -> dict[str, Any]:
    """Ensure signals[fqn] has all default keys. Returns the entry."""
    if fqn not in signals:
        signals[fqn] = {
            "columns": [], "primary_keys": [], "unique_indexes": [],
            "foreign_keys": [], "auto_increment_columns": [],
            "change_capture": None, "sensitivity_classifications": [],
        }
    return signals[fqn]


def _build_object_types_map(object_types_raw: list | dict) -> dict[str, str]:
    """Build a normalized FQN → bucket name mapping from raw object_types rows."""
    if isinstance(object_types_raw, dict):
        return object_types_raw
    result: dict[str, str] = {}
    if isinstance(object_types_raw, list):
        for row in object_types_raw:
            fqn = normalize(f"{row['schema_name']}.{row['name']}")
            bucket = _TYPE_MAPPING.get(row.get("type", "").strip())
            if bucket:
                result[fqn] = bucket
    return result


def _apply_column_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        sig["columns"].append({
            "name": row["column_name"],
            "sql_type": format_sql_type(
                row["type_name"], row["max_length"], row["precision"], row["scale"],
            ),
            "is_nullable": bool(row.get("is_nullable")),
            "is_identity": bool(row.get("is_identity")),
        })


def _apply_pk_unique_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        if row.get("is_primary_key"):
            existing = next((pk for pk in sig["primary_keys"] if pk["constraint_name"] == row["index_name"]), None)
            if existing is None:
                sig["primary_keys"].append({"constraint_name": row["index_name"], "columns": [row["column_name"]]})
            else:
                existing["columns"].append(row["column_name"])
        else:
            existing = next((ui for ui in sig["unique_indexes"] if ui["index_name"] == row["index_name"]), None)
            if existing is None:
                sig["unique_indexes"].append({"index_name": row["index_name"], "columns": [row["column_name"]]})
            else:
                existing["columns"].append(row["column_name"])


def _apply_fk_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        existing = next((f for f in sig["foreign_keys"] if f["constraint_name"] == row["constraint_name"]), None)
        if existing is None:
            sig["foreign_keys"].append({
                "constraint_name": row["constraint_name"],
                "columns": [row["column_name"]],
                "referenced_schema": row["ref_schema"],
                "referenced_table": row["ref_table"],
                "referenced_columns": [row["ref_column"]],
            })
        else:
            existing["columns"].append(row["column_name"])
            existing["referenced_columns"].append(row["ref_column"])


def _apply_identity_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        entry: dict[str, Any] = {"column": row["column_name"], "mechanism": "identity"}
        if "seed_value" in row:
            entry["seed"] = row["seed_value"]
        if "increment_value" in row:
            entry["increment"] = row["increment_value"]
        sig["auto_increment_columns"].append(entry)


def _apply_change_capture_rows(
    signals: dict[str, dict[str, Any]], cdc_rows: list, ct_rows: list,
) -> None:
    for row in cdc_rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        sig["change_capture"] = {"enabled": True, "mechanism": "cdc"}
    for row in ct_rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        sig["change_capture"] = {"enabled": True, "mechanism": "change_tracking"}


def _apply_sensitivity_rows(signals: dict[str, dict[str, Any]], rows: list) -> None:
    for row in rows:
        fqn = normalize(f"{row['schema_name']}.{row['table_name']}")
        sig = _ensure_table_skeleton(signals, fqn)
        sig["sensitivity_classifications"].append({
            "column": row["column_name"],
            "label": row.get("label", ""),
            "information_type": row.get("information_type", ""),
        })


def _build_routing_flags(
    definitions_rows: list, scan_routing_flags: Any,
) -> dict[str, dict[str, bool]]:
    result: dict[str, dict[str, bool]] = {}
    for row in definitions_rows:
        definition = row.get("definition")
        if definition:
            fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
            flags = scan_routing_flags(definition)
            if flags["needs_llm"] or flags["needs_enrich"]:
                result[fqn] = flags
    return result


def _build_proc_params(proc_params_rows: list) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in proc_params_rows:
        fqn = normalize(f"{row['schema_name']}.{row['proc_name']}")
        if fqn not in result:
            result[fqn] = []
        result[fqn].append({
            "name": row["param_name"],
            "sql_type": format_sql_type(
                row["type_name"], row.get("max_length", 0),
                row.get("precision", 0), row.get("scale", 0),
            ),
            "is_output": bool(row.get("is_output")),
            "has_default": bool(row.get("has_default_value")),
        })
    return result


# ── write-catalog ─────────────────────────────────────────────────────────────


@app.command("write-catalog")
def write_catalog(
    staging_dir: Path = typer.Option(..., help="Directory with staging JSON files from MCP queries"),
    output_folder: Path = typer.Option(..., help="DDL output directory (catalog/ written here)"),
    database: str = typer.Option(..., help="Source database name"),
) -> None:
    """Process staging JSON files and write all catalog JSON files.

    Expected staging files (saved by the agent from MCP query results):
      table_columns.json, pk_unique.json, foreign_keys.json,
      identity_columns.json, cdc.json, change_tracking.json (optional),
      sensitivity.json (optional), object_types.json, routing_flags.json (optional),
      proc_params.json (optional),
      proc_dmf.json, view_dmf.json, func_dmf.json
    """
    from shared.catalog import scan_routing_flags
    from shared.catalog_dmf import write_catalog_files

    # ── Load staging files ────────────────────────────────────────────────
    table_columns_rows = _read_json_optional(staging_dir / "table_columns.json")
    pk_unique_rows = _read_json_optional(staging_dir / "pk_unique.json")
    fk_rows = _read_json_optional(staging_dir / "foreign_keys.json")
    identity_rows = _read_json_optional(staging_dir / "identity_columns.json")
    cdc_rows = _read_json_optional(staging_dir / "cdc.json")
    ct_rows = _read_json_optional(staging_dir / "change_tracking.json")
    sensitivity_rows = _read_json_optional(staging_dir / "sensitivity.json")
    object_types_raw = _read_json_optional(staging_dir / "object_types.json")
    proc_dmf_rows = _read_json_optional(staging_dir / "proc_dmf.json")
    view_dmf_rows = _read_json_optional(staging_dir / "view_dmf.json")
    func_dmf_rows = _read_json_optional(staging_dir / "func_dmf.json")
    proc_params_rows = _read_json_optional(staging_dir / "proc_params.json")
    definitions_rows = _read_json_optional(staging_dir / "definitions.json")

    # ── Build derived structures ─────────────────────────────────────────
    object_types = _build_object_types_map(object_types_raw)

    table_signals: dict[str, dict[str, Any]] = {}
    _apply_column_rows(table_signals, table_columns_rows)
    _apply_pk_unique_rows(table_signals, pk_unique_rows)
    _apply_fk_rows(table_signals, fk_rows)
    _apply_identity_rows(table_signals, identity_rows)
    _apply_change_capture_rows(table_signals, cdc_rows, ct_rows)
    _apply_sensitivity_rows(table_signals, sensitivity_rows)

    routing_flags = _build_routing_flags(definitions_rows, scan_routing_flags)
    proc_params = _build_proc_params(proc_params_rows)

    # ── Write catalog files ───────────────────────────────────────────────
    counts = write_catalog_files(
        output_folder,
        table_signals=table_signals,
        proc_dmf_rows=proc_dmf_rows,
        view_dmf_rows=view_dmf_rows,
        func_dmf_rows=func_dmf_rows,
        object_types=object_types,
        routing_flags=routing_flags,
        database=database,
        proc_params=proc_params,
    )

    typer.echo(json.dumps(counts))


# ── write-manifest ────────────────────────────────────────────────────────────


@app.command("write-manifest")
def write_manifest(
    output_folder: Path = typer.Option(..., help="DDL output directory"),
    technology: str = typer.Option(..., help="Source technology: sql_server, fabric_warehouse, fabric_lakehouse, snowflake"),
    database: str = typer.Option(..., help="Source database name"),
    schemas: str = typer.Option(..., help="Comma-separated list of extracted schemas"),
) -> None:
    """Write manifest.json to the output folder."""
    if technology not in _TECH_DIALECT:
        typer.echo(f"Unknown technology: {technology}. Must be one of {list(_TECH_DIALECT.keys())}.", err=True)
        raise typer.Exit(1)

    manifest = {
        "schema_version": "1.0",
        "technology": technology,
        "dialect": _TECH_DIALECT[technology],
        "source_database": database,
        "extracted_schemas": [s.strip() for s in schemas.split(",")],
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }

    output_folder.mkdir(parents=True, exist_ok=True)
    out_path = output_folder / "manifest.json"
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    typer.echo(json.dumps({"file": str(out_path)}))


if __name__ == "__main__":
    app()
