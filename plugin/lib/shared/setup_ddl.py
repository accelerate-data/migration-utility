"""CLI helpers for the setup-ddl skill.

Each subcommand either accepts raw JSON (MCP query results saved by the agent)
and performs deterministic processing, or connects directly to the source DB
for discovery.

Usage (via uv):
    uv run --project <shared> setup-ddl extract --database <db> --schemas silver,bronze [--project-root <dir>]
    uv run --project <shared> setup-ddl assemble-modules --input <json> --project-root <dir> --type procedures
    uv run --project <shared> setup-ddl assemble-tables --input <json> --project-root <dir>
    uv run --project <shared> setup-ddl write-catalog --staging-dir <dir> --project-root <dir> --database <name>
    uv run --project <shared> setup-ddl write-manifest --project-root <dir> --technology sql_server --database <name> --schemas bronze,silver
    uv run --project <shared> setup-ddl list-databases --project-root <dir>
    uv run --project <shared> setup-ddl list-schemas --project-root <dir> [--database <name>]

All JSON output goes to stdout; warnings/progress go to stderr.

Exit codes:
    0  success
    1  domain failure (invalid type, unknown technology, unsupported operation)
    2  IO, parse, or connection error
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import typer

from shared.catalog import write_json as _write_catalog_json
from shared.db_connect import cursor_to_dicts as _cursor_to_dicts
from shared.db_connect import oracle_connect as _oracle_connect
from shared.db_connect import sql_server_connect as _sql_server_connect
from shared.name_resolver import normalize
from shared.sql_types import format_sql_type

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, pretty_exceptions_enable=False)


# ── Exceptions ────────────────────────────────────────────────────────────────


class UnsupportedOperationError(Exception):
    """Raised when an operation is not supported for the configured technology."""


# ── Technology → dialect mapping ──────────────────────────────────────────────

_TECH_DIALECT = {
    "sql_server": "tsql",
    "fabric_warehouse": "tsql",
    "fabric_lakehouse": "spark",
    "snowflake": "snowflake",
    "oracle": "oracle",
}


# ── Guard helper ─────────────────────────────────────────────────────────────


def _require_technology(project_root: Path) -> str:
    """Read manifest.json and return the technology field.

    Raises ValueError with the guard message if manifest.json is missing
    or has no technology.
    """
    from shared.guards import check_technology
    result = check_technology(project_root)
    if not result["passed"]:
        raise ValueError(result["message"])
    manifest = json.loads((project_root / "manifest.json").read_text(encoding="utf-8"))
    return manifest["technology"]


# ── Oracle processing helpers ─────────────────────────────────────────────────


def _build_oracle_schema_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group ALL_OBJECTS rows by OWNER → sorted list of {owner, tables, procedures, views, functions}.

    Handles both uppercase (Oracle native) and lowercase key names.
    Counts TABLE, PROCEDURE, VIEW/MATERIALIZED VIEW, and FUNCTION. Other object types are ignored.
    """
    Entry = dict[str, Any]
    buckets: dict[str, Entry] = {}
    for row in rows:
        owner = row.get("OWNER") or row.get("owner") or ""
        obj_type = (row.get("OBJECT_TYPE") or row.get("object_type") or "").upper()
        if not owner:
            continue
        if owner not in buckets:
            buckets[owner] = {"owner": owner, "tables": 0, "procedures": 0, "views": 0, "functions": 0}
        if obj_type == "TABLE":
            buckets[owner]["tables"] += 1
        elif obj_type == "PROCEDURE":
            buckets[owner]["procedures"] += 1
        elif obj_type in ("VIEW", "MATERIALIZED VIEW"):
            buckets[owner]["views"] += 1
        elif obj_type == "FUNCTION":
            buckets[owner]["functions"] += 1
    return sorted(buckets.values(), key=lambda x: x["owner"])


# ── Helpers ───────────────────────────────────────────────────────────────────


def _read_json(path: Path) -> Any:
    """Read and parse a JSON file."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Corrupt JSON in {path}: {exc}") from exc


def _read_json_optional(path: Path) -> Any:
    """Read a JSON file if it exists, else return empty list/dict."""
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Corrupt JSON in {path}: {exc}") from exc


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
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in definitions_rows:
        definition = row.get("definition")
        if definition:
            fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
            flags = scan_routing_flags(definition)
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


def _build_view_definitions_map(
    definitions_rows: list,
    object_types: dict[str, str],
) -> dict[str, str]:
    """Extract raw DDL strings for views from definitions_rows.

    Filters to objects whose FQN maps to the ``views`` bucket in *object_types*.
    Returns a normalized FQN → definition string mapping.
    """
    result: dict[str, str] = {}
    for row in definitions_rows:
        definition = row.get("definition")
        if not definition:
            continue
        fqn = normalize(f"{row['schema_name']}.{row['object_name']}")
        if object_types.get(fqn) == "views":
            result[fqn] = definition
    return result


def _build_view_columns_map(
    view_columns_rows: list,
) -> dict[str, list[dict[str, Any]]]:
    """Build a normalized view FQN → column list mapping from sys.columns rows.

    Each column entry has ``name``, ``sql_type``, and ``is_nullable`` fields.
    Columns are ordered by ``column_id`` within each view.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in view_columns_rows:
        fqn = normalize(f"{row['schema_name']}.{row['view_name']}")
        if fqn not in grouped:
            grouped[fqn] = []
        grouped[fqn].append({
            "_column_id": row.get("column_id", 0),
            "name": row["column_name"],
            "sql_type": format_sql_type(
                row["type_name"], row["max_length"], row["precision"], row["scale"],
            ),
            "is_nullable": bool(row.get("is_nullable")),
        })
    result: dict[str, list[dict[str, Any]]] = {}
    for fqn, cols in grouped.items():
        cols.sort(key=lambda c: c["_column_id"])
        result[fqn] = [{k: v for k, v in c.items() if k != "_column_id"} for c in cols]
    return result


# ── Business logic (run_* functions) ─────────────────────────────────────────


def run_assemble_modules(input_path: Path, project_root: Path, object_type: str) -> dict[str, Any]:
    """Assemble a GO-delimited .sql file from OBJECT_DEFINITION results."""
    if object_type not in ("procedures", "views", "functions"):
        raise ValueError(f"Invalid type: {object_type}. Must be procedures, views, or functions.")

    rows = _read_json(input_path)
    blocks: list[str] = []
    for row in rows:
        definition = row.get("definition")
        if definition:
            blocks.append(definition.strip())

    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / f"{object_type}.sql"
    out_path.write_text(
        "\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""),
        encoding="utf-8",
    )
    return {"file": str(out_path), "count": len(blocks)}


def run_assemble_tables(input_path: Path, project_root: Path) -> dict[str, Any]:
    """Build CREATE TABLE statements from column metadata and write tables.sql."""
    rows = _read_json(input_path)

    # Determine quoting style: Oracle uses plain identifiers; T-SQL uses [brackets]
    technology = _require_technology(project_root)
    oracle_style = technology == "oracle"

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
            nullable = " NOT NULL" if not c.get("is_nullable") else " NULL"
            if oracle_style:
                col_defs.append(f"    {c['column_name']} {type_str}{nullable}")
            else:
                identity = ""
                if c.get("is_identity"):
                    seed = c.get("seed_value", 1)
                    inc = c.get("increment_value", 1)
                    identity = f" IDENTITY({seed},{inc})"
                col_defs.append(f"    [{c['column_name']}] {type_str}{identity}{nullable}")

        if oracle_style:
            ddl = f"CREATE TABLE {schema_name}.{table_name} (\n" + ",\n".join(col_defs) + "\n)"
        else:
            ddl = f"CREATE TABLE [{schema_name}].[{table_name}] (\n" + ",\n".join(col_defs) + "\n)"
        blocks.append(ddl)

    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / "tables.sql"
    out_path.write_text(
        "\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""),
        encoding="utf-8",
    )
    return {"file": str(out_path), "count": len(blocks)}


def _mark_stale(project_root: Path, removed_fqns: set[str]) -> None:
    """Set ``stale: true`` on catalog files for objects no longer in the source."""
    from shared.env_config import resolve_catalog_dir

    catalog_dir = resolve_catalog_dir(project_root)
    for fqn in sorted(removed_fqns):
        for bucket in ("tables", "procedures", "views", "functions"):
            p = catalog_dir / bucket / f"{fqn}.json"
            if p.exists():
                try:
                    data = json.loads(p.read_text(encoding="utf-8"))
                    data["stale"] = True
                    _write_catalog_json(p, data)
                except (json.JSONDecodeError, OSError) as exc:
                    logger.warning("event=mark_stale_error fqn=%s error=%s", fqn, exc)
                    continue
                logger.warning("event=catalog_stale_object fqn=%s bucket=%s", fqn, bucket)
                break


def run_write_catalog(staging_dir: Path, project_root: Path, database: str) -> dict[str, Any]:
    """Process staging JSON files and write all catalog JSON files.

    Uses diff-aware logic: computes DDL hashes from staging data, compares
    against hashes stored in existing catalog files, and only rewrites
    changed or new objects.  Removed objects are flagged as stale.
    """
    from shared.catalog import scan_routing_flags
    from shared.catalog_diff import classify_objects, compute_object_hashes, load_existing_hashes
    from shared.catalog_dmf import write_catalog_files
    from shared.env_config import resolve_catalog_dir

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
    view_columns_rows = _read_json_optional(staging_dir / "view_columns.json")

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
    view_definitions = _build_view_definitions_map(definitions_rows, object_types)
    view_columns = _build_view_columns_map(view_columns_rows)

    # ── Diff-aware classification ─────────────────────────────────────────
    fresh_hashes = compute_object_hashes(definitions_rows, table_signals, object_types)
    existing_hashes = load_existing_hashes(project_root)
    diff = classify_objects(fresh_hashes, existing_hashes)

    logger.info(
        "event=catalog_diff unchanged=%d changed=%d new=%d removed=%d",
        len(diff.unchanged), len(diff.changed), len(diff.new), len(diff.removed),
    )

    # ── Mark removed objects as stale ─────────────────────────────────────
    if diff.removed:
        _mark_stale(project_root, diff.removed)

    # ── Ensure catalog subdirectories exist (without wiping) ──────────────
    for subdir in ("tables", "procedures", "views", "functions"):
        (resolve_catalog_dir(project_root) / subdir).mkdir(parents=True, exist_ok=True)

    # ── Write only changed + new objects ──────────────────────────────────
    write_filter = diff.changed | diff.new

    counts = write_catalog_files(
        project_root,
        table_signals=table_signals,
        proc_dmf_rows=proc_dmf_rows,
        view_dmf_rows=view_dmf_rows,
        func_dmf_rows=func_dmf_rows,
        object_types=object_types,
        routing_flags=routing_flags,
        database=database,
        proc_params=proc_params,
        write_filter=write_filter,
        hashes=fresh_hashes,
        view_definitions=view_definitions,
        view_columns=view_columns,
    )

    counts["unchanged"] = len(diff.unchanged)
    counts["changed"] = len(diff.changed)
    counts["new"] = len(diff.new)
    counts["removed"] = len(diff.removed)

    return counts


def run_write_partial_manifest(
    project_root: Path, technology: str,
) -> dict[str, Any]:
    """Write a partial manifest.json with technology and dialect only.

    Called by init-ad-migration to record the chosen source technology.
    setup-ddl later enriches it with database and schema details.
    """
    if technology not in _TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {list(_TECH_DIALECT.keys())}."
        )

    manifest = {
        "schema_version": "1.0",
        "technology": technology,
        "dialect": _TECH_DIALECT[technology],
    }

    project_root.mkdir(parents=True, exist_ok=True)
    out_path = project_root / "manifest.json"
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {"file": str(out_path)}


def run_write_manifest(
    project_root: Path, technology: str, database: str, schemas: list[str],
) -> dict[str, Any]:
    """Write manifest.json to the project root.

    If a partial manifest already exists (from init-ad-migration), merges
    over it. Otherwise creates a fresh manifest.
    """
    if technology not in _TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {list(_TECH_DIALECT.keys())}."
        )

    out_path = project_root / "manifest.json"

    # Read existing partial manifest if present
    existing: dict[str, Any] = {}
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            existing = {}

    manifest = {
        **existing,
        "schema_version": "1.0",
        "technology": technology,
        "dialect": _TECH_DIALECT[technology],
        "source_database": database,
        "extracted_schemas": schemas,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }

    project_root.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return {"file": str(out_path)}


def run_list_databases(project_root: Path) -> dict[str, Any]:
    """List user databases on the source SQL Server, excluding system databases.

    Raises UnsupportedOperationError for Oracle.
    Raises ValueError if manifest.json is missing or has no technology.
    """
    technology = _require_technology(project_root)
    if technology == "oracle":
        raise UnsupportedOperationError("list-databases is not supported for Oracle.")
    conn = _sql_server_connect("master")
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sys.databases "
            "WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') "
            "  AND state_desc = 'ONLINE' "
            "ORDER BY name"
        )
        databases = [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()
    logger.info(
        "event=list_databases technology=%s count=%d", technology, len(databases),
    )
    return {"databases": databases}


def run_list_schemas(project_root: Path, database: Optional[str]) -> dict[str, Any]:
    """List schemas with per-type object counts on the source system.

    SQL Server / Fabric Warehouse: queries sys.schemas + sys.objects.
      --database is required. Returns {schema, tables, procedures, views, functions}.
    Oracle: queries ALL_OBJECTS grouped by owner.
      --database is ignored. Returns {owner, tables, procedures, views, functions}.
    """
    technology = _require_technology(project_root)

    if technology in ("sql_server", "fabric_warehouse"):
        if not database:
            raise ValueError("--database is required for SQL Server / Fabric Warehouse")
        conn = _sql_server_connect(database)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT s.name AS schema_name, "
                "    SUM(CASE WHEN o.type = 'U'               THEN 1 ELSE 0 END) AS tables, "
                "    SUM(CASE WHEN o.type = 'P'               THEN 1 ELSE 0 END) AS procedures, "
                "    SUM(CASE WHEN o.type = 'V'               THEN 1 ELSE 0 END) AS views, "
                "    SUM(CASE WHEN o.type IN ('FN','IF','TF') THEN 1 ELSE 0 END) AS functions "
                "FROM sys.schemas s "
                "LEFT JOIN sys.objects o "
                "    ON o.schema_id = s.schema_id AND o.is_ms_shipped = 0 "
                "GROUP BY s.name "
                "ORDER BY s.name"
            )
            schemas = [
                {"schema": row[0], "tables": row[1], "procedures": row[2], "views": row[3], "functions": row[4]}
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()
        logger.info(
            "event=list_schemas technology=%s database=%s count=%d",
            technology, database, len(schemas),
        )
        return {"schemas": schemas}

    if technology == "oracle":
        conn = _oracle_connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT OWNER, OBJECT_TYPE, OBJECT_NAME "
                "FROM ALL_OBJECTS "
                "ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME"
            )
            rows = _cursor_to_dicts(cursor)
        finally:
            conn.close()
        schemas = _build_oracle_schema_summary(rows)
        logger.info("event=list_schemas technology=oracle count=%d", len(schemas))
        return {"schemas": schemas}

    raise ValueError(f"list-schemas is not supported for technology '{technology}'")


def run_extract(
    project_root: Path,
    database: str | None,
    schemas: list[str],
) -> dict[str, Any]:
    """Connect to the source DB and extract DDL, catalog, and manifest in one pass.

    Reads technology from manifest.json (must already exist). Runs all extraction
    queries internally using a temp directory — no .staging/ is written to disk.
    After writing catalog files, restores any LLM-enriched fields (scoping,
    profile, refactor) that existed before extraction. Finishes with catalog-enrich.

    Raises ValueError if manifest.json is missing, technology is unrecognised,
    --database is missing for SQL Server / Fabric Warehouse, or --schemas is empty.
    Raises RuntimeError if the required DB driver is not installed.
    """
    from shared.catalog import snapshot_enriched_fields, restore_enriched_fields
    from shared.catalog_enrich import enrich_catalog
    from shared.diagnostics import run_diagnostics

    technology = _require_technology(project_root)

    if not schemas:
        raise ValueError("--schemas is required and must be non-empty")
    if technology in ("sql_server", "fabric_warehouse") and not database:
        raise ValueError(
            f"--database is required for technology '{technology}'"
        )

    dialect = _TECH_DIALECT.get(technology, "tsql")
    db_name = database or ""

    logger.info(
        "event=extract_start technology=%s database=%s schemas=%s",
        technology, db_name, schemas,
    )

    enriched_snapshot = snapshot_enriched_fields(project_root)

    with tempfile.TemporaryDirectory() as _tmp:
        staging_dir = Path(_tmp)

        if technology in ("sql_server", "fabric_warehouse"):
            from shared.sqlserver_extract import run_sqlserver_extraction
            run_sqlserver_extraction(staging_dir, db_name, schemas)
        elif technology == "oracle":
            from shared.oracle_extract import run_oracle_extraction
            run_oracle_extraction(staging_dir, schemas)
        else:
            raise ValueError(
                f"setup-ddl extract is not supported for technology '{technology}'"
            )

        # ── Assemble DDL files ────────────────────────────────────────────
        # Split definitions.json by object type and assemble per-type .sql files.
        obj_type_rows = _read_json_optional(staging_dir / "object_types.json")
        type_lookup = {
            normalize(f"{r['schema_name']}.{r['name']}"): r.get("type", "").strip()
            for r in obj_type_rows
        }
        definitions_rows_all = _read_json_optional(staging_dir / "definitions.json")

        for obj_label, type_codes in [
            ("procedures", {"P"}),
            ("views", {"V"}),
            ("functions", {"FN", "IF", "TF"}),
        ]:
            typed_defs = [
                r for r in definitions_rows_all
                if type_lookup.get(normalize(f"{r['schema_name']}.{r['object_name']}")) in type_codes
            ]
            if typed_defs:
                typed_path = staging_dir / f"{obj_label}.json"
                typed_path.write_text(
                    json.dumps(typed_defs, ensure_ascii=False), encoding="utf-8"
                )
                run_assemble_modules(typed_path, project_root, obj_label)

        table_cols_path = staging_dir / "table_columns.json"
        if table_cols_path.exists():
            run_assemble_tables(table_cols_path, project_root)

        run_write_manifest(project_root, technology, db_name, schemas)
        counts = run_write_catalog(staging_dir, project_root, db_name)

    restore_enriched_fields(project_root, enriched_snapshot)

    enrich_result = enrich_catalog(project_root, dialect=dialect)
    diag_result = run_diagnostics(project_root, dialect=dialect)
    logger.info(
        "event=extract_complete technology=%s tables=%s procedures=%s enrich=%s diagnostics=%s",
        technology, counts.get("tables"), counts.get("procedures"), enrich_result, diag_result,
    )

    return {**counts, "enrich": enrich_result, "diagnostics": diag_result}


# ── CLI wrappers ─────────────────────────────────────────────────────────────


@app.command("assemble-modules")
def assemble_modules(
    input: Path = typer.Option(..., help="JSON file with [{schema_name, object_name, definition}]"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
    type: str = typer.Option(..., help="Object type: procedures, views, or functions"),
) -> None:
    """Assemble a GO-delimited .sql file from OBJECT_DEFINITION results."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_assemble_modules(input, project_root, type)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2 if "Corrupt JSON" in str(exc) else 1) from exc
    typer.echo(json.dumps(result))


@app.command("assemble-tables")
def assemble_tables(
    input: Path = typer.Option(..., help="JSON file with column metadata rows"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
) -> None:
    """Build CREATE TABLE statements from sys.columns metadata and write tables.sql."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_assemble_tables(input, project_root)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2 if "Corrupt JSON" in str(exc) else 1) from exc
    typer.echo(json.dumps(result))


@app.command("write-catalog")
def write_catalog(
    staging_dir: Path = typer.Option(..., help="Directory with staging JSON files from MCP queries"),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
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
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_write_catalog(staging_dir, project_root, database)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2 if "Corrupt JSON" in str(exc) else 1) from exc
    typer.echo(json.dumps(result))


@app.command("write-manifest")
def write_manifest(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing ddl/, catalog/, manifest.json (defaults to CWD)"
    ),
    technology: str = typer.Option(..., help="Source technology: sql_server, fabric_warehouse, fabric_lakehouse, snowflake, oracle"),
    database: str = typer.Option(..., help="Source database name"),
    schemas: str = typer.Option(..., help="Comma-separated list of extracted schemas"),
) -> None:
    """Write manifest.json to the project root."""
    if project_root is None:
        project_root = Path.cwd()
    schema_list = [s.strip() for s in schemas.split(",")]
    try:
        result = run_write_manifest(project_root, technology, database, schema_list)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result))


@app.command("write-partial-manifest")
def write_partial_manifest(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root directory (defaults to CWD)"
    ),
    technology: str = typer.Option(..., help="Source technology: sql_server, fabric_warehouse, fabric_lakehouse, snowflake, oracle"),
) -> None:
    """Write a partial manifest.json with technology and dialect only."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_write_partial_manifest(project_root, technology)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(json.dumps(result))


@app.command("list-databases")
def list_databases(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)"
    ),
) -> None:
    """List user databases on the source system (SQL Server only)."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_list_databases(project_root)
    except UnsupportedOperationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(result))


@app.command("list-schemas")
def list_schemas(
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)"
    ),
    database: Optional[str] = typer.Option(
        None, "--database",
        help="Source database name (required for SQL Server / Fabric Warehouse)"
    ),
) -> None:
    """List schemas with object counts on the source system."""
    if project_root is None:
        project_root = Path.cwd()
    try:
        result = run_list_schemas(project_root, database)
    except UnsupportedOperationError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(result))


@app.command("extract")
def extract(
    database: Optional[str] = typer.Option(
        None, "--database",
        help="Source database name (required for SQL Server / Fabric Warehouse; ignored for Oracle)",
    ),
    schemas: str = typer.Option(
        ..., "--schemas",
        help="Comma-separated list of schemas to extract",
    ),
    project_root: Optional[Path] = typer.Option(
        None, "--project-root",
        help="Project root containing manifest.json (defaults to CWD)",
    ),
) -> None:
    """Connect to the source DB and extract DDL, catalog, and manifest in one pass.

    Reads technology from manifest.json. Runs all extraction queries internally,
    writes ddl/, catalog/, and manifest.json, then runs catalog-enrich.
    Preserves any existing LLM-enriched catalog fields (scoping, profile, refactor).
    """
    if project_root is None:
        project_root = Path.cwd()
    schema_list = [s.strip() for s in schemas.split(",") if s.strip()]
    try:
        result = run_extract(project_root, database, schema_list)
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(2) from exc
    typer.echo(json.dumps(result))


if __name__ == "__main__":
    app()
