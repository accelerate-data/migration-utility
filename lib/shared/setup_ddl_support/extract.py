"""Extraction and source-discovery entrypoints for setup-ddl."""

from __future__ import annotations

import json
import logging
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts, oracle_connect, sql_server_connect
from shared.name_resolver import normalize
from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale, run_write_catalog
from shared.setup_ddl_support.manifest import (
    TECH_DIALECT,
    UnsupportedOperationError,
    build_oracle_schema_summary,
    get_connection_identity,
    identity_changed,
    read_manifest_strict,
    require_technology,
    run_write_manifest,
)
from shared.setup_ddl_support.staging_io import read_json, read_json_optional
from shared.sql_types import format_sql_type

logger = logging.getLogger(__name__)


def run_assemble_modules(input_path: Path, project_root: Path, object_type: str) -> dict[str, Any]:
    if object_type not in ("procedures", "views", "functions"):
        raise ValueError(f"Invalid type: {object_type}. Must be procedures, views, or functions.")
    rows = read_json(input_path)
    blocks = [row.get("definition", "").strip() for row in rows if row.get("definition")]
    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / f"{object_type}.sql"
    out_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return {"file": str(out_path), "count": len(blocks)}


def run_assemble_tables(input_path: Path, project_root: Path) -> dict[str, Any]:
    rows = read_json(input_path)
    technology = require_technology(project_root)
    oracle_style = technology == "oracle"
    tables: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        tables.setdefault((row["schema_name"], row["table_name"]), []).append(row)
    for cols in tables.values():
        cols.sort(key=lambda r: r.get("column_id", 0))
    blocks: list[str] = []
    for (schema_name, table_name), cols in tables.items():
        col_defs: list[str] = []
        for col in cols:
            type_str = format_sql_type(col["type_name"], col["max_length"], col["precision"], col["scale"])
            nullable = " NOT NULL" if not col.get("is_nullable") else " NULL"
            if oracle_style:
                col_defs.append(f"    {col['column_name']} {type_str}{nullable}")
            else:
                identity = ""
                if col.get("is_identity"):
                    identity = f" IDENTITY({col.get('seed_value', 1)},{col.get('increment_value', 1)})"
                col_defs.append(f"    [{col['column_name']}] {type_str}{identity}{nullable}")
        if oracle_style:
            ddl = f"CREATE TABLE {schema_name}.{table_name} (\n" + ",\n".join(col_defs) + "\n)"
        else:
            ddl = f"CREATE TABLE [{schema_name}].[{table_name}] (\n" + ",\n".join(col_defs) + "\n)"
        blocks.append(ddl)
    ddl_dir = project_root / "ddl"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddl_dir / "tables.sql"
    out_path.write_text("\nGO\n".join(blocks) + ("\nGO\n" if blocks else ""), encoding="utf-8")
    return {"file": str(out_path), "count": len(blocks)}


def run_list_databases(project_root: Path) -> dict[str, Any]:
    technology = require_technology(project_root)
    if technology == "oracle":
        raise UnsupportedOperationError("list-databases is not supported for Oracle.")
    conn = sql_server_connect("master")
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
    logger.info("event=list_databases technology=%s count=%d", technology, len(databases))
    return {"databases": databases}


def run_list_schemas(project_root: Path, database: str | None) -> dict[str, Any]:
    technology = require_technology(project_root)
    if technology == "sql_server":
        if not database:
            raise ValueError("--database is required for SQL Server")
        conn = sql_server_connect(database)
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
        logger.info("event=list_schemas technology=%s database=%s count=%d", technology, database, len(schemas))
        return {"schemas": schemas}
    if technology == "oracle":
        conn = oracle_connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT OWNER, OBJECT_TYPE, OBJECT_NAME "
                "FROM ALL_OBJECTS "
                "ORDER BY OWNER, OBJECT_TYPE, OBJECT_NAME"
            )
            rows = cursor_to_dicts(cursor)
        finally:
            conn.close()
        schemas = build_oracle_schema_summary(rows)
        logger.info("event=list_schemas technology=oracle count=%d", len(schemas))
        return {"schemas": schemas}
    raise ValueError(f"list-schemas is not supported for technology '{technology}'")


def run_db_extraction(technology: str, staging_dir: Path, db_name: str, schemas: list[str]) -> None:
    if technology == "sql_server":
        from shared.sqlserver_extract import run_sqlserver_extraction

        run_sqlserver_extraction(staging_dir, db_name, schemas)
    elif technology == "oracle":
        from shared.oracle_extract import run_oracle_extraction

        run_oracle_extraction(staging_dir, schemas)
    else:
        raise ValueError(f"setup-ddl extract is not supported for technology '{technology}'")


def _repo_relative(project_root: Path, path: str | Path) -> str:
    raw_path = Path(path)
    resolved = raw_path if raw_path.is_absolute() else project_root / raw_path
    try:
        return str(resolved.relative_to(project_root))
    except ValueError:
        return str(raw_path)


def _catalog_snapshot(project_root: Path) -> dict[str, str]:
    catalog_dir = project_root / "catalog"
    if not catalog_dir.is_dir():
        return {}
    return {
        str(path.relative_to(project_root)): sha256(path.read_bytes()).hexdigest()
        for path in sorted(catalog_dir.glob("*/*.json"))
        if path.is_file()
    }


def _changed_catalog_paths(project_root: Path, before: dict[str, str]) -> list[str]:
    after = _catalog_snapshot(project_root)
    return sorted(path for path, digest in after.items() if before.get(path) != digest)


def assemble_ddl_from_staging(staging_dir: Path, project_root: Path) -> list[str]:
    written_paths: list[str] = []
    obj_type_rows = read_json_optional(staging_dir / "object_types.json")
    type_lookup = {
        normalize(f"{row['schema_name']}.{row['name']}"): row.get("type", "").strip()
        for row in obj_type_rows
    }
    definitions_rows = read_json_optional(staging_dir / "definitions.json")
    for obj_label, type_codes in [("procedures", {"P"}), ("views", {"V"}), ("functions", {"FN", "IF", "TF"})]:
        typed_defs = [
            row
            for row in definitions_rows
            if type_lookup.get(normalize(f"{row['schema_name']}.{row['object_name']}")) in type_codes
        ]
        if typed_defs:
            typed_path = staging_dir / f"{obj_label}.json"
            typed_path.write_text(json.dumps(typed_defs, ensure_ascii=False), encoding="utf-8")
            result = run_assemble_modules(typed_path, project_root, obj_label)
            written_paths.append(_repo_relative(project_root, result["file"]))
    table_cols_path = staging_dir / "table_columns.json"
    if table_cols_path.exists():
        result = run_assemble_tables(table_cols_path, project_root)
        written_paths.append(_repo_relative(project_root, result["file"]))
    return written_paths


def run_extract(project_root: Path, database: str | None, schemas: list[str]) -> dict[str, Any]:
    from shared.catalog import restore_enriched_fields, snapshot_enriched_fields
    from shared.catalog_enrich import enrich_catalog
    from shared.diagnostics import run_diagnostics

    technology = require_technology(project_root)
    if not schemas:
        raise ValueError("--schemas is required and must be non-empty")
    if technology == "sql_server" and not database:
        raise ValueError(f"--database is required for technology '{technology}'")
    dialect = TECH_DIALECT.get(technology, "tsql")
    db_name = database or ""
    logger.info("event=extract_start technology=%s database=%s schemas=%s", technology, db_name, schemas)
    existing_manifest = read_manifest_strict(project_root)
    current_identity = get_connection_identity(technology, db_name)
    if identity_changed(existing_manifest, current_identity):
        logger.info("event=identity_changed technology=%s pre_stale_all=true", technology)
        mark_all_catalog_stale(project_root)
    enriched_snapshot = snapshot_enriched_fields(project_root)
    with tempfile.TemporaryDirectory() as tmp:
        staging_dir = Path(tmp)
        run_db_extraction(technology, staging_dir, db_name, schemas)
        ddl_paths = assemble_ddl_from_staging(staging_dir, project_root)
        run_write_manifest(project_root, technology, db_name, schemas)
        counts = run_write_catalog(staging_dir, project_root, db_name)
    catalog_snapshot = _catalog_snapshot(project_root)
    restore_enriched_fields(project_root, enriched_snapshot)
    enrich_result = enrich_catalog(project_root, dialect=dialect)
    diag_result = run_diagnostics(project_root, dialect=dialect)
    logger.info(
        "event=extract_complete technology=%s tables=%s procedures=%s enrich=%s diagnostics=%s",
        technology, counts.get("tables"), counts.get("procedures"), enrich_result, diag_result,
    )
    written_paths = ["manifest.json", *ddl_paths]
    written_paths.extend(str(path) for path in counts.get("written_paths", []))
    written_paths.extend(_changed_catalog_paths(project_root, catalog_snapshot))
    return {
        **counts,
        "written_paths": sorted(set(written_paths)),
        "enrich": enrich_result.model_dump() if hasattr(enrich_result, "model_dump") else enrich_result,
        "diagnostics": diag_result,
    }
