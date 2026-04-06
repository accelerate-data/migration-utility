from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts as _rows_to_dicts
from shared.db_connect import sql_server_connect as _sql_server_connect

logger = logging.getLogger(__name__)


def _write_json(staging_dir: Path, filename: str, rows: list[dict[str, Any]]) -> None:
    """Write rows as JSON to staging_dir / filename."""
    out_path = staging_dir / filename
    out_path.write_text(json.dumps(rows, default=str), encoding="utf-8")
    logger.info("event=sqlserver_query file=%s rows=%d", filename, len(rows))


def _build_in_clause(schemas: list[str]) -> str:
    """Build a SQL IN clause literal from validated schema names."""
    for s in schemas:
        if "'" in s or ";" in s:
            raise ValueError(f"Invalid schema name: {s!r}")
    return ", ".join(f"'{s}'" for s in schemas)


def _run_dmf_queries(
    conn: Any,
    schemas: list[str],
    object_type_filter: str,
    staging_dir: Path,
    filename: str,
) -> None:
    """Run sys.dm_sql_referenced_entities for each matching object via a Python loop.

    Iterates over all objects of the given type in the selected schemas and
    calls the DMF individually, accumulating all rows into one list.
    """
    in_clause = _build_in_clause(schemas)

    if object_type_filter == "P":
        type_predicate = "o.type = 'P'"
    elif object_type_filter == "V":
        type_predicate = "o.type = 'V'"
    else:
        type_predicate = "o.type IN ('FN', 'IF', 'TF')"

    list_sql = (
        f"SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name "
        f"FROM sys.objects o "
        f"WHERE {type_predicate} AND o.is_ms_shipped = 0 "
        f"  AND SCHEMA_NAME(o.schema_id) IN ({in_clause})"
    )
    cursor = conn.cursor()
    cursor.execute(list_sql)
    objects = [(row[0], row[1]) for row in cursor.fetchall()]
    cursor.close()

    all_rows: list[dict[str, Any]] = []
    dmf_columns = [
        "referencing_schema",
        "referencing_name",
        "referenced_schema",
        "referenced_entity",
        "referenced_minor_name",
        "referenced_class_desc",
        "is_selected",
        "is_updated",
        "is_select_all",
        "is_insert_all",
        "is_all_columns_found",
        "is_caller_dependent",
        "is_ambiguous",
    ]

    for schema_name, obj_name in objects:
        qualified = f"[{schema_name}].[{obj_name}]"
        dmf_sql = (
            f"SELECT "
            f"    '{schema_name}' AS referencing_schema, "
            f"    '{obj_name}' AS referencing_name, "
            f"    ISNULL(ref.referenced_schema_name, '') AS referenced_schema, "
            f"    ISNULL(ref.referenced_entity_name, '') AS referenced_entity, "
            f"    ISNULL(ref.referenced_minor_name, '') AS referenced_minor_name, "
            f"    ISNULL(ref.referenced_class_desc, '') AS referenced_class_desc, "
            f"    ISNULL(ref.is_selected, 0) AS is_selected, "
            f"    ISNULL(ref.is_updated, 0) AS is_updated, "
            f"    ISNULL(ref.is_select_all, 0) AS is_select_all, "
            f"    ISNULL(ref.is_insert_all, 0) AS is_insert_all, "
            f"    ISNULL(ref.is_all_columns_found, 0) AS is_all_columns_found, "
            f"    ISNULL(ref.is_caller_dependent, 0) AS is_caller_dependent, "
            f"    ISNULL(ref.is_ambiguous, 0) AS is_ambiguous "
            f"FROM sys.dm_sql_referenced_entities('{qualified}', 'OBJECT') ref"
        )
        try:
            dmf_cursor = conn.cursor()
            dmf_cursor.execute(dmf_sql)
            rows = _rows_to_dicts(dmf_cursor)
            dmf_cursor.close()
            all_rows.extend(rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "event=sqlserver_dmf_skip object=%s.%s error=%s",
                schema_name,
                obj_name,
                exc,
            )
            all_rows.append(
                dict(zip(dmf_columns, [
                    schema_name,
                    obj_name,
                    "",
                    "",
                    "",
                    f"ERROR: {exc}",
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                ]))
            )

    _write_json(staging_dir, filename, all_rows)


def run_sqlserver_extraction(
    staging_dir: Path,
    database: str,
    schemas: list[str],
) -> None:
    """Connect to SQL Server and run all extraction queries.

    Writes staging JSON files to staging_dir. Each file contains a list of
    row dicts. Empty results write an empty list (not skipped).

    Raises ValueError if connection env vars are missing or schemas list is empty.
    Raises RuntimeError if pyodbc is not installed.
    """
    if not schemas:
        raise ValueError("schemas list must be non-empty")

    logger.info(
        "event=sqlserver_extract database=%s schemas=%s",
        database,
        schemas,
    )

    in_clause = _build_in_clause(schemas)
    conn: Any = None

    try:
        conn = _sql_server_connect(database)

        # --- table_columns.json ---
        sql = (
            f"SELECT "
            f"    SCHEMA_NAME(t.schema_id) AS schema_name, "
            f"    t.name AS table_name, "
            f"    c.name AS column_name, "
            f"    c.column_id, "
            f"    tp.name AS type_name, "
            f"    c.max_length, "
            f"    c.precision, "
            f"    c.scale, "
            f"    c.is_nullable, "
            f"    c.is_identity, "
            f"    CONVERT(BIGINT, ic.seed_value) AS seed_value, "
            f"    CONVERT(BIGINT, ic.increment_value) AS increment_value "
            f"FROM sys.tables t "
            f"JOIN sys.columns c ON c.object_id = t.object_id "
            f"JOIN sys.types tp ON tp.user_type_id = c.user_type_id "
            f"LEFT JOIN sys.identity_columns ic "
            f"    ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
            f"WHERE t.is_ms_shipped = 0 "
            f"  AND SCHEMA_NAME(t.schema_id) IN ({in_clause}) "
            f"ORDER BY schema_name, table_name, c.column_id"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "table_columns.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- pk_unique.json ---
        sql = (
            f"SELECT "
            f"    SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
            f"    i.name AS index_name, i.is_unique, i.is_primary_key, "
            f"    c.name AS column_name, ic.key_ordinal "
            f"FROM sys.tables t "
            f"JOIN sys.indexes i ON i.object_id = t.object_id "
            f"    AND (i.is_primary_key = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0)) "
            f"JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            f"JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause}) "
            f"ORDER BY schema_name, table_name, i.index_id, ic.key_ordinal"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "pk_unique.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- foreign_keys.json ---
        sql = (
            f"SELECT "
            f"    SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
            f"    fk.name AS constraint_name, "
            f"    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name, "
            f"    SCHEMA_NAME(rt.schema_id) AS ref_schema, rt.name AS ref_table, "
            f"    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column "
            f"FROM sys.foreign_keys fk "
            f"JOIN sys.tables t ON t.object_id = fk.parent_object_id "
            f"JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
            f"JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id "
            f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause}) "
            f"ORDER BY schema_name, table_name, fk.name, fkc.constraint_column_id"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "foreign_keys.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- identity_columns.json ---
        sql = (
            f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
            f"       c.name AS column_name "
            f"FROM sys.identity_columns c "
            f"JOIN sys.tables t ON t.object_id = c.object_id "
            f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause})"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "identity_columns.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- cdc.json ---
        sql = (
            f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name "
            f"FROM sys.tables t "
            f"WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 1 "
            f"  AND SCHEMA_NAME(t.schema_id) IN ({in_clause})"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "cdc.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- change_tracking.json (graceful) ---
        try:
            sql = (
                f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name "
                f"FROM sys.change_tracking_tables ct "
                f"JOIN sys.tables t ON t.object_id = ct.object_id "
                f"WHERE SCHEMA_NAME(t.schema_id) IN ({in_clause})"
            )
            cursor = conn.cursor()
            cursor.execute(sql)
            _write_json(staging_dir, "change_tracking.json", _rows_to_dicts(cursor))
            cursor.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "event=sqlserver_query_skip file=change_tracking.json error=%s", exc
            )
            _write_json(staging_dir, "change_tracking.json", [])

        # --- sensitivity.json (graceful) ---
        try:
            sql = (
                f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
                f"       sc.label, sc.information_type, "
                f"       COL_NAME(sc.major_id, sc.minor_id) AS column_name "
                f"FROM sys.sensitivity_classifications sc "
                f"JOIN sys.tables t ON t.object_id = sc.major_id "
                f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause})"
            )
            cursor = conn.cursor()
            cursor.execute(sql)
            _write_json(staging_dir, "sensitivity.json", _rows_to_dicts(cursor))
            cursor.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "event=sqlserver_query_skip file=sensitivity.json error=%s", exc
            )
            _write_json(staging_dir, "sensitivity.json", [])

        # --- object_types.json ---
        sql = (
            f"SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name, o.type "
            f"FROM sys.objects o "
            f"WHERE o.is_ms_shipped = 0 "
            f"  AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF') "
            f"  AND SCHEMA_NAME(o.schema_id) IN ({in_clause})"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "object_types.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- definitions.json ---
        sql = (
            f"SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name, "
            f"       OBJECT_DEFINITION(o.object_id) AS definition "
            f"FROM sys.objects o "
            f"WHERE o.type IN ('P', 'V', 'FN', 'IF', 'TF') AND o.is_ms_shipped = 0 "
            f"  AND SCHEMA_NAME(o.schema_id) IN ({in_clause})"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "definitions.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- proc_params.json ---
        sql = (
            f"SELECT "
            f"    SCHEMA_NAME(o.schema_id) AS schema_name, "
            f"    o.name AS proc_name, "
            f"    p.name AS param_name, "
            f"    TYPE_NAME(p.user_type_id) AS type_name, "
            f"    p.max_length, "
            f"    p.precision, "
            f"    p.scale, "
            f"    p.is_output, "
            f"    p.has_default_value "
            f"FROM sys.parameters p "
            f"JOIN sys.objects o ON o.object_id = p.object_id "
            f"WHERE o.type = 'P' AND o.is_ms_shipped = 0 AND p.parameter_id > 0 "
            f"  AND SCHEMA_NAME(o.schema_id) IN ({in_clause}) "
            f"ORDER BY schema_name, proc_name, p.parameter_id"
        )
        cursor = conn.cursor()
        cursor.execute(sql)
        _write_json(staging_dir, "proc_params.json", _rows_to_dicts(cursor))
        cursor.close()

        # --- proc_dmf.json, view_dmf.json, func_dmf.json ---
        _run_dmf_queries(conn, schemas, "P", staging_dir, "proc_dmf.json")
        _run_dmf_queries(conn, schemas, "V", staging_dir, "view_dmf.json")
        _run_dmf_queries(conn, schemas, "FN", staging_dir, "func_dmf.json")

    finally:
        if conn is not None:
            conn.close()
