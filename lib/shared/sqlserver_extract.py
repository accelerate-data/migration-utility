from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts as _rows_to_dicts
from shared.db_connect import sql_server_connect as _sql_server_connect
from shared.setup_ddl_support.db_helpers import build_schema_in_clause, write_staging_json

logger = logging.getLogger(__name__)


def _write_json(staging_dir: Path, filename: str, rows: list[Any]) -> None:
    """Write rows as JSON to staging_dir / filename."""
    write_staging_json(staging_dir, filename, rows, logger=logger, event_name="sqlserver_query")


def _is_optional_metadata_unavailable(exc: Exception) -> bool:
    """Return True when a metadata query failed because the feature is unavailable."""
    message = str(exc).lower()
    markers = (
        "invalid object name",
        "is not supported",
        "not supported in this version",
        "could not find stored procedure",
        "cannot find the object",
    )
    return any(marker in message for marker in markers)


@dataclass(frozen=True)
class _SqlServerQuerySpec:
    filename: str
    sql_factory: Callable[[list[str]], str]
    optional: bool = False
    row_transform: Callable[[list[dict[str, Any]]], list[Any]] | None = None


def _run_sqlserver_query_spec(
    conn: Any,
    spec: _SqlServerQuerySpec,
    staging_dir: Path,
    schemas: list[str],
) -> None:
    """Run one SQL Server metadata extraction query and write its staging artifact."""
    cursor: Any = None
    try:
        cursor = conn.cursor()
        sql = spec.sql_factory(schemas)
        cursor.execute(sql)
        rows: list[Any] = _rows_to_dicts(cursor)
        if spec.row_transform is not None:
            rows = spec.row_transform(rows)
    except Exception as exc:  # noqa: BLE001
        if not spec.optional or not _is_optional_metadata_unavailable(exc):
            raise
        logger.warning(
            "event=sqlserver_query_skip file=%s reason=feature_unavailable error=%s",
            spec.filename,
            exc,
        )
        rows = []
    finally:
        if cursor is not None:
            cursor.close()

    _write_json(staging_dir, spec.filename, rows)


def _schema_clause(schemas: list[str]) -> str:
    return build_schema_in_clause(schemas)


def _table_columns_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
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


def _pk_unique_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT "
        f"    SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
        f"    i.name AS index_name, i.is_unique, i.is_primary_key, "
        f"    c.name AS column_name, ic.key_ordinal "
        f"FROM sys.tables t "
        f"JOIN sys.indexes i ON i.object_id = t.object_id "
        f"    AND (i.is_primary_key = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0 AND i.has_filter = 0)) "
        f"JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        f"    AND ic.key_ordinal > 0 AND ic.is_included_column = 0 "
        f"JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause}) "
        f"ORDER BY schema_name, table_name, i.index_id, ic.key_ordinal"
    )


def _foreign_keys_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
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


def _identity_columns_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
        f"       c.name AS column_name "
        f"FROM sys.identity_columns c "
        f"JOIN sys.tables t ON t.object_id = c.object_id "
        f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause})"
    )


def _cdc_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name "
        f"FROM sys.tables t "
        f"WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 1 "
        f"  AND SCHEMA_NAME(t.schema_id) IN ({in_clause})"
    )


def _change_tracking_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name "
        f"FROM sys.change_tracking_tables ct "
        f"JOIN sys.tables t ON t.object_id = ct.object_id "
        f"WHERE SCHEMA_NAME(t.schema_id) IN ({in_clause})"
    )


def _sensitivity_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, "
        f"       sc.label, sc.information_type, "
        f"       COL_NAME(sc.major_id, sc.minor_id) AS column_name "
        f"FROM sys.sensitivity_classifications sc "
        f"JOIN sys.tables t ON t.object_id = sc.major_id "
        f"WHERE t.is_ms_shipped = 0 AND SCHEMA_NAME(t.schema_id) IN ({in_clause})"
    )


def _object_types_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name, o.type "
        f"FROM sys.objects o "
        f"WHERE o.is_ms_shipped = 0 "
        f"  AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF') "
        f"  AND SCHEMA_NAME(o.schema_id) IN ({in_clause})"
    )


def _definitions_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name, "
        f"       OBJECT_DEFINITION(o.object_id) AS definition "
        f"FROM sys.objects o "
        f"WHERE o.type IN ('P', 'V', 'FN', 'IF', 'TF') AND o.is_ms_shipped = 0 "
        f"  AND SCHEMA_NAME(o.schema_id) IN ({in_clause})"
    )


def _proc_params_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
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


def _view_columns_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(v.schema_id) AS schema_name, "
        f"       v.name AS view_name, "
        f"       c.name AS column_name, c.column_id, "
        f"       tp.name AS type_name, c.max_length, c.precision, c.scale, "
        f"       c.is_nullable "
        f"FROM sys.views v "
        f"JOIN sys.columns c ON c.object_id = v.object_id "
        f"JOIN sys.types tp ON tp.user_type_id = c.user_type_id "
        f"WHERE v.is_ms_shipped = 0 "
        f"  AND SCHEMA_NAME(v.schema_id) IN ({in_clause}) "
        f"ORDER BY schema_name, view_name, c.column_id"
    )


def _indexed_views_sql(schemas: list[str]) -> str:
    in_clause = _schema_clause(schemas)
    return (
        f"SELECT SCHEMA_NAME(v.schema_id) AS schema_name, v.name "
        f"FROM sys.views v "
        f"WHERE EXISTS (SELECT 1 FROM sys.indexes i "
        f"  WHERE i.object_id = v.object_id AND i.type = 1) "
        f"  AND v.is_ms_shipped = 0 "
        f"  AND SCHEMA_NAME(v.schema_id) IN ({in_clause})"
    )


def _indexed_view_fqns(rows: list[dict[str, Any]]) -> list[str]:
    return [f"{row['schema_name']}.{row['name']}".lower() for row in rows]


def _sqlserver_query_specs() -> tuple[_SqlServerQuerySpec, ...]:
    return (
        _SqlServerQuerySpec("table_columns.json", _table_columns_sql),
        _SqlServerQuerySpec("pk_unique.json", _pk_unique_sql),
        _SqlServerQuerySpec("foreign_keys.json", _foreign_keys_sql),
        _SqlServerQuerySpec("identity_columns.json", _identity_columns_sql),
        _SqlServerQuerySpec("cdc.json", _cdc_sql),
        _SqlServerQuerySpec("change_tracking.json", _change_tracking_sql, optional=True),
        _SqlServerQuerySpec("sensitivity.json", _sensitivity_sql, optional=True),
        _SqlServerQuerySpec("object_types.json", _object_types_sql),
        _SqlServerQuerySpec("definitions.json", _definitions_sql),
        _SqlServerQuerySpec("proc_params.json", _proc_params_sql),
        _SqlServerQuerySpec("view_columns.json", _view_columns_sql),
        _SqlServerQuerySpec("indexed_views.json", _indexed_views_sql, row_transform=_indexed_view_fqns),
    )


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
    in_clause = build_schema_in_clause(schemas)

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
        schema_name_lit = schema_name.replace("'", "''")
        obj_name_lit = obj_name.replace("'", "''")
        dmf_sql = (
            f"SELECT "
            f"    '{schema_name_lit}' AS referencing_schema, "
            f"    '{obj_name_lit}' AS referencing_name, "
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

    conn: Any = None

    try:
        conn = _sql_server_connect(database)
        for spec in _sqlserver_query_specs():
            _run_sqlserver_query_spec(conn, spec, staging_dir, schemas)

        _run_dmf_queries(conn, schemas, "P", staging_dir, "proc_dmf.json")
        _run_dmf_queries(conn, schemas, "V", staging_dir, "view_dmf.json")
        _run_dmf_queries(conn, schemas, "FN", staging_dir, "func_dmf.json")

    finally:
        if conn is not None:
            conn.close()
