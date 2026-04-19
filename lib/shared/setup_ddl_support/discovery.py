"""Source database and schema discovery entrypoints for setup-ddl."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts, oracle_connect, sql_server_connect
from shared.setup_ddl_support.manifest import (
    UnsupportedOperationError,
    build_oracle_schema_summary,
    require_technology,
)

logger = logging.getLogger(__name__)


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
