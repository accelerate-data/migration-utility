from __future__ import annotations

import logging
from typing import Any

from shared.db_connect import cursor_to_dicts as _cursor_rows
from shared.oracle_extract_queries import definitions_object_sql, view_text_sql

logger = logging.getLogger(__name__)


def oracle_type_to_class_desc(oracle_type: str) -> str:
    mapping = {
        "TABLE": "USER_TABLE",
        "VIEW": "VIEW",
        "PROCEDURE": "SQL_STORED_PROCEDURE",
        "FUNCTION": "SQL_SCALAR_FUNCTION",
        "PACKAGE": "SQL_STORED_PROCEDURE",
    }
    return mapping.get(oracle_type.upper(), oracle_type.upper())


def read_metadata_ddl(conn: Any, object_type: str, object_name: str, owner: str) -> str:
    cur = conn.cursor()
    cur.execute(
        "SELECT DBMS_METADATA.GET_DDL(:obj_type, :obj_name, :owner) FROM DUAL",
        obj_type=object_type,
        obj_name=object_name,
        owner=owner,
    )
    result = cur.fetchone()
    value = result[0]
    return value.read() if hasattr(value, "read") else str(value)


def read_view_metadata_ddl(conn: Any, view_name: str, owner: str) -> str:
    cur = conn.cursor()
    cur.execute(
        "SELECT DBMS_METADATA.GET_DDL('VIEW', :n, :o) FROM DUAL",
        n=view_name,
        o=owner,
    )
    result = cur.fetchone()
    value = result[0]
    return value.read() if hasattr(value, "read") else str(value)


def definition_from_view_text(owner: str, view_name: str, text: str) -> str:
    return f"CREATE OR REPLACE VIEW {owner}.{view_name} AS\n{text.strip()}"


def extract_definition_rows(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(definitions_object_sql(schemas))
    objects = _cursor_rows(cur)

    rows: list[dict[str, Any]] = []
    for obj in objects:
        fqn = f"{obj['OWNER']}.{obj['OBJECT_TYPE']}.{obj['OBJECT_NAME']}"
        try:
            definition = read_metadata_ddl(conn, obj["OBJECT_TYPE"], obj["OBJECT_NAME"], obj["OWNER"])
            rows.append({
                "schema_name": obj["OWNER"],
                "object_name": obj["OBJECT_NAME"],
                "definition": definition,
            })
        except Exception as exc:  # noqa: BLE001
            logger.warning("event=oracle_ddl_skip object=%s error=%s", fqn, exc)

    return rows


def extract_view_ddl_rows(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    """Extract view DDL from ALL_VIEWS, reconstructing CREATE OR REPLACE VIEW statements."""
    cur = conn.cursor()
    cur.execute(view_text_sql(schemas))
    rows: list[dict[str, Any]] = []
    for row in _cursor_rows(cur):
        owner = row["OWNER"]
        view_name = row["VIEW_NAME"]
        text = row.get("TEXT") or ""
        fqn = f"{owner}.{view_name}"
        if not text.strip() or len(text) == 32767:
            try:
                definition = read_view_metadata_ddl(conn, view_name, owner).strip()
                rows.append({
                    "schema_name": owner,
                    "object_name": view_name,
                    "definition": definition,
                })
            except Exception as exc:  # noqa: BLE001
                logger.warning("event=oracle_view_long_truncation object=%s error=%s", fqn, exc)
                truncated_def = definition_from_view_text(owner, view_name, text) if text.strip() else ""
                if truncated_def:
                    rows.append({
                        "schema_name": owner,
                        "object_name": view_name,
                        "definition": truncated_def,
                        "long_truncation": True,
                    })
            continue
        rows.append({
            "schema_name": owner,
            "object_name": view_name,
            "definition": definition_from_view_text(owner, view_name, text),
        })
    logger.info("event=oracle_view_ddl count=%d", len(rows))
    return rows


__all__ = [
    "oracle_type_to_class_desc",
    "read_metadata_ddl",
    "read_view_metadata_ddl",
    "definition_from_view_text",
    "extract_definition_rows",
    "extract_view_ddl_rows",
]
