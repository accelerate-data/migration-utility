from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts as _cursor_rows
from shared.oracle_extract_ddl import oracle_type_to_class_desc
from shared.oracle_extract_queries import (
    dmf_sql,
    foreign_keys_sql,
    identity_columns_sql,
    invalid_object_types_sql,
    object_types_sql,
    packages_sql,
    pk_unique_sql,
    proc_params_sql,
    table_columns_sql,
)
from shared.setup_ddl_support.db_helpers import write_staging_json

logger = logging.getLogger(__name__)


def write_oracle_staging_json(staging_dir: Path, filename: str, rows: list[Any]) -> None:
    """Write Oracle extraction rows to a staging JSON file."""
    write_staging_json(staging_dir, filename, rows, logger=logger, event_name="oracle_query")


def extract_table_columns(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(table_columns_sql(schemas))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "schema_name": row["OWNER"],
            "table_name": row["TABLE_NAME"],
            "column_name": row["COLUMN_NAME"],
            "column_id": row["COLUMN_ID"],
            "type_name": row["DATA_TYPE"],
            "max_length": oracle_column_length(row),
            "precision": row["DATA_PRECISION"] if row["DATA_PRECISION"] is not None else 0,
            "scale": row["DATA_SCALE"] if row["DATA_SCALE"] is not None else 0,
            "is_nullable": 1 if row["NULLABLE"] == "Y" else 0,
            "is_identity": 1 if row.get("IDENTITY_COLUMN") == "YES" else 0,
            "seed_value": None,
            "increment_value": None,
        })
    return rows


def oracle_column_length(row: dict[str, Any]) -> int:
    type_name = str(row.get("DATA_TYPE") or "").upper()
    if type_name in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        value = row.get("CHAR_LENGTH")
    else:
        value = row.get("DATA_LENGTH")
    return int(value) if value is not None else 0


def extract_pk_unique(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(pk_unique_sql(schemas))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "schema_name": row["OWNER"],
            "table_name": row["TABLE_NAME"],
            "index_name": row["CONSTRAINT_NAME"],
            "is_unique": 1,
            "is_primary_key": 1 if row["CONSTRAINT_TYPE"] == "P" else 0,
            "column_name": row["COLUMN_NAME"],
            "key_ordinal": row["POSITION"],
        })
    return rows


def extract_foreign_keys(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(foreign_keys_sql(schemas))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "schema_name": row["OWNER"],
            "table_name": row["TABLE_NAME"],
            "constraint_name": row["CONSTRAINT_NAME"],
            "column_name": row["COLUMN_NAME"],
            "ref_schema": row["REF_OWNER"],
            "ref_table": row["REF_TABLE_NAME"],
            "ref_column": row["REF_COLUMN_NAME"],
        })
    return rows


def extract_identity_columns(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(identity_columns_sql(schemas))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "schema_name": row["OWNER"],
            "table_name": row["TABLE_NAME"],
            "column_name": row["COLUMN_NAME"],
        })
    return rows


def extract_object_types(conn: Any, schemas: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Extract object types and return object type rows plus materialized-view FQNs."""
    cur = conn.cursor()
    cur.execute(object_types_sql(schemas))
    type_map = {
        "TABLE": "U",
        "VIEW": "V",
        "PROCEDURE": "P",
        "FUNCTION": "FN",
        "MATERIALIZED VIEW": "V",
    }
    rows = []
    mv_fqns: list[str] = []
    for row in _cursor_rows(cur):
        obj_type = row["OBJECT_TYPE"]
        fqn = f"{row['OWNER']}.{row['OBJECT_NAME']}".lower()
        rows.append({
            "schema_name": row["OWNER"],
            "name": row["OBJECT_NAME"],
            "type": type_map.get(obj_type, obj_type),
        })
        if obj_type == "MATERIALIZED VIEW":
            mv_fqns.append(fqn)

    invalid_cur = conn.cursor()
    invalid_cur.execute(invalid_object_types_sql(schemas))
    for inv_row in _cursor_rows(invalid_cur):
        logger.warning(
            "event=oracle_invalid_object owner=%s name=%s type=%s status=%s",
            inv_row["OWNER"],
            inv_row["OBJECT_NAME"],
            inv_row["OBJECT_TYPE"],
            inv_row["STATUS"],
        )

    return rows, mv_fqns


def extract_dmf(conn: Any, schemas: list[str], dep_type: str) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(dmf_sql(schemas, dep_type))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "referencing_schema": row["OWNER"],
            "referencing_name": row["NAME"],
            "referenced_schema": row["REFERENCED_OWNER"],
            "referenced_entity": row["REFERENCED_NAME"],
            "referenced_minor_name": "",
            "referenced_class_desc": oracle_type_to_class_desc(row["REFERENCED_TYPE"]),
            "is_selected": False,
            "is_updated": False,
            "is_select_all": False,
            "is_insert_all": False,
            "is_all_columns_found": False,
            "is_caller_dependent": False,
            "is_ambiguous": False,
        })
    return rows


def extract_proc_params(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(proc_params_sql(schemas))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "schema_name": row["OWNER"],
            "proc_name": row["OBJECT_NAME"],
            "param_name": row["ARGUMENT_NAME"],
            "type_name": row["DATA_TYPE"],
            "max_length": row["DATA_LENGTH"] if row["DATA_LENGTH"] is not None else 0,
            "precision": row["DATA_PRECISION"] if row["DATA_PRECISION"] is not None else 0,
            "scale": row["DATA_SCALE"] if row["DATA_SCALE"] is not None else 0,
            "is_output": row["IN_OUT"] in ("OUT", "IN/OUT"),
            "has_default_value": row["DEFAULTED"] == "Y",
        })
    return rows


def extract_packages(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    """Extract package member names for package-aware diagnostics."""
    cur = conn.cursor()
    cur.execute(packages_sql(schemas))
    rows = []
    for row in _cursor_rows(cur):
        rows.append({
            "schema_name": row["OWNER"],
            "package_name": row["PACKAGE_NAME"],
            "member_name": row["OBJECT_NAME"],
            "member_type": row["MEMBER_TYPE"],
        })
    return rows


__all__ = [
    "write_oracle_staging_json",
    "extract_table_columns",
    "oracle_column_length",
    "extract_pk_unique",
    "extract_foreign_keys",
    "extract_identity_columns",
    "extract_object_types",
    "extract_dmf",
    "extract_proc_params",
    "extract_packages",
]
