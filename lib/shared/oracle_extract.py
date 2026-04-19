from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from shared.db_connect import cursor_to_dicts as _cursor_rows
from shared.db_connect import oracle_connect as _oracle_connect
from shared.oracle_extract_queries import (
    dmf_sql,
    definitions_object_sql,
    foreign_keys_sql,
    identity_columns_sql,
    invalid_object_types_sql,
    object_types_sql,
    packages_sql,
    pk_unique_sql,
    proc_params_sql,
    table_columns_sql,
    view_text_sql,
)
from shared.setup_ddl_support.db_helpers import write_staging_json

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _oracle_type_to_class_desc(oracle_type: str) -> str:
    mapping = {
        "TABLE": "USER_TABLE",
        "VIEW": "VIEW",
        "PROCEDURE": "SQL_STORED_PROCEDURE",
        "FUNCTION": "SQL_SCALAR_FUNCTION",
        "PACKAGE": "SQL_STORED_PROCEDURE",
    }
    return mapping.get(oracle_type.upper(), oracle_type.upper())


def _write(staging_dir: Path, filename: str, rows: list[Any]) -> None:
    """Write rows as JSON to staging_dir / filename."""
    write_staging_json(staging_dir, filename, rows, logger=logger, event_name="oracle_query")


# ── Extraction functions ──────────────────────────────────────────────────────


def _extract_definitions(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(definitions_object_sql(schemas))
    objects = _cursor_rows(cur)

    rows: list[dict[str, Any]] = []
    for obj in objects:
        fqn = f"{obj['OWNER']}.{obj['OBJECT_TYPE']}.{obj['OBJECT_NAME']}"
        try:
            ddl_cur = conn.cursor()
            ddl_cur.execute(
                "SELECT DBMS_METADATA.GET_DDL(:obj_type, :obj_name, :owner) FROM DUAL",
                obj_type=obj["OBJECT_TYPE"],
                obj_name=obj["OBJECT_NAME"],
                owner=obj["OWNER"],
            )
            result = ddl_cur.fetchone()
            definition = result[0].read() if hasattr(result[0], "read") else str(result[0])
            rows.append({
                "schema_name": obj["OWNER"],
                "object_name": obj["OBJECT_NAME"],
                "definition": definition,
            })
        except Exception as exc:  # noqa: BLE001
            logger.warning("event=oracle_ddl_skip object=%s error=%s", fqn, exc)

    return rows


def _extract_view_ddl(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    """Extract view DDL from ALL_VIEWS, reconstructing CREATE OR REPLACE VIEW statements.

    ALL_VIEWS.TEXT is a LONG column containing the view body (the AS SELECT part).
    We reconstruct the full DDL as: CREATE OR REPLACE VIEW owner.view_name AS <TEXT>.

    Falls back to DBMS_METADATA.GET_DDL per view when TEXT is empty or at the
    32,767-byte LONG truncation boundary (oracledb thin mode silently truncates).
    """
    cur = conn.cursor()
    cur.execute(view_text_sql(schemas))
    rows: list[dict[str, Any]] = []
    for row in _cursor_rows(cur):
        owner = row["OWNER"]
        view_name = row["VIEW_NAME"]
        text = row.get("TEXT") or ""
        fqn = f"{owner}.{view_name}"
        # ALL_VIEWS.TEXT is a LONG column. oracledb thin mode silently truncates
        # LONG values at 32,767 bytes — truncated text arrives exactly at that
        # boundary without any error signal. Treat empty or 32,767-byte text as
        # potentially truncated and fall back to DBMS_METADATA.GET_DDL (CLOB).
        if not text.strip() or len(text) == 32767:
            try:
                ddl_cur = conn.cursor()
                ddl_cur.execute(
                    "SELECT DBMS_METADATA.GET_DDL('VIEW', :n, :o) FROM DUAL",
                    n=view_name,
                    o=owner,
                )
                result = ddl_cur.fetchone()
                definition = result[0].read() if hasattr(result[0], "read") else str(result[0])
                rows.append({
                    "schema_name": owner,
                    "object_name": view_name,
                    "definition": definition.strip(),
                })
            except Exception as exc:  # noqa: BLE001
                logger.warning("event=oracle_view_long_truncation object=%s error=%s", fqn, exc)
                # Create entry with truncated DDL so the view appears in catalog with a diagnostic
                truncated_def = f"CREATE OR REPLACE VIEW {owner}.{view_name} AS\n{text.strip()}" if text.strip() else ""
                if truncated_def:
                    rows.append({
                        "schema_name": owner,
                        "object_name": view_name,
                        "definition": truncated_def,
                        "long_truncation": True,
                    })
            continue
        definition = f"CREATE OR REPLACE VIEW {owner}.{view_name} AS\n{text.strip()}"
        rows.append({"schema_name": owner, "object_name": view_name, "definition": definition})
    logger.info("event=oracle_view_ddl count=%d", len(rows))
    return rows


def _extract_table_columns(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
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
            "max_length": _oracle_column_length(row),
            "precision": row["DATA_PRECISION"] if row["DATA_PRECISION"] is not None else 0,
            "scale": row["DATA_SCALE"] if row["DATA_SCALE"] is not None else 0,
            "is_nullable": 1 if row["NULLABLE"] == "Y" else 0,
            "is_identity": 1 if row.get("IDENTITY_COLUMN") == "YES" else 0,
            "seed_value": None,
            "increment_value": None,
        })
    return rows


def _oracle_column_length(row: dict[str, Any]) -> int:
    type_name = str(row.get("DATA_TYPE") or "").upper()
    if type_name in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"}:
        value = row.get("CHAR_LENGTH")
    else:
        value = row.get("DATA_LENGTH")
    return int(value) if value is not None else 0


def _extract_pk_unique(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
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


def _extract_foreign_keys(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
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


def _extract_identity_columns(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
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


def _extract_object_types(conn: Any, schemas: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    """Extract object types and return (rows, mv_fqns).

    Returns a 2-tuple: the object_types rows (with MVs mapped to type "V") and
    a list of normalized FQNs for materialized views.
    """
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

    # Log invalid objects for user awareness
    invalid_cur = conn.cursor()
    invalid_cur.execute(invalid_object_types_sql(schemas))
    for inv_row in _cursor_rows(invalid_cur):
        logger.warning(
            "event=oracle_invalid_object owner=%s name=%s type=%s status=%s",
            inv_row["OWNER"], inv_row["OBJECT_NAME"], inv_row["OBJECT_TYPE"], inv_row["STATUS"],
        )

    return rows, mv_fqns


def _extract_dmf(conn: Any, schemas: list[str], dep_type: str) -> list[dict[str, Any]]:
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
            "referenced_class_desc": _oracle_type_to_class_desc(row["REFERENCED_TYPE"]),
            "is_selected": False,
            "is_updated": False,
            "is_select_all": False,
            "is_insert_all": False,
            "is_all_columns_found": False,
            "is_caller_dependent": False,
            "is_ambiguous": False,
        })
    return rows


def _extract_proc_params(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
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


def _extract_packages(conn: Any, schemas: list[str]) -> list[dict[str, Any]]:
    """Extract package member names for package-aware diagnostics.

    Returns a list of dicts with package_name, member_name, member_type, schema_name.
    """
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


# ── Public entry point ────────────────────────────────────────────────────────


def run_oracle_extraction(
    staging_dir: Path,
    schemas: list[str],
) -> None:
    """Connect to Oracle and run all extraction queries.

    Writes staging JSON files to staging_dir. cdc.json, change_tracking.json,
    and sensitivity.json are always written as empty lists (Oracle does not
    support these signals).

    Raises ValueError if connection env vars are missing.
    Raises RuntimeError if oracledb is not installed.
    """
    logger.info("event=oracle_extract schemas=%s", schemas)

    conn = _oracle_connect()
    try:
        proc_func_defs = _extract_definitions(conn, schemas)
        view_defs = _extract_view_ddl(conn, schemas)
        _write(staging_dir, "definitions.json", proc_func_defs + view_defs)
        _write(staging_dir, "table_columns.json", _extract_table_columns(conn, schemas))
        _write(staging_dir, "pk_unique.json", _extract_pk_unique(conn, schemas))
        _write(staging_dir, "foreign_keys.json", _extract_foreign_keys(conn, schemas))
        _write(staging_dir, "identity_columns.json", _extract_identity_columns(conn, schemas))
        object_types_rows, mv_fqns = _extract_object_types(conn, schemas)
        _write(staging_dir, "object_types.json", object_types_rows)
        if mv_fqns:
            _write(staging_dir, "mv_fqns.json", mv_fqns)
        _write(staging_dir, "proc_dmf.json", _extract_dmf(conn, schemas, "PROCEDURE"))
        _write(staging_dir, "view_dmf.json", _extract_dmf(conn, schemas, "VIEW"))
        _write(staging_dir, "func_dmf.json", _extract_dmf(conn, schemas, "FUNCTION"))
        _write(staging_dir, "proc_params.json", _extract_proc_params(conn, schemas))
        _write(staging_dir, "packages.json", _extract_packages(conn, schemas))
        # Oracle does not support these signals — write empty lists for pipeline compatibility
        _write(staging_dir, "cdc.json", [])
        _write(staging_dir, "change_tracking.json", [])
        _write(staging_dir, "sensitivity.json", [])
    finally:
        conn.close()
