from __future__ import annotations

import logging
from pathlib import Path

from shared.db_connect import oracle_connect as _oracle_connect
from shared.oracle_extract_ddl import (
    extract_definition_rows as _extract_definitions,
    extract_view_ddl_rows as _extract_view_ddl,
    oracle_type_to_class_desc as _oracle_type_to_class_desc,
)
from shared.oracle_extract_services import (
    extract_dmf as _extract_dmf,
    extract_foreign_keys as _extract_foreign_keys,
    extract_identity_columns as _extract_identity_columns,
    extract_object_types as _extract_object_types,
    extract_packages as _extract_packages,
    extract_pk_unique as _extract_pk_unique,
    extract_proc_params as _extract_proc_params,
    extract_table_columns as _extract_table_columns,
    extract_view_columns as _extract_view_columns,
    oracle_column_length as _oracle_column_length,
    write_oracle_staging_json as _write,
)

logger = logging.getLogger(__name__)


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
        _write(staging_dir, "view_columns.json", _extract_view_columns(conn, schemas))
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


__all__ = [
    "run_oracle_extraction",
    "_oracle_type_to_class_desc",
    "_extract_definitions",
    "_extract_view_ddl",
    "_extract_table_columns",
    "_oracle_column_length",
    "_extract_pk_unique",
    "_extract_foreign_keys",
    "_extract_identity_columns",
    "_extract_object_types",
    "_extract_view_columns",
    "_extract_dmf",
    "_extract_proc_params",
    "_extract_packages",
]
