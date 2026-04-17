"""Oracle sandbox fixture and view materialization service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.sandbox.oracle_services import (
    _get_oracle_not_null_defaults,
    _import_oracledb,
    _parse_qualified_name,
    _validate_oracle_qualified_name,
)

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox

logger = logging.getLogger(__name__)


class OracleFixtureService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def seed_fixtures(
        self,
        cursor: Any,
        fixtures: list[dict[str, Any]],
    ) -> None:
        """Seed fixture rows into sandbox tables.

        ``table`` in each fixture is already schema-qualified by the caller
        (e.g. ``MIGRATIONTEST.SILVER_DIMCURRENCY``).  The INSERT uses the
        name as-is — the sandbox PDB is just a connection target.
        """
        for fixture in fixtures:
            table_name = fixture["table"]

            rows = fixture.get("rows", [])
            if not rows:
                continue

            fixture_columns = set(rows[0].keys())
            not_null_defaults = _get_oracle_not_null_defaults(
                cursor,
                table_name,
            )
            fill_cols = {
                col: default
                for col, default in not_null_defaults.items()
                if col not in fixture_columns
            }
            schema, bare_table = _parse_qualified_name(table_name)
            if fill_cols:
                logger.info(
                    "event=oracle_auto_fill_not_null schema=%s table=%s columns=%s",
                    schema,
                    bare_table,
                    sorted(fill_cols.keys()),
                )

            columns = list(fixture_columns | fill_cols.keys())
            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
            insert_sql = (
                f'INSERT INTO "{schema}"."{bare_table}" '
                f"({col_list}) VALUES ({placeholders})"
            )
            value_lists = [
                [row.get(c, fill_cols.get(c)) for c in columns]
                for row in rows
            ]
            cursor.executemany(insert_sql, value_lists)

    def ensure_view_tables(
        self,
        sandbox_db: str,
        given: list[dict[str, Any]],
    ) -> list[str]:
        """CTAS view-sourced fixtures as empty shell tables in the sandbox.

        Fixture table names are schema-qualified (e.g.
        ``MIGRATIONTEST.VW_PRODUCT``).  The schema and object name are parsed
        to query ``ALL_VIEWS`` in the source and to issue DDL in the sandbox.
        """
        materialized: list[str] = []
        with self._backend._connect_source() as source_conn, \
             self._backend._connect_sandbox(sandbox_db) as sandbox_conn:
            source_cursor = source_conn.cursor()
            sandbox_cursor = sandbox_conn.cursor()
            for fixture in given:
                qualified_name = fixture["table"]
                _validate_oracle_qualified_name(qualified_name)
                schema, obj_name = _parse_qualified_name(qualified_name)
                source_cursor.execute(
                    "SELECT 1 FROM ALL_VIEWS "
                    "WHERE OWNER = UPPER(:1) AND VIEW_NAME = UPPER(:2)",
                    [schema, obj_name],
                )
                if source_cursor.fetchone() is None:
                    continue
                try:
                    sandbox_cursor.execute(f'DROP TABLE "{schema}"."{obj_name}"')
                except _import_oracledb().DatabaseError as exc:
                    logger.debug(
                        "event=oracle_view_table_drop_skipped sandbox=%s view=%s error=%s",
                        sandbox_db,
                        qualified_name,
                        exc,
                    )
                try:
                    sandbox_cursor.execute(f'DROP VIEW "{schema}"."{obj_name}"')
                except _import_oracledb().DatabaseError as exc:
                    logger.debug(
                        "event=oracle_view_drop_skipped sandbox=%s view=%s error=%s",
                        sandbox_db,
                        qualified_name,
                        exc,
                    )
                columns = self._backend._load_object_columns(
                    source_cursor,
                    schema,
                    obj_name,
                )
                self._backend._create_empty_table(
                    sandbox_cursor,
                    schema,
                    obj_name,
                    columns,
                )
                materialized.append(qualified_name)
                logger.info(
                    "event=oracle_view_materialized sandbox=%s view=%s",
                    sandbox_db,
                    qualified_name,
                )
        return materialized
