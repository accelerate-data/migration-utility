"""Oracle sandbox fixture and view materialization service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.sandbox.oracle_services import (
    _get_oracle_not_null_defaults,
    _import_oracledb,
    _validate_oracle_identifier,
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
        sandbox_schema: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        """Seed fixture rows into sandbox tables."""
        for fixture in fixtures:
            table_name = fixture["table"]

            rows = fixture.get("rows", [])
            if not rows:
                continue

            fixture_columns = set(rows[0].keys())
            not_null_defaults = _get_oracle_not_null_defaults(
                cursor,
                sandbox_schema,
                table_name,
            )
            fill_cols = {
                col: default
                for col, default in not_null_defaults.items()
                if col not in fixture_columns
            }
            if fill_cols:
                logger.info(
                    "event=oracle_auto_fill_not_null sandbox=%s table=%s columns=%s",
                    sandbox_schema,
                    table_name,
                    sorted(fill_cols.keys()),
                )

            columns = list(fixture_columns | fill_cols.keys())
            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
            insert_sql = (
                f'INSERT INTO "{sandbox_schema}"."{table_name}" '
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
        """CTAS view-sourced fixtures as empty shell tables in the sandbox."""
        materialized: list[str] = []
        with self._backend._connect_source() as source_conn, \
             self._backend._connect_sandbox(sandbox_db) as sandbox_conn:
            source_cursor = source_conn.cursor()
            sandbox_cursor = sandbox_conn.cursor()
            for fixture in given:
                view_name = fixture["table"]
                _validate_oracle_identifier(view_name)
                source_cursor.execute(
                    "SELECT 1 FROM ALL_VIEWS "
                    "WHERE OWNER = UPPER(:1) AND VIEW_NAME = UPPER(:2)",
                    [self._backend.source_schema, view_name],
                )
                if source_cursor.fetchone() is None:
                    continue
                try:
                    sandbox_cursor.execute(f'DROP TABLE "{sandbox_db}"."{view_name}"')
                except _import_oracledb().DatabaseError as exc:
                    logger.debug(
                        "event=oracle_view_table_drop_skipped sandbox=%s view=%s error=%s",
                        sandbox_db,
                        view_name,
                        exc,
                    )
                try:
                    sandbox_cursor.execute(f'DROP VIEW "{sandbox_db}"."{view_name}"')
                except _import_oracledb().DatabaseError as exc:
                    logger.debug(
                        "event=oracle_view_drop_skipped sandbox=%s view=%s error=%s",
                        sandbox_db,
                        view_name,
                        exc,
                    )
                columns = self._backend._load_object_columns(
                    source_cursor,
                    self._backend.source_schema,
                    view_name,
                )
                self._backend._create_empty_table(
                    sandbox_cursor,
                    sandbox_db,
                    view_name,
                    columns,
                )
                materialized.append(view_name)
                logger.info(
                    "event=oracle_view_materialized sandbox=%s view=%s",
                    sandbox_db,
                    view_name,
                )
        return materialized
