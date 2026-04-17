"""SQL Server sandbox fixture and view materialization service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.sandbox.sql_server_services import (
    _get_identity_columns,
    _get_not_null_defaults,
    _import_pyodbc,
    _quote_identifier,
    _split_identifier_parts,
)

if TYPE_CHECKING:
    from shared.sandbox.sql_server import SqlServerSandbox

logger = logging.getLogger(__name__)


class SqlServerFixtureService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def seed_fixtures(
        self,
        cursor: Any,
        sandbox_db: str,
        fixtures: list[dict[str, Any]],
    ) -> None:
        """Seed fixture rows into sandbox tables within an existing transaction."""
        cursor.execute(
            "SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name) "
            "FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE t.is_ms_shipped = 0"
        )
        all_tables = [row[0] for row in cursor.fetchall()]
        for tbl in all_tables:
            cursor.execute(f"DISABLE TRIGGER ALL ON {tbl}")
        if all_tables:
            logger.info(
                "event=triggers_disabled sandbox_db=%s count=%d",
                sandbox_db,
                len(all_tables),
            )

        fk_disabled_tables: list[str] = []
        for fixture in fixtures:
            table = fixture["table"]
            quoted_table = _quote_identifier(table)
            if fixture.get("rows"):
                cursor.execute(f"ALTER TABLE {quoted_table} NOCHECK CONSTRAINT ALL")
                fk_disabled_tables.append(table)
        if fk_disabled_tables:
            logger.info(
                "event=fk_constraints_disabled sandbox_db=%s tables=%s",
                sandbox_db,
                fk_disabled_tables,
            )

        for fixture in fixtures:
            table = fixture["table"]
            quoted_table = _quote_identifier(table)
            rows = fixture.get("rows", [])
            if not rows:
                continue
            fixture_columns = set(rows[0].keys())

            not_null_defaults = _get_not_null_defaults(cursor, table)
            fill_cols = {
                col: default
                for col, default in not_null_defaults.items()
                if col not in fixture_columns
            }
            if fill_cols:
                logger.info(
                    "event=auto_fill_not_null sandbox_db=%s table=%s columns=%s",
                    sandbox_db,
                    table,
                    sorted(fill_cols.keys()),
                )

            columns = list(fixture_columns | fill_cols.keys())
            identity_cols = _get_identity_columns(cursor, table)
            needs_identity_insert = bool(identity_cols & fixture_columns)

            if needs_identity_insert:
                cursor.execute(f"SET IDENTITY_INSERT {quoted_table} ON")
                logger.info(
                    "event=identity_insert_enabled sandbox_db=%s table=%s columns=%s",
                    sandbox_db,
                    table,
                    sorted(identity_cols & fixture_columns),
                )

            col_list = ", ".join(f"[{c}]" for c in columns)
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
            value_lists = [
                [row.get(c, fill_cols.get(c)) for c in columns]
                for row in rows
            ]
            cursor.executemany(insert_sql, value_lists)

            if needs_identity_insert:
                cursor.execute(f"SET IDENTITY_INSERT {quoted_table} OFF")

        for table in fk_disabled_tables:
            cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")

    def ensure_view_tables(
        self,
        sandbox_db: str,
        given: list[dict[str, Any]],
    ) -> list[str]:
        """CTAS view-sourced fixtures as empty shell tables in the sandbox."""
        materialized: list[str] = []
        with self._backend._connect_source(database=self._backend.source_database) as src_conn, \
             self._backend._connect(database=sandbox_db) as sb_conn:
            src_cur = src_conn.cursor()
            sb_cur = sb_conn.cursor()
            for fixture in given:
                parts = _split_identifier_parts(fixture["table"])
                if len(parts) != 2:
                    logger.warning(
                        "event=view_check_skipped reason=unexpected_parts fqn=%r parts=%d",
                        fixture["table"],
                        len(parts),
                    )
                    continue
                schema_name, obj_name = parts
                src_cur.execute(
                    "SELECT 1 FROM INFORMATION_SCHEMA.VIEWS "
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                    schema_name,
                    obj_name,
                )
                if src_cur.fetchone() is None:
                    continue
                fqn = f"[{schema_name}].[{obj_name}]"
                try:
                    sb_cur.execute(f"DROP TABLE IF EXISTS {fqn}")
                except _import_pyodbc().Error:
                    pass
                try:
                    sb_cur.execute(f"DROP VIEW IF EXISTS {fqn}")
                except _import_pyodbc().Error:
                    pass
                columns = self._backend._load_object_columns(src_cur, schema_name, obj_name)
                self._backend._create_empty_table(sb_cur, schema_name, obj_name, columns)
                materialized.append(f"{schema_name}.{obj_name}")
                logger.info(
                    "event=view_materialized sandbox_db=%s fqn=%s",
                    sandbox_db,
                    fqn,
                )
        return materialized
