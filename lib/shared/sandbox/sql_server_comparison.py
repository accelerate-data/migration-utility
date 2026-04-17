"""SQL Server sandbox SQL comparison service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.sandbox.base import (
    build_compare_error,
    build_compare_result,
    capture_rows as _capture_rows_base,
)
from shared.sandbox.sql_server_services import (
    _import_pyodbc,
    _validate_fixtures,
    _validate_readonly_sql,
    _validate_sandbox_db_name,
)

if TYPE_CHECKING:
    from shared.sandbox.sql_server import SqlServerSandbox

logger = logging.getLogger(__name__)


class SqlServerComparisonService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        _validate_sandbox_db_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql_a)
        _validate_readonly_sql(sql_b)

        logger.info(
            "event=compare_two_sql sandbox_db=%s",
            sandbox_db,
        )

        try:
            self._backend._fixtures.ensure_view_tables(sandbox_db, fixtures)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=view_materialize_failed sandbox_db=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_compare_error("VIEW_MATERIALIZE_FAILED", str(exc))

        try:
            rows_a: list[dict[str, Any]] = []
            rows_b: list[dict[str, Any]] = []
            with self._backend._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    for label, sql in [("A", sql_a), ("B", sql_b)]:
                        try:
                            cursor.execute("SET PARSEONLY ON")
                            cursor.execute(sql)
                            cursor.execute("SET PARSEONLY OFF")
                        except _import_pyodbc().Error as parse_exc:
                            cursor.execute("SET PARSEONLY OFF")
                            conn.rollback()
                            logger.error(
                                "event=sql_syntax_error sandbox_db=%s label=%s error=%s",
                                sandbox_db,
                                label,
                                parse_exc,
                            )
                            return build_compare_error(
                                "SQL_SYNTAX_ERROR",
                                f"SQL {label} has syntax errors: {parse_exc}",
                            )

                    self._backend._fixtures.seed_fixtures(cursor, sandbox_db, fixtures)

                    cursor.execute(sql_a)
                    rows_a = _capture_rows_base(cursor)

                    cursor.execute(sql_b)
                    rows_b = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            result = build_compare_result(rows_a, rows_b)
            logger.info(
                "event=compare_two_sql_complete sandbox_db=%s equivalent=%s "
                "a_count=%d b_count=%d",
                sandbox_db,
                result["equivalent"],
                result["a_count"],
                result["b_count"],
            )
            return result

        except _import_pyodbc().Error as exc:
            logger.error(
                "event=compare_two_sql_failed sandbox_db=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_compare_error("COMPARE_SQL_FAILED", str(exc))
