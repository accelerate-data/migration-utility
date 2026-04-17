"""Oracle sandbox SQL comparison service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import sqlglot

from shared.sandbox.base import (
    build_compare_error,
    build_compare_result,
    capture_rows as _capture_rows_base,
)
from shared.sandbox.oracle_services import (
    _import_oracledb,
    _validate_fixtures,
    _validate_oracle_sandbox_name,
    _validate_readonly_sql,
)

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox

logger = logging.getLogger(__name__)


class OracleComparisonService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def compare_two_sql(
        self,
        sandbox_db: str,
        sql_a: str,
        sql_b: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[str, Any]:
        _validate_oracle_sandbox_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql_a)
        _validate_readonly_sql(sql_b)

        logger.info("event=oracle_compare_two_sql sandbox=%s", sandbox_db)

        for label, sql in (("A", sql_a), ("B", sql_b)):
            try:
                sqlglot.parse_one(sql, dialect="oracle")
            except sqlglot.errors.ParseError as exc:
                logger.error(
                    "event=oracle_sql_syntax_error sandbox=%s label=%s error=%s",
                    sandbox_db,
                    label,
                    exc,
                )
                return build_compare_error(
                    "SQL_SYNTAX_ERROR",
                    f"SQL {label} has syntax errors: {exc}",
        )

        try:
            self._backend._fixtures.ensure_view_tables(sandbox_db, fixtures)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=view_materialize_failed sandbox=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_compare_error("VIEW_MATERIALIZE_FAILED", str(exc))

        try:
            rows_a: list[dict[str, Any]] = []
            rows_b: list[dict[str, Any]] = []
            with self._backend._connect_sandbox(sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._backend._fixtures.seed_fixtures(cursor, fixtures)
                    cursor.execute(sql_a)
                    rows_a = _capture_rows_base(cursor)
                    cursor.execute(sql_b)
                    rows_b = _capture_rows_base(cursor)
                finally:
                    conn.rollback()
            result = build_compare_result(rows_a, rows_b)
            logger.info(
                "event=oracle_compare_two_sql_complete sandbox=%s equivalent=%s "
                "a_count=%d b_count=%d",
                sandbox_db,
                result["equivalent"],
                result["a_count"],
                result["b_count"],
            )
            return result
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_compare_two_sql_failed sandbox=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_compare_error("COMPARE_SQL_FAILED", str(exc))
