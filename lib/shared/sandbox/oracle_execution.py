"""Oracle sandbox execution service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.output_models.sandbox import TestHarnessExecuteOutput
from shared.sandbox.base import (
    build_execute_error,
    build_execute_output,
    capture_rows as _capture_rows_base,
)
from shared.sandbox.oracle_services import (
    _import_oracledb,
    _validate_fixtures,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
    _validate_readonly_sql,
)

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox

logger = logging.getLogger(__name__)


class OracleExecutionService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        _validate_oracle_sandbox_name(sandbox_db)

        scenario_name = scenario.get("name", "unnamed")
        for key in ("target_table", "procedure", "given"):
            if key not in scenario:
                raise KeyError(f"Scenario missing required key: {key!r}")
        target_table = scenario["target_table"]
        procedure = scenario["procedure"]
        given = scenario["given"]

        _validate_oracle_identifier(target_table)
        _validate_oracle_identifier(procedure)
        _validate_fixtures(given)

        try:
            self._backend._ensure_view_tables(sandbox_db, given)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_view_materialize_failed sandbox=%s scenario=%s error=%s",
                sandbox_db,
                scenario_name,
                exc,
            )
            return build_execute_error(scenario_name, "VIEW_MATERIALIZE_FAILED", str(exc))

        logger.info(
            "event=oracle_execute_scenario sandbox=%s scenario=%s procedure=%s",
            sandbox_db,
            scenario_name,
            procedure,
        )

        result_rows: list[dict[str, Any]] = []
        try:
            with self._backend._connect() as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._backend._seed_fixtures(cursor, sandbox_db, given)
                    cursor.execute(f'BEGIN "{sandbox_db}"."{procedure}"; END;')
                    cursor.execute(f'SELECT * FROM "{sandbox_db}".{target_table}')
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=oracle_scenario_complete sandbox=%s scenario=%s rows=%d",
                sandbox_db,
                scenario_name,
                len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_scenario_failed sandbox=%s scenario=%s error=%s",
                sandbox_db,
                scenario_name,
                exc,
            )
            return build_execute_error(scenario_name, "SCENARIO_FAILED", str(exc))

    def execute_select(
        self,
        sandbox_db: str,
        sql: str,
        fixtures: list[dict[str, Any]],
    ) -> TestHarnessExecuteOutput:
        _validate_oracle_sandbox_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql)

        scenario_name = "execute_select"
        logger.info("event=oracle_execute_select sandbox=%s", sandbox_db)

        try:
            self._backend._ensure_view_tables(sandbox_db, fixtures)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_view_materialize_failed sandbox=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_execute_error(scenario_name, "VIEW_MATERIALIZE_FAILED", str(exc))

        result_rows: list[dict[str, Any]] = []
        try:
            with self._backend._connect() as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._backend._seed_fixtures(cursor, sandbox_db, fixtures)
                    cursor.execute(sql)
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=oracle_execute_select_complete sandbox=%s rows=%d",
                sandbox_db,
                len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_execute_select_failed sandbox=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_execute_error(scenario_name, "EXECUTE_SELECT_FAILED", str(exc))
