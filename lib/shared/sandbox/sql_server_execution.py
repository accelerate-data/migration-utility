"""SQL Server sandbox execution service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.output_models.sandbox import ErrorEntry, TestHarnessExecuteOutput
from shared.sandbox.base import (
    build_execute_error,
    build_execute_output,
    capture_rows as _capture_rows_base,
)
from shared.sandbox.sql_server_services import (
    _detect_remote_exec_target,
    _import_pyodbc,
    _validate_fixtures,
    _validate_identifier,
    _validate_readonly_sql,
    _validate_sandbox_db_name,
)

if TYPE_CHECKING:
    from shared.sandbox.sql_server import SqlServerSandbox

logger = logging.getLogger(__name__)


class SqlServerExecutionService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def execute_scenario(
        self,
        sandbox_db: str,
        scenario: dict[str, Any],
    ) -> TestHarnessExecuteOutput:
        _validate_sandbox_db_name(sandbox_db)

        scenario_name = scenario.get("name", "unnamed")
        for key in ("target_table", "procedure", "given"):
            if key not in scenario:
                raise KeyError(f"Scenario missing required key: {key!r}")
        target_table = scenario["target_table"]
        procedure = scenario["procedure"]
        given = scenario["given"]

        _validate_identifier(target_table)
        _validate_identifier(procedure)
        _validate_fixtures(given)

        try:
            self._backend._fixtures.ensure_view_tables(sandbox_db, given)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=view_materialize_failed sandbox_db=%s scenario=%s error=%s",
                sandbox_db,
                scenario_name,
                exc,
            )
            return TestHarnessExecuteOutput(
                scenario_name=scenario_name,
                status="error",
                ground_truth_rows=[],
                row_count=0,
                errors=[ErrorEntry(code="VIEW_MATERIALIZE_FAILED", message=str(exc))],
            )

        logger.info(
            "event=execute_scenario sandbox_db=%s scenario=%s procedure=%s",
            sandbox_db,
            scenario_name,
            procedure,
        )

        result_rows: list[dict[str, Any]] = []
        try:
            with self._backend._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    cursor.execute("SELECT OBJECT_DEFINITION(OBJECT_ID(?))", procedure)
                    proc_definition_rows = cursor.fetchall()
                    proc_definition = proc_definition_rows[0][0] if proc_definition_rows else None
                    remote_exec = (
                        _detect_remote_exec_target(proc_definition)
                        if proc_definition else None
                    )
                    if remote_exec:
                        target = remote_exec["target"]
                        kind = remote_exec["kind"]
                        logger.error(
                            "event=scenario_remote_exec_unsupported sandbox_db=%s scenario=%s "
                            "procedure=%s target=%s kind=%s",
                            sandbox_db,
                            scenario_name,
                            procedure,
                            target,
                            kind,
                        )
                        return TestHarnessExecuteOutput(
                            scenario_name=scenario_name,
                            status="error",
                            ground_truth_rows=[],
                            row_count=0,
                            errors=[ErrorEntry(
                                code="REMOTE_EXEC_UNSUPPORTED",
                                message=(
                                    f"Sandbox cannot execute {kind} procedure call "
                                    f"{target} from {procedure}. The sandbox only clones "
                                    "objects from the source database."
                                ),
                            )],
                        )

                    self._backend._fixtures.seed_fixtures(cursor, sandbox_db, given)

                    cursor.execute(f"EXEC {procedure}")

                    cursor.execute(f"SELECT * FROM {target_table}")
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=scenario_complete sandbox_db=%s scenario=%s rows=%d",
                sandbox_db,
                scenario_name,
                len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)

        except _import_pyodbc().Error as exc:
            logger.error(
                "event=scenario_failed sandbox_db=%s scenario=%s error=%s",
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
        _validate_sandbox_db_name(sandbox_db)
        _validate_fixtures(fixtures)
        _validate_readonly_sql(sql)

        scenario_name = "execute_select"
        logger.info("event=execute_select sandbox_db=%s", sandbox_db)

        try:
            self._backend._fixtures.ensure_view_tables(sandbox_db, fixtures)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=view_materialize_failed sandbox_db=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_execute_error(scenario_name, "VIEW_MATERIALIZE_FAILED", str(exc))

        result_rows: list[dict[str, Any]] = []
        try:
            with self._backend._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()
                try:
                    self._backend._fixtures.seed_fixtures(cursor, sandbox_db, fixtures)
                    cursor.execute(sql)
                    result_rows = _capture_rows_base(cursor)
                finally:
                    conn.rollback()

            logger.info(
                "event=execute_select_complete sandbox_db=%s rows=%d",
                sandbox_db,
                len(result_rows),
            )
            return build_execute_output(scenario_name, result_rows)
        except _import_pyodbc().Error as exc:
            logger.error(
                "event=execute_select_failed sandbox_db=%s error=%s",
                sandbox_db,
                exc,
            )
            return build_execute_error(scenario_name, "EXECUTE_SELECT_FAILED", str(exc))
