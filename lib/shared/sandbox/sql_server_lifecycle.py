"""SQL Server sandbox lifecycle service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
)
from shared.sandbox.base import generate_sandbox_name
from shared.sandbox.sql_server_services import (
    _import_pyodbc,
    _validate_identifier,
    _validate_sandbox_db_name,
)

if TYPE_CHECKING:
    from shared.sandbox.sql_server import SqlServerSandbox

logger = logging.getLogger(__name__)


class SqlServerLifecycleService:
    def __init__(self, backend: SqlServerSandbox) -> None:
        self._backend = backend

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        sandbox_db = generate_sandbox_name()
        logger.info(
            "event=sandbox_up sandbox_db=%s source=%s schemas=%s",
            sandbox_db,
            self._backend.source_database,
            schemas,
        )
        result = self._sandbox_clone_into(sandbox_db, schemas)
        logger.info(
            "event=sandbox_up_complete sandbox_db=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_db,
            result.status,
            len(result.tables_cloned),
            len(result.views_cloned),
            len(result.procedures_cloned),
            len(result.errors),
        )
        return result

    def sandbox_reset(self, sandbox_db: str, schemas: list[str]) -> SandboxUpOutput:
        _validate_sandbox_db_name(sandbox_db)
        logger.info(
            "event=sandbox_reset sandbox_db=%s source=%s schemas=%s",
            sandbox_db,
            self._backend.source_database,
            schemas,
        )
        down_result = self.sandbox_down(sandbox_db)
        if down_result.status == "error":
            return SandboxUpOutput(
                sandbox_database=sandbox_db,
                status="error",
                tables_cloned=[],
                views_cloned=[],
                procedures_cloned=[],
                errors=[
                    ErrorEntry(
                        code="SANDBOX_RESET_FAILED",
                        message="Failed to drop existing sandbox before reset.",
                    ),
                    *down_result.errors,
                ],
            )

        result = self._sandbox_clone_into(sandbox_db, schemas)
        logger.info(
            "event=sandbox_reset_complete sandbox_db=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_db,
            result.status,
            len(result.tables_cloned),
            len(result.views_cloned),
            len(result.procedures_cloned),
            len(result.errors),
        )
        return result

    def sandbox_down(self, sandbox_db: str) -> SandboxDownOutput:
        _validate_sandbox_db_name(sandbox_db)
        logger.info("event=sandbox_down sandbox_db=%s", sandbox_db)

        try:
            quoted = f"[{sandbox_db}]"
            with self._backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_ID(?)", sandbox_db)
                exists = cursor.fetchone()[0] is not None
                if exists:
                    cursor.execute(
                        f"ALTER DATABASE {quoted} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                    )
                    cursor.execute(f"DROP DATABASE {quoted}")
            logger.info("event=sandbox_down_complete sandbox_db=%s", sandbox_db)
            return SandboxDownOutput(sandbox_database=sandbox_db, status="ok")
        except _import_pyodbc().Error as exc:
            logger.error("event=sandbox_down_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return SandboxDownOutput(
                sandbox_database=sandbox_db,
                status="error",
                errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message=str(exc))],
            )

    def sandbox_status(
        self,
        sandbox_db: str,
        schemas: list[str] | None = None,
    ) -> SandboxStatusOutput:
        _validate_sandbox_db_name(sandbox_db)
        logger.info("event=sandbox_status sandbox_db=%s", sandbox_db)

        try:
            with self._backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DB_ID(?)", sandbox_db)
                exists = cursor.fetchone()[0] is not None

            if exists:
                tables_count, views_count, procedures_count = self._sandbox_content_counts(
                    sandbox_db,
                    schemas,
                )
                has_content = any(
                    count > 0 for count in (tables_count, views_count, procedures_count)
                )
                logger.info(
                    "event=sandbox_status_complete sandbox_db=%s exists=true has_content=%s "
                    "tables=%d views=%d procedures=%d",
                    sandbox_db,
                    has_content,
                    tables_count,
                    views_count,
                    procedures_count,
                )
                return SandboxStatusOutput(
                    sandbox_database=sandbox_db,
                    status="ok",
                    exists=True,
                    has_content=has_content,
                    tables_count=tables_count,
                    views_count=views_count,
                    procedures_count=procedures_count,
                )
            logger.info("event=sandbox_status_complete sandbox_db=%s exists=false", sandbox_db)
            return SandboxStatusOutput(
                sandbox_database=sandbox_db,
                status="not_found",
                exists=False,
                has_content=False,
                tables_count=0,
                views_count=0,
                procedures_count=0,
            )
        except _import_pyodbc().Error as exc:
            logger.error("event=sandbox_status_failed sandbox_db=%s error=%s", sandbox_db, exc)
            return SandboxStatusOutput(
                sandbox_database=sandbox_db,
                status="error",
                exists=False,
                errors=[ErrorEntry(code="SANDBOX_STATUS_FAILED", message=str(exc))],
            )

    def _sandbox_clone_into(
        self,
        sandbox_db: str,
        schemas: list[str],
    ) -> SandboxUpOutput:
        _validate_identifier(self._backend.source_database)
        _validate_sandbox_db_name(sandbox_db)

        errors: list[ErrorEntry] = []
        tables_cloned: list[str] = []
        views_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            self._backend._create_sandbox_db(sandbox_db)
            with self._backend._connect(database=sandbox_db) as sandbox_conn, \
                 self._backend._connect_source(database=self._backend.source_database) as source_conn:
                sandbox_cursor = sandbox_conn.cursor()
                source_cursor = source_conn.cursor()

                errors.extend(
                    ErrorEntry(**e)
                    for e in self._backend._create_schemas(sandbox_cursor, schemas)
                )

                t_cloned, t_errors = self._backend._clone_tables(
                    source_cursor, sandbox_cursor, schemas,
                )
                tables_cloned.extend(t_cloned)
                errors.extend(ErrorEntry(**e) for e in t_errors)

                v_cloned, v_errors = self._backend._clone_views(
                    source_cursor, sandbox_cursor, schemas,
                )
                views_cloned.extend(v_cloned)
                errors.extend(ErrorEntry(**e) for e in v_errors)

                p_cloned, p_errors = self._backend._clone_procedures(
                    source_cursor, sandbox_cursor, schemas,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(ErrorEntry(**e) for e in p_errors)

        except _import_pyodbc().Error as exc:
            logger.error(
                "event=sandbox_up_failed sandbox_db=%s error=%s",
                sandbox_db,
                exc,
            )
            self._backend.sandbox_down(sandbox_db)
            return SandboxUpOutput(
                sandbox_database=sandbox_db,
                status="error",
                tables_cloned=tables_cloned,
                views_cloned=views_cloned,
                procedures_cloned=procedures_cloned,
                errors=[ErrorEntry(code="SANDBOX_UP_FAILED", message=str(exc))],
            )

        status = "ok" if not errors else "partial"
        return SandboxUpOutput(
            sandbox_database=sandbox_db,
            status=status,
            tables_cloned=tables_cloned,
            views_cloned=views_cloned,
            procedures_cloned=procedures_cloned,
            errors=errors,
        )

    def _sandbox_content_counts(
        self,
        sandbox_db: str,
        schemas: list[str] | None,
    ) -> tuple[int, int, int]:
        schema_filter = ""
        params: list[str] = []
        if schemas:
            placeholders = ", ".join("?" for _ in schemas)
            schema_filter = f" AND s.name IN ({placeholders})"
            params = list(schemas)

        queries = [
            (
                "SELECT COUNT(*) FROM sys.tables t "
                "JOIN sys.schemas s ON t.schema_id = s.schema_id "
                f"WHERE t.is_ms_shipped = 0{schema_filter}"
            ),
            (
                "SELECT COUNT(*) FROM sys.views v "
                "JOIN sys.schemas s ON v.schema_id = s.schema_id "
                f"WHERE v.is_ms_shipped = 0{schema_filter}"
            ),
            (
                "SELECT COUNT(*) FROM sys.procedures p "
                "JOIN sys.schemas s ON p.schema_id = s.schema_id "
                f"WHERE p.is_ms_shipped = 0{schema_filter}"
            ),
        ]

        counts: list[int] = []
        with self._backend._connect(database=sandbox_db) as conn:
            cursor = conn.cursor()
            for query in queries:
                cursor.execute(query, *params)
                counts.append(int(cursor.fetchone()[0]))
        return counts[0], counts[1], counts[2]
