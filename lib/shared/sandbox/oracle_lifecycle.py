"""Oracle sandbox lifecycle service."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from shared.output_models.sandbox import (
    ErrorEntry,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
)
from shared.sandbox.base import generate_sandbox_name
from shared.sandbox.oracle_services import (
    _import_oracledb,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
)

if TYPE_CHECKING:
    from shared.sandbox.oracle import OracleSandbox

logger = logging.getLogger(__name__)


class OracleLifecycleService:
    def __init__(self, backend: OracleSandbox) -> None:
        self._backend = backend

    def sandbox_up(self, schemas: list[str]) -> SandboxUpOutput:
        source_schema = schemas[0] if schemas else self._backend.source_schema
        sandbox_schema = generate_sandbox_name()
        logger.info(
            "event=oracle_sandbox_up sandbox=%s source_schema=%s",
            sandbox_schema,
            source_schema,
        )
        result = self._sandbox_clone_into(sandbox_schema, source_schema)
        logger.info(
            "event=oracle_sandbox_up_complete sandbox=%s status=%s "
            "tables=%d views=%d procedures=%d errors=%d",
            sandbox_schema,
            result.status,
            len(result.tables_cloned),
            len(result.views_cloned),
            len(result.procedures_cloned),
            len(result.errors),
        )
        return result

    def sandbox_reset(self, sandbox_db: str, schemas: list[str]) -> SandboxUpOutput:
        _validate_oracle_sandbox_name(sandbox_db)
        source_schema = schemas[0] if schemas else self._backend.source_schema
        _validate_oracle_identifier(source_schema)
        logger.info(
            "event=oracle_sandbox_reset sandbox=%s schemas=%s",
            sandbox_db,
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

        result = self._sandbox_clone_into(sandbox_db, source_schema)
        logger.info(
            "event=oracle_sandbox_reset_complete sandbox=%s status=%s "
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
        _validate_oracle_sandbox_name(sandbox_db)
        logger.info("event=oracle_sandbox_down sandbox=%s", sandbox_db)

        try:
            with self._backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
                    [sandbox_db],
                )
                if cursor.fetchone()[0] > 0:
                    cursor.execute(f'DROP USER "{sandbox_db}" CASCADE')
            logger.info("event=oracle_sandbox_down_complete sandbox=%s", sandbox_db)
            return SandboxDownOutput(sandbox_database=sandbox_db, status="ok")
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_down_failed sandbox=%s error=%s",
                sandbox_db,
                exc,
            )
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
        _validate_oracle_sandbox_name(sandbox_db)
        logger.info("event=oracle_sandbox_status sandbox=%s", sandbox_db)

        try:
            with self._backend._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM ALL_USERS WHERE USERNAME = :1",
                    [sandbox_db],
                )
                exists = cursor.fetchone()[0] > 0
                if exists:
                    tables_count, views_count, procedures_count = self._sandbox_content_counts(
                        cursor,
                        sandbox_db,
                    )

            if exists:
                has_content = any(
                    count > 0 for count in (tables_count, views_count, procedures_count)
                )
                logger.info(
                    "event=oracle_sandbox_status_complete sandbox=%s exists=true "
                    "has_content=%s tables=%d views=%d procedures=%d",
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
            logger.info(
                "event=oracle_sandbox_status_complete sandbox=%s exists=false",
                sandbox_db,
            )
            return SandboxStatusOutput(
                sandbox_database=sandbox_db,
                status="not_found",
                exists=False,
                has_content=False,
                tables_count=0,
                views_count=0,
                procedures_count=0,
            )
        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_status_failed sandbox=%s error=%s",
                sandbox_db,
                exc,
            )
            return SandboxStatusOutput(
                sandbox_database=sandbox_db,
                status="error",
                exists=False,
                errors=[ErrorEntry(code="SANDBOX_STATUS_FAILED", message=str(exc))],
            )

    def _sandbox_clone_into(
        self,
        sandbox_schema: str,
        source_schema: str,
    ) -> SandboxUpOutput:
        _validate_oracle_identifier(source_schema)
        _validate_oracle_sandbox_name(sandbox_schema)

        errors: list[ErrorEntry] = []
        tables_cloned: list[str] = []
        views_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            with self._backend._connect() as sandbox_conn, \
                 self._backend._connect_source() as source_conn:
                sandbox_cursor = sandbox_conn.cursor()
                source_cursor = source_conn.cursor()
                self._backend._create_sandbox_schema(sandbox_cursor, sandbox_schema)

                t_cloned, t_errors = self._backend._clone_tables(
                    source_cursor, sandbox_cursor, sandbox_schema, source_schema,
                )
                tables_cloned.extend(t_cloned)
                errors.extend(ErrorEntry(**e) for e in t_errors)

                v_cloned, v_errors = self._backend._clone_views(
                    source_cursor, sandbox_cursor, sandbox_schema, source_schema,
                )
                views_cloned.extend(v_cloned)
                errors.extend(ErrorEntry(**e) for e in v_errors)

                p_cloned, p_errors = self._backend._clone_procedures(
                    source_cursor, sandbox_cursor, sandbox_schema, source_schema,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(ErrorEntry(**e) for e in p_errors)

        except _import_oracledb().DatabaseError as exc:
            logger.error(
                "event=oracle_sandbox_up_failed sandbox=%s error=%s",
                sandbox_schema,
                exc,
            )
            self._backend.sandbox_down(sandbox_schema)
            return SandboxUpOutput(
                sandbox_database=sandbox_schema,
                status="error",
                tables_cloned=tables_cloned,
                views_cloned=views_cloned,
                procedures_cloned=procedures_cloned,
                errors=[ErrorEntry(code="SANDBOX_UP_FAILED", message=str(exc))],
            )

        status = "ok" if not errors else "partial"
        return SandboxUpOutput(
            sandbox_database=sandbox_schema,
            status=status,
            tables_cloned=tables_cloned,
            views_cloned=views_cloned,
            procedures_cloned=procedures_cloned,
            errors=errors,
        )

    def _sandbox_content_counts(self, cursor: Any, sandbox_db: str) -> tuple[int, int, int]:
        cursor.execute("SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = :1", [sandbox_db])
        tables_count = int(cursor.fetchone()[0])
        cursor.execute("SELECT COUNT(*) FROM ALL_VIEWS WHERE OWNER = :1", [sandbox_db])
        views_count = int(cursor.fetchone()[0])
        cursor.execute(
            "SELECT COUNT(*) FROM ALL_OBJECTS "
            "WHERE OWNER = :1 AND OBJECT_TYPE IN ('PROCEDURE', 'FUNCTION', 'PACKAGE')",
            [sandbox_db],
        )
        procedures_count = int(cursor.fetchone()[0])
        return tables_count, views_count, procedures_count
