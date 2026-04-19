"""SQL Server sandbox database and schema lifecycle helpers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING


def _services():
    from shared.sandbox import sql_server_services

    return sql_server_services

if TYPE_CHECKING:
    import pyodbc

logger = logging.getLogger(__name__)


class SqlServerLifecycleCoreMixin:
    def _create_sandbox_db(self, sandbox_db: str) -> None:
        """Create the sandbox database, dropping it first if it exists."""
        _services()._validate_sandbox_db_name(sandbox_db)
        quoted = f"[{sandbox_db}]"
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DB_ID(?)", sandbox_db)
            rows = cursor.fetchall()
            exists = rows[0][0] is not None
            if exists:
                cursor.execute(
                    f"ALTER DATABASE {quoted} SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
                )
                cursor.execute(f"DROP DATABASE {quoted}")
            cursor.execute(f"CREATE DATABASE {quoted}")
            logger.info("event=database_created sandbox_db=%s", sandbox_db)

    def _create_schemas(
        self, sandbox_cursor: pyodbc.Cursor, schemas: list[str],
    ) -> list[dict[str, str]]:
        """Create schemas in the sandbox. Returns list of error dicts."""
        errors: list[dict[str, str]] = []
        for schema in schemas:
            _services()._validate_identifier(schema)
            try:
                sandbox_cursor.execute(
                    "SELECT COUNT(*) FROM sys.schemas WHERE name = ?",
                    schema,
                )
                schema_exists = sandbox_cursor.fetchall()[0][0] > 0
                if not schema_exists:
                    sandbox_cursor.execute(f"CREATE SCHEMA [{schema}]")
            except _services()._import_pyodbc().Error as exc:
                errors.append({
                    "code": "SCHEMA_CREATE_FAILED",
                    "message": f"Failed to create schema {schema}: {exc}",
                })
        return errors
