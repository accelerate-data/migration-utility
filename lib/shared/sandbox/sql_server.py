"""SQL Server sandbox backend using pyodbc."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pyodbc

from shared.sandbox.base import SandboxBackend

logger = logging.getLogger(__name__)

_RUN_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_ ]*$")


def _validate_run_id(run_id: str) -> None:
    """Validate run_id contains only safe characters."""
    if not _RUN_ID_RE.match(run_id):
        raise ValueError(
            f"Invalid run_id: {run_id!r}. "
            f"Must match [a-zA-Z0-9_-] and be 1-64 characters."
        )


def _validate_identifier(name: str) -> None:
    """Validate a SQL identifier (schema, table, procedure name) is safe.

    Accepts bracket-quoted identifiers like [dbo].[Product] or plain
    identifiers like dbo.Product. Rejects anything containing quotes,
    semicolons, or other injection vectors.
    """
    if not name:
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    # Reject characters that can never appear in a safe identifier
    if re.search(r"[;'\"\-\\]", name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    # Strip matched bracket pairs [name] → name, reject unbalanced brackets
    stripped = re.sub(r"\[([^\[\]]+)\]", r"\1", name)
    if "[" in stripped or "]" in stripped:
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    parts = stripped.split(".")
    for part in parts:
        if not part or not _IDENTIFIER_RE.match(part):
            raise ValueError(f"Unsafe SQL identifier: {name!r}")


class SqlServerSandbox(SandboxBackend):
    """Manage a throwaway SQL Server database for ground-truth capture."""

    def __init__(
        self,
        host: str,
        port: str,
        database: str,
        password: str,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.password = password

    @classmethod
    def from_env(cls, manifest: dict[str, Any]) -> SqlServerSandbox:
        """Create an instance from MSSQL_* env vars and manifest config.

        Raises ValueError if required configuration is missing.
        """
        host = os.environ.get("MSSQL_HOST", "")
        port = os.environ.get("MSSQL_PORT", "1433")
        database = manifest.get("source_database", os.environ.get("MSSQL_DB", ""))
        password = os.environ.get("SA_PASSWORD", "")

        missing = []
        if not host:
            missing.append("MSSQL_HOST")
        if not password:
            missing.append("SA_PASSWORD")
        if not database:
            missing.append("MSSQL_DB (or source_database in manifest)")
        if missing:
            raise ValueError(f"Required environment variables not set: {missing}")

        return cls(host=host, port=port, database=database, password=password)

    @contextmanager
    def _connect(self, *, database: str | None = None) -> Generator[pyodbc.Connection, None, None]:
        db = database or self.database
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={db};"
            f"UID=sa;PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str, autocommit=True)
        try:
            yield conn
        finally:
            conn.close()

    # ── sandbox_up helpers ───────────────────────────────────────────────

    def _create_sandbox_db(
        self, sandbox_db: str, run_id: str,
    ) -> None:
        """Create the sandbox database, dropping it first if it exists."""
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"IF DB_ID(N'{sandbox_db}') IS NOT NULL "
                f"DROP DATABASE [{sandbox_db}]"
            )
            cursor.execute(f"CREATE DATABASE [{sandbox_db}]")
            logger.info("event=database_created run_id=%s db=%s", run_id, sandbox_db)

    def _create_schemas(
        self, sandbox_cursor: pyodbc.Cursor, schemas: list[str],
    ) -> list[dict[str, str]]:
        """Create schemas in the sandbox. Returns list of error dicts."""
        errors: list[dict[str, str]] = []
        for schema in schemas:
            try:
                sandbox_cursor.execute(
                    f"IF NOT EXISTS ("
                    f"  SELECT 1 FROM sys.schemas WHERE name = N'{schema}'"
                    f") EXEC('CREATE SCHEMA [{schema}]')"
                )
            except pyodbc.Error as exc:
                errors.append({
                    "code": "SCHEMA_CREATE_FAILED",
                    "message": f"Failed to create schema {schema}: {exc}",
                })
        return errors

    def _clone_tables(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        source_database: str,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone table structure from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        source_cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ("
            + ",".join(f"N'{s}'" for s in schemas)
            + ") ORDER BY TABLE_SCHEMA, TABLE_NAME"
        )
        for schema_name, table_name in source_cursor.fetchall():
            fqn = f"[{schema_name}].[{table_name}]"
            try:
                sandbox_cursor.execute(
                    f"SELECT TOP 0 * INTO {fqn} "
                    f"FROM [{source_database}].{fqn}"
                )
                cloned.append(f"{schema_name}.{table_name}")
            except pyodbc.Error as exc:
                errors.append({
                    "code": "TABLE_CLONE_FAILED",
                    "message": f"Failed to clone {fqn}: {exc}",
                })
        return cloned, errors

    def _clone_procedures(
        self,
        source_cursor: pyodbc.Cursor,
        sandbox_cursor: pyodbc.Cursor,
        schemas: list[str],
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Clone procedure definitions from source to sandbox. Returns (cloned, errors)."""
        cloned: list[str] = []
        errors: list[dict[str, str]] = []
        source_cursor.execute(
            "SELECT s.name AS schema_name, p.name AS proc_name, "
            "       OBJECT_DEFINITION(p.object_id) AS definition "
            "FROM sys.procedures p "
            "JOIN sys.schemas s ON p.schema_id = s.schema_id "
            "WHERE s.name IN ("
            + ",".join(f"N'{s}'" for s in schemas)
            + ") ORDER BY s.name, p.name"
        )
        for schema_name, proc_name, definition in source_cursor.fetchall():
            fqn = f"{schema_name}.{proc_name}"
            if definition is None:
                errors.append({
                    "code": "PROC_DEFINITION_NULL",
                    "message": f"Cannot read definition for {fqn} (encrypted or inaccessible)",
                })
                continue
            try:
                sandbox_cursor.execute(definition)
                cloned.append(fqn)
            except pyodbc.Error as exc:
                errors.append({
                    "code": "PROC_CLONE_FAILED",
                    "message": f"Failed to clone procedure {fqn}: {exc}",
                })
        return cloned, errors

    # ── Public API ───────────────────────────────────────────────────────

    def sandbox_up(
        self,
        run_id: str,
        schemas: list[str],
        source_database: str,
    ) -> dict[str, Any]:
        _validate_run_id(run_id)
        _validate_identifier(source_database)
        for s in schemas:
            _validate_identifier(s)
        sandbox_db = self.sandbox_db_name(run_id)

        logger.info(
            "event=sandbox_up run_id=%s sandbox_db=%s source=%s schemas=%s",
            run_id, sandbox_db, source_database, schemas,
        )

        self._create_sandbox_db(sandbox_db, run_id)

        errors: list[dict[str, str]] = []
        tables_cloned: list[str] = []
        procedures_cloned: list[str] = []

        try:
            with self._connect(database=sandbox_db) as sandbox_conn, \
                 self._connect(database=source_database) as source_conn:
                sandbox_cursor = sandbox_conn.cursor()
                source_cursor = source_conn.cursor()

                errors.extend(self._create_schemas(sandbox_cursor, schemas))

                t_cloned, t_errors = self._clone_tables(
                    source_cursor, sandbox_cursor, source_database, schemas,
                )
                tables_cloned.extend(t_cloned)
                errors.extend(t_errors)

                p_cloned, p_errors = self._clone_procedures(
                    source_cursor, sandbox_cursor, schemas,
                )
                procedures_cloned.extend(p_cloned)
                errors.extend(p_errors)

        except pyodbc.Error as exc:
            logger.error("event=sandbox_up_failed run_id=%s error=%s", run_id, exc)
            return {
                "run_id": run_id,
                "sandbox_database": sandbox_db,
                "status": "error",
                "tables_cloned": tables_cloned,
                "procedures_cloned": procedures_cloned,
                "errors": [{"code": "SANDBOX_UP_FAILED", "message": str(exc)}],
            }

        status = "ok" if not errors else "partial"
        logger.info(
            "event=sandbox_up_complete run_id=%s status=%s "
            "tables=%d procedures=%d errors=%d",
            run_id, status, len(tables_cloned), len(procedures_cloned), len(errors),
        )
        return {
            "run_id": run_id,
            "sandbox_database": sandbox_db,
            "status": status,
            "tables_cloned": tables_cloned,
            "procedures_cloned": procedures_cloned,
            "errors": errors,
        }

    def sandbox_down(self, run_id: str) -> dict[str, Any]:
        _validate_run_id(run_id)
        sandbox_db = self.sandbox_db_name(run_id)
        logger.info("event=sandbox_down run_id=%s sandbox_db=%s", run_id, sandbox_db)

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"IF DB_ID(N'{sandbox_db}') IS NOT NULL "
                    f"BEGIN "
                    f"  ALTER DATABASE [{sandbox_db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                    f"  DROP DATABASE [{sandbox_db}]; "
                    f"END"
                )
            logger.info("event=sandbox_down_complete run_id=%s", run_id)
            return {"run_id": run_id, "sandbox_database": sandbox_db, "status": "ok"}
        except pyodbc.Error as exc:
            logger.error("event=sandbox_down_failed run_id=%s error=%s", run_id, exc)
            return {
                "run_id": run_id,
                "sandbox_database": sandbox_db,
                "status": "error",
                "errors": [{"code": "SANDBOX_DOWN_FAILED", "message": str(exc)}],
            }

    def execute_scenario(
        self,
        run_id: str,
        scenario: dict[str, Any],
    ) -> dict[str, Any]:
        _validate_run_id(run_id)
        sandbox_db = self.sandbox_db_name(run_id)

        scenario_name = scenario.get("name", "unnamed")
        for key in ("target_table", "procedure", "given"):
            if key not in scenario:
                raise KeyError(f"Scenario missing required key: {key!r}")
        target_table = scenario["target_table"]
        procedure = scenario["procedure"]
        given = scenario["given"]

        _validate_identifier(target_table)
        _validate_identifier(procedure)
        for fixture in given:
            _validate_identifier(fixture["table"])

        for fixture in given:
            for row in fixture.get("rows", []):
                for col_name in row:
                    _validate_identifier(col_name)

        logger.info(
            "event=execute_scenario run_id=%s scenario=%s procedure=%s",
            run_id, scenario_name, procedure,
        )

        try:
            with self._connect(database=sandbox_db) as conn:
                conn.autocommit = False
                cursor = conn.cursor()

                try:
                    for fixture in given:
                        table = fixture["table"]
                        rows = fixture["rows"]
                        if not rows:
                            continue
                        columns = list(rows[0].keys())
                        col_list = ", ".join(f"[{c}]" for c in columns)
                        placeholders = ", ".join("?" for _ in columns)
                        insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
                        value_lists = [[row[c] for c in columns] for row in rows]
                        cursor.executemany(insert_sql, value_lists)

                    cursor.execute(f"EXEC {procedure}")

                    cursor.execute(f"SELECT * FROM {target_table}")
                    result_columns = [desc[0] for desc in cursor.description]
                    result_rows = [
                        dict(zip(result_columns, row)) for row in cursor.fetchall()
                    ]
                finally:
                    conn.rollback()

            logger.info(
                "event=scenario_complete run_id=%s scenario=%s rows=%d",
                run_id, scenario_name, len(result_rows),
            )
            return {
                "schema_version": "1.0",
                "run_id": run_id,
                "scenario_name": scenario_name,
                "status": "ok",
                "ground_truth_rows": _serialize_rows(result_rows),
                "row_count": len(result_rows),
                "errors": [],
            }

        except pyodbc.Error as exc:
            logger.error(
                "event=scenario_failed run_id=%s scenario=%s error=%s",
                run_id, scenario_name, exc,
            )
            return {
                "schema_version": "1.0",
                "run_id": run_id,
                "scenario_name": scenario_name,
                "status": "error",
                "ground_truth_rows": [],
                "row_count": 0,
                "errors": [{"code": "SCENARIO_FAILED", "message": str(exc)}],
            }


def _serialize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ensure all values are JSON-serializable."""
    return [
        {
            k: v if isinstance(v, (int, float, str, bool, type(None))) else str(v)
            for k, v in row.items()
        }
        for row in rows
    ]
