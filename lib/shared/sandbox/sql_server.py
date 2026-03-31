"""SQL Server sandbox backend using pyodbc."""

from __future__ import annotations

import logging
from typing import Any

import pyodbc

from shared.sandbox.base import SandboxBackend

logger = logging.getLogger(__name__)


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

    def _connect(self, *, database: str | None = None) -> pyodbc.Connection:
        db = database or self.database
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={db};"
            f"UID=sa;PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str, autocommit=True)

    def sandbox_up(
        self,
        run_id: str,
        schemas: list[str],
        source_database: str,
    ) -> dict[str, Any]:
        sandbox_db = self.sandbox_db_name(run_id)
        tables_cloned: list[str] = []
        procedures_cloned: list[str] = []
        errors: list[dict[str, str]] = []

        logger.info(
            "event=sandbox_up run_id=%s sandbox_db=%s source=%s schemas=%s",
            run_id, sandbox_db, source_database, schemas,
        )

        conn = self._connect()
        cursor = conn.cursor()

        try:
            cursor.execute(
                f"IF DB_ID('{sandbox_db}') IS NOT NULL "
                f"DROP DATABASE [{sandbox_db}]"
            )
            cursor.execute(f"CREATE DATABASE [{sandbox_db}]")
            conn.close()
            logger.info("event=database_created run_id=%s db=%s", run_id, sandbox_db)

            sandbox_conn = self._connect(database=sandbox_db)
            sandbox_cursor = sandbox_conn.cursor()

            for schema in schemas:
                try:
                    sandbox_cursor.execute(
                        f"IF NOT EXISTS ("
                        f"  SELECT 1 FROM sys.schemas WHERE name = '{schema}'"
                        f") EXEC('CREATE SCHEMA [{schema}]')"
                    )
                except pyodbc.Error as exc:
                    errors.append({
                        "code": "SCHEMA_CREATE_FAILED",
                        "message": f"Failed to create schema {schema}: {exc}",
                    })

            source_conn = self._connect(database=source_database)
            source_cursor = source_conn.cursor()

            # Clone tables (schema only, no data, no constraints)
            source_cursor.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA IN ("
                + ",".join(f"'{s}'" for s in schemas)
                + ") ORDER BY TABLE_SCHEMA, TABLE_NAME"
            )
            for schema_name, table_name in source_cursor.fetchall():
                fqn = f"[{schema_name}].[{table_name}]"
                try:
                    sandbox_cursor.execute(
                        f"SELECT TOP 0 * INTO {fqn} "
                        f"FROM [{source_database}].{fqn}"
                    )
                    tables_cloned.append(f"{schema_name}.{table_name}")
                except pyodbc.Error as exc:
                    errors.append({
                        "code": "TABLE_CLONE_FAILED",
                        "message": f"Failed to clone {fqn}: {exc}",
                    })

            # Clone procedures
            source_cursor.execute(
                "SELECT s.name AS schema_name, p.name AS proc_name, "
                "       OBJECT_DEFINITION(p.object_id) AS definition "
                "FROM sys.procedures p "
                "JOIN sys.schemas s ON p.schema_id = s.schema_id "
                "WHERE s.name IN ("
                + ",".join(f"'{s}'" for s in schemas)
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
                    procedures_cloned.append(fqn)
                except pyodbc.Error as exc:
                    errors.append({
                        "code": "PROC_CLONE_FAILED",
                        "message": f"Failed to clone procedure {fqn}: {exc}",
                    })

            source_conn.close()
            sandbox_conn.close()

        except pyodbc.Error as exc:
            logger.error(
                "event=sandbox_up_failed run_id=%s error=%s", run_id, exc,
            )
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
        sandbox_db = self.sandbox_db_name(run_id)
        logger.info("event=sandbox_down run_id=%s sandbox_db=%s", run_id, sandbox_db)

        conn = self._connect()
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"IF DB_ID('{sandbox_db}') IS NOT NULL "
                f"BEGIN "
                f"  ALTER DATABASE [{sandbox_db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"  DROP DATABASE [{sandbox_db}]; "
                f"END"
            )
            conn.close()
            logger.info("event=sandbox_down_complete run_id=%s", run_id)
            return {"run_id": run_id, "sandbox_database": sandbox_db, "status": "ok"}
        except pyodbc.Error as exc:
            conn.close()
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
        sandbox_db = self.sandbox_db_name(run_id)
        scenario_name = scenario.get("name", "unnamed")
        target_table = scenario["target_table"]
        procedure = scenario["procedure"]
        given = scenario["given"]

        logger.info(
            "event=execute_scenario run_id=%s scenario=%s procedure=%s",
            run_id, scenario_name, procedure,
        )

        conn = self._connect(database=sandbox_db)
        cursor = conn.cursor()

        try:
            # 1. Insert fixture rows
            for fixture in given:
                table = fixture["table"]
                rows = fixture["rows"]
                if not rows:
                    continue
                columns = list(rows[0].keys())
                col_list = ", ".join(f"[{c}]" for c in columns)
                placeholders = ", ".join("?" for _ in columns)
                insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
                for row in rows:
                    values = [row[c] for c in columns]
                    cursor.execute(insert_sql, values)

            # 2. Execute the procedure
            cursor.execute(f"EXEC {procedure}")

            # 3. Capture ground truth from target table
            cursor.execute(f"SELECT * FROM {target_table}")
            result_columns = [desc[0] for desc in cursor.description]
            result_rows = [
                dict(zip(result_columns, row)) for row in cursor.fetchall()
            ]

            # 4. Clean up — truncate all fixture tables and the target
            tables_to_clean = {target_table}
            for fixture in given:
                tables_to_clean.add(fixture["table"])
            for table in tables_to_clean:
                cursor.execute(f"DELETE FROM {table}")

            conn.close()

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
            conn.close()
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
    serialized = []
    for row in rows:
        clean: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, (int, float, str, bool, type(None))):
                clean[k] = v
            else:
                clean[k] = str(v)
        serialized.append(clean)
    return serialized
