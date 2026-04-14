"""SQL Server DB-ops adapter."""

from __future__ import annotations

from typing import Any

from shared.db_connect import build_sql_server_connection_string
from shared.dbops.base import ColumnSpec, DatabaseOperations

_pyodbc = None


def _import_pyodbc():
    global _pyodbc
    if _pyodbc is None:
        try:
            import pyodbc
        except ImportError as exc:
            raise ImportError(
                "pyodbc is required for SQL Server DB operations. Install it with: uv pip install pyodbc"
            ) from exc
        _pyodbc = pyodbc
    return _pyodbc


class SqlServerOperations(DatabaseOperations):
    fixture_script_relpath = "scripts/sql/sql_server/materialize-migration-test.sh"

    def environment_name(self) -> str:
        return self.role.connection.database or "MigrationTest"

    def materialize_migration_test_env(self) -> dict[str, str]:
        env = {
            "MSSQL_HOST": self.role.connection.host or "localhost",
            "MSSQL_PORT": self.role.connection.port or "1433",
            "MSSQL_DB": self.environment_name(),
            "MSSQL_SCHEMA": self.role.connection.schema_name or "MigrationTest",
        }
        if self.role.connection.user:
            env["MSSQL_USER"] = self.role.connection.user
        if self.role.connection.driver:
            env["MSSQL_DRIVER"] = self.role.connection.driver
        password = self._read_secret(self.role.connection.password_env)
        if password:
            env["SA_PASSWORD"] = password
        return env

    def _connect(self):
        host = self.role.connection.host or "localhost"
        port = self.role.connection.port or "1433"
        database = self.role.connection.database or self.environment_name()
        user = self.role.connection.user or "sa"
        driver = self.role.connection.driver or "ODBC Driver 18 for SQL Server"
        password = self._read_secret(self.role.connection.password_env)
        if not password:
            raise ValueError(
                "runtime.target.connection.password_env must reference a set environment variable for SQL Server target setup"
            )
        return _import_pyodbc().connect(
            build_sql_server_connection_string(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                driver=driver,
            ),
            autocommit=True,
        )

    def ensure_source_schema(self, schema_name: str) -> None:
        self._validate_identifier(schema_name)
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema_name)
            if cursor.fetchone() is None:
                cursor.execute(f"CREATE SCHEMA [{schema_name}]")
        finally:
            conn.close()

    def list_source_tables(self, schema_name: str) -> set[str]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ?",
                schema_name,
            )
            return {row[0].lower() for row in cursor.fetchall()}
        finally:
            conn.close()

    def create_source_table(
        self,
        schema_name: str,
        table_name: str,
        columns: list[ColumnSpec],
    ) -> None:
        self._validate_identifier(schema_name)
        self._validate_identifier(table_name)
        for column in columns:
            self._validate_identifier(column.name)
        rendered = ", ".join(
            f"[{column.name}] {self._map_type(column.source_type)} {'NULL' if column.nullable else 'NOT NULL'}"
            for column in columns
        )
        ddl = f"CREATE TABLE [{schema_name}].[{table_name}] ({rendered})"
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(ddl)
        finally:
            conn.close()

    def _map_type(self, source_type: str) -> str:
        normalized = source_type.upper().strip()
        if normalized:
            return normalized
        return "NVARCHAR(MAX)"
