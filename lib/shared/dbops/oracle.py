"""Oracle DB-ops adapter."""

from __future__ import annotations

from shared.dbops.base import ColumnSpec, DatabaseOperations

_oracledb = None


def _import_oracledb():
    global _oracledb
    if _oracledb is None:
        try:
            import oracledb
        except ImportError as exc:
            raise ImportError(
                "oracledb is required for Oracle DB operations. Install it with: uv pip install oracledb"
            ) from exc
        _oracledb = oracledb
    return _oracledb


class OracleOperations(DatabaseOperations):
    fixture_script_relpath = "tests/integration/oracle/fixtures/materialize.sh"

    def environment_name(self) -> str:
        return self.role.connection.service or "FREEPDB1"

    def materialize_migration_test_env(self) -> dict[str, str]:
        env = {
            "ORACLE_HOST": self.role.connection.host or "localhost",
            "ORACLE_PORT": self.role.connection.port or "1521",
            "ORACLE_SERVICE": self.environment_name(),
        }
        if self.role.connection.user:
            env["ORACLE_USER"] = self.role.connection.user
        if self.role.connection.schema_name:
            env["ORACLE_SCHEMA"] = self.role.connection.schema_name
        password = self._read_secret(self.role.connection.password_env)
        if password:
            env["ORACLE_PWD"] = password
        return env

    def _connect(self):
        host = self.role.connection.host or "localhost"
        port = self.role.connection.port or "1521"
        service = self.role.connection.service or self.environment_name()
        user = self.role.connection.user
        password = self._read_secret(self.role.connection.password_env)
        if not user:
            raise ValueError("runtime.target.connection.user is required for Oracle target setup")
        if not password:
            raise ValueError(
                "runtime.target.connection.password_env must reference a set environment variable for Oracle target setup"
            )
        return _import_oracledb().connect(
            user=user,
            password=password,
            dsn=f"{host}:{port}/{service}",
        )

    def ensure_source_schema(self, schema_name: str) -> None:
        self._validate_identifier(schema_name)
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM all_users WHERE username = :1",
                [schema_name.upper()],
            )
            if cursor.fetchone()[0] == 0:
                raise ValueError(
                    f"Oracle schema '{schema_name}' does not exist. "
                    "Create the schema/user before running /setup-target."
                )
        finally:
            conn.close()

    def list_source_tables(self, schema_name: str) -> set[str]:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name",
                [schema_name.upper()],
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
        self._validate_identifier(table_name)
        for column in columns:
            self._validate_identifier(column.name)
        self.ensure_source_schema(schema_name)
        rendered = ", ".join(
            f'"{column.name}" {self._map_type(column.source_type)} {"NULL" if column.nullable else "NOT NULL"}'
            for column in columns
        )
        ddl = f'CREATE TABLE "{schema_name}"."{table_name}" ({rendered})'
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(ddl)
            conn.commit()
        finally:
            conn.close()

    def read_table_rows(
        self,
        schema_name: str,
        table_name: str,
        columns: list[str] | None = None,
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        self._validate_identifier(schema_name)
        self._validate_identifier(table_name)
        selected_columns = list(columns or [])
        for column in selected_columns:
            self._validate_identifier(column)
        select_list = ", ".join(f'"{column}"' for column in selected_columns) if selected_columns else "*"
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f'SELECT {select_list} FROM "{schema_name}"."{table_name}"')
            result_columns = [column[0] for column in cursor.description]
            rows = [tuple(row) for row in cursor.fetchall()]
            return result_columns, rows
        finally:
            conn.close()

    def _map_type(self, source_type: str) -> str:
        normalized = self._base_type_token(source_type)
        if normalized in {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"}:
            return "NUMBER(19)"
        if normalized in {"DECIMAL", "NUMERIC", "MONEY"}:
            return "NUMBER(38,10)"
        if normalized in {"FLOAT", "DOUBLE", "REAL"}:
            return "BINARY_DOUBLE"
        if normalized == "DATE":
            return "DATE"
        if normalized in {"TIME", "TIMESTAMP", "DATETIME"}:
            return "TIMESTAMP"
        if normalized in {"BINARY", "VARBINARY", "BLOB", "RAW"}:
            return "BLOB"
        return "VARCHAR2(4000)"
