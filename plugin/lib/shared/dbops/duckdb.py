"""DuckDB DB-ops adapter."""

from __future__ import annotations

from pathlib import Path

from shared.dbops.base import ColumnSpec, DatabaseOperations

_duckdb = None


def _import_duckdb():
    global _duckdb
    if _duckdb is None:
        try:
            import duckdb
        except ImportError as exc:
            raise ImportError(
                "duckdb is required for DuckDB DB operations. Install it with: uv pip install duckdb"
            ) from exc
        _duckdb = duckdb
    return _duckdb


class DuckDbOperations(DatabaseOperations):
    fixture_script_relpath = "scripts/sql/duckdb/materialize-migration-test.sh"

    def environment_name(self) -> str:
        path = Path(self.role.connection.path or ".runtime/duckdb/migrationtest.duckdb")
        if not path.is_absolute() and self.project_root is not None:
            path = self.project_root / path
        return str(path)

    def materialize_migration_test_env(self) -> dict[str, str]:
        return {"DUCKDB_PATH": self.environment_name()}

    def _connect(self):
        db_path = Path(self.environment_name())
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return _import_duckdb().connect(str(db_path))

    def ensure_source_schema(self, schema_name: str) -> None:
        conn = self._connect()
        try:
            conn.execute(f'create schema if not exists "{schema_name}"')
        finally:
            conn.close()

    def list_source_tables(self, schema_name: str) -> set[str]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "select table_name from information_schema.tables where table_schema = ? and table_type = 'BASE TABLE'",
                [schema_name],
            ).fetchall()
            return {row[0].lower() for row in rows}
        finally:
            conn.close()

    def create_source_table(
        self,
        schema_name: str,
        table_name: str,
        columns: list[ColumnSpec],
    ) -> None:
        rendered = ", ".join(
            f'"{column.name}" {self._map_type(column.source_type)} {"NULL" if column.nullable else "NOT NULL"}'
            for column in columns
        )
        conn = self._connect()
        try:
            conn.execute(f'create table "{schema_name}"."{table_name}" ({rendered})')
        finally:
            conn.close()

    def _map_type(self, source_type: str) -> str:
        normalized = self._base_type_token(source_type)
        if normalized in {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"}:
            return "BIGINT"
        if normalized in {"DECIMAL", "NUMERIC", "MONEY"}:
            return "DECIMAL(38, 10)"
        if normalized in {"FLOAT", "DOUBLE", "REAL"}:
            return "DOUBLE"
        if normalized == "DATE":
            return "DATE"
        if normalized in {"TIME", "TIMESTAMP", "DATETIME"}:
            return "TIMESTAMP"
        if normalized in {"BOOL", "BOOLEAN", "BIT"}:
            return "BOOLEAN"
        if normalized in {"BINARY", "VARBINARY", "BLOB", "RAW"}:
            return "BLOB"
        return "VARCHAR"
