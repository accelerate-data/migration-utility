"""DuckDB DB-ops adapter."""

from __future__ import annotations

from shared.dbops.base import DatabaseOperations


class DuckDbOperations(DatabaseOperations):
    fixture_script_relpath = "scripts/sql/duckdb/materialize-migration-test.sh"

    def environment_name(self) -> str:
        return self.role.connection.path or ".runtime/duckdb/migrationtest.duckdb"

    def materialize_migration_test_env(self) -> dict[str, str]:
        return {"DUCKDB_PATH": self.environment_name()}
