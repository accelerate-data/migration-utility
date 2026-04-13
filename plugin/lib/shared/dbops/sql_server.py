"""SQL Server DB-ops adapter."""

from __future__ import annotations

from shared.dbops.base import DatabaseOperations


class SqlServerOperations(DatabaseOperations):
    fixture_script_relpath = "scripts/sql/sql_server/materialize-migration-test.sh"

    def environment_name(self) -> str:
        return self.role.connection.database or "MigrationTest"

    def materialize_migration_test_env(self) -> dict[str, str]:
        env = {
            "MSSQL_HOST": self.role.connection.host or "localhost",
            "MSSQL_PORT": self.role.connection.port or "1433",
            "MSSQL_DB": self.environment_name(),
        }
        if self.role.connection.user:
            env["MSSQL_USER"] = self.role.connection.user
        if self.role.connection.driver:
            env["MSSQL_DRIVER"] = self.role.connection.driver
        password = self._read_secret(self.role.connection.password_env) or self._read_secret("SA_PASSWORD")
        if password:
            env["SA_PASSWORD"] = password
        return env
