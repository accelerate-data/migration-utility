"""Oracle DB-ops adapter."""

from __future__ import annotations

from shared.dbops.base import DatabaseOperations


class OracleOperations(DatabaseOperations):
    fixture_script_relpath = "scripts/sql/oracle/materialize-migration-test.sh"

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
        password = self._read_secret(self.role.connection.password_env) or self._read_secret("ORACLE_PWD")
        if password:
            env["ORACLE_PWD"] = password
        return env
