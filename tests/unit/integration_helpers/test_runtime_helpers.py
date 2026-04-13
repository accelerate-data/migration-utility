from __future__ import annotations

from shared.runtime_config_models import RuntimeRole
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    SQL_SERVER_MIGRATION_DATABASE,
    build_oracle_admin_role,
    build_sql_server_source_role,
)


def test_build_sql_server_source_role_defaults_to_migrationtest() -> None:
    role = build_sql_server_source_role()

    assert isinstance(role, RuntimeRole)
    assert role.technology == "sql_server"
    assert role.connection.database == SQL_SERVER_MIGRATION_DATABASE
    assert role.connection.password_env == "SA_PASSWORD"


def test_build_oracle_admin_role_defaults_to_migrationtest_schema() -> None:
    role = build_oracle_admin_role()

    assert isinstance(role, RuntimeRole)
    assert role.technology == "oracle"
    assert role.connection.service == "FREEPDB1"
    assert role.connection.schema_name == ORACLE_MIGRATION_SCHEMA
    assert role.connection.password_env == "ORACLE_PWD"
