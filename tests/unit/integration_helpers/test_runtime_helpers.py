from __future__ import annotations

from shared.runtime_config_models import RuntimeRole
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    SQL_SERVER_MIGRATION_DATABASE,
    build_oracle_dsn,
    build_oracle_sandbox_manifest,
    build_oracle_admin_role,
    build_sql_server_connection_string,
    build_sql_server_sandbox_manifest,
    build_sql_server_source_role,
    find_oracle_cli,
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


def test_build_sql_server_connection_string_targets_canonical_database() -> None:
    connection_string = build_sql_server_connection_string(login_timeout=1)

    assert f"DATABASE={SQL_SERVER_MIGRATION_DATABASE};" in connection_string
    assert "LoginTimeout=1;" in connection_string


def test_build_sql_server_sandbox_manifest_uses_canonical_source_database() -> None:
    manifest = build_sql_server_sandbox_manifest()

    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]
    assert source["database"] == SQL_SERVER_MIGRATION_DATABASE
    assert source["password_env"] == "SA_PASSWORD"
    assert sandbox["password_env"] == "SA_PASSWORD"


def test_build_oracle_sandbox_manifest_uses_canonical_schema_defaults() -> None:
    manifest = build_oracle_sandbox_manifest()

    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]
    assert source["schema"] == ORACLE_MIGRATION_SCHEMA
    assert source["password_env"] == "ORACLE_SCHEMA_PASSWORD"
    assert sandbox["password_env"] == "ORACLE_PWD"


def test_build_oracle_dsn_uses_default_service() -> None:
    assert build_oracle_dsn() == "localhost:1521/FREEPDB1"


def test_find_oracle_cli_prefers_sqlcl_bin(monkeypatch) -> None:
    monkeypatch.setenv("SQLCL_BIN", "/tmp/sql")

    assert find_oracle_cli() == "/tmp/sql"
