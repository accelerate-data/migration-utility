from __future__ import annotations

import pytest
from shared.runtime_config_models import RuntimeRole
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    SQL_SERVER_MIGRATION_DATABASE,
    _require_env,
    build_oracle_dsn,
    build_oracle_sandbox_manifest,
    build_oracle_admin_role,
    build_sql_server_connection_string,
    build_sql_server_sandbox_manifest,
    build_sql_server_source_role,
)


@pytest.fixture(autouse=True)
def _set_sql_server_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MSSQL_HOST", "localhost")
    monkeypatch.setenv("MSSQL_PORT", "1433")
    monkeypatch.setenv("MSSQL_USER", "sa")
    monkeypatch.setenv("MSSQL_DRIVER", "FreeTDS")
    monkeypatch.setenv("SA_PASSWORD", "testpassword")
    monkeypatch.setenv("MSSQL_ADMIN_DATABASE", "master")


@pytest.fixture()
def _set_oracle_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_HOST", "localhost")
    monkeypatch.setenv("ORACLE_PORT", "1521")
    monkeypatch.setenv("ORACLE_SERVICE", "FREEPDB1")
    monkeypatch.setenv("ORACLE_ADMIN_USER", "sys")
    monkeypatch.setenv("ORACLE_PWD", "testpassword")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "FREE")


def test_require_env_raises_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MSSQL_HOST", raising=False)
    with pytest.raises(EnvironmentError, match="MSSQL_HOST"):
        _require_env("MSSQL_HOST")


def test_require_env_returns_value() -> None:
    assert _require_env("MSSQL_HOST") == "localhost"


def test_build_sql_server_source_role_defaults_to_migrationtest() -> None:
    role = build_sql_server_source_role()

    assert isinstance(role, RuntimeRole)
    assert role.technology == "sql_server"
    assert role.connection.database == SQL_SERVER_MIGRATION_DATABASE
    assert role.connection.password_env == "SA_PASSWORD"


@pytest.mark.usefixtures("_set_oracle_env")
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


def test_build_sql_server_connection_string_escapes_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SA_PASSWORD", "pa;ss}word")

    connection_string = build_sql_server_connection_string()

    assert "PWD={pa;ss}}word};" in connection_string


def test_build_sql_server_sandbox_manifest_uses_canonical_source_database() -> None:
    manifest = build_sql_server_sandbox_manifest()

    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]
    assert source["database"] == SQL_SERVER_MIGRATION_DATABASE
    assert source["password_env"] == "SA_PASSWORD"
    assert sandbox["password_env"] == "SA_PASSWORD"


@pytest.mark.usefixtures("_set_oracle_env")
def test_build_oracle_sandbox_manifest_uses_canonical_schema_defaults() -> None:
    manifest = build_oracle_sandbox_manifest()

    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]
    assert source["schema"] == ORACLE_MIGRATION_SCHEMA
    assert source["password_env"] == "ORACLE_SCHEMA_PASSWORD"
    assert sandbox["password_env"] == "ORACLE_PWD"


@pytest.mark.usefixtures("_set_oracle_env")
def test_build_oracle_dsn_uses_configured_service() -> None:
    assert build_oracle_dsn() == "localhost:1521/FREEPDB1"


def test_build_sql_server_source_role_raises_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MSSQL_HOST", raising=False)
    with pytest.raises(EnvironmentError, match="MSSQL_HOST"):
        build_sql_server_source_role()


@pytest.mark.usefixtures("_set_oracle_env")
def test_build_oracle_admin_role_raises_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORACLE_HOST", raising=False)
    with pytest.raises(EnvironmentError, match="ORACLE_HOST"):
        build_oracle_admin_role()
