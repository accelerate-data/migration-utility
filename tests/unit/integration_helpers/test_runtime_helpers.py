from __future__ import annotations

import pytest
from shared.runtime_config_models import RuntimeRole
from tests.integration.runtime_helpers import (
    ORACLE_MIGRATION_SCHEMA,
    ORACLE_SANDBOX_ENV,
    ORACLE_SOURCE_ENV,
    SQL_SERVER_MIGRATION_DATABASE,
    SQL_SERVER_SANDBOX_ENV,
    SQL_SERVER_SOURCE_ENV,
    build_oracle_dsn,
    build_oracle_fixture_admin_role,
    build_oracle_sandbox_admin_connect_kwargs,
    build_oracle_sandbox_dsn,
    build_oracle_sandbox_manifest,
    build_sql_server_admin_connection_string,
    build_sql_server_connection_string,
    build_sql_server_fixture_admin_role,
    build_sql_server_sandbox_manifest,
    oracle_is_available,
    oracle_sandbox_is_available,
    require_env,
    sql_server_sandbox_is_available,
)


def test_build_sql_server_fixture_admin_role_defaults_to_migrationtest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "source-host")
    monkeypatch.setenv("SOURCE_MSSQL_PORT", "1500")
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "source-host")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox-user")

    role = build_sql_server_fixture_admin_role()

    assert isinstance(role, RuntimeRole)
    assert role.technology == "sql_server"
    assert role.connection.database == SQL_SERVER_MIGRATION_DATABASE
    assert role.connection.schema_name is not None
    assert role.connection.host == "source-host"
    assert role.connection.port == "1500"
    assert role.connection.user == "sandbox-user"
    assert role.connection.password_env == "SANDBOX_MSSQL_PASSWORD"


def test_build_oracle_fixture_admin_role_defaults_to_migrationtest_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_ORACLE_HOST", "source-host")
    monkeypatch.setenv("SOURCE_ORACLE_PORT", "1522")
    monkeypatch.setenv("SOURCE_ORACLE_SERVICE", "SOURCEPDB")
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "source-host")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "sandbox-user")

    role = build_oracle_fixture_admin_role()

    assert isinstance(role, RuntimeRole)
    assert role.technology == "oracle"
    assert role.connection.host == "source-host"
    assert role.connection.port == "1522"
    assert role.connection.service == "SOURCEPDB"
    assert role.connection.user == "sandbox-user"
    assert role.connection.schema_name == ORACLE_MIGRATION_SCHEMA
    assert role.connection.password_env == "SANDBOX_ORACLE_PASSWORD"


def test_build_sql_server_fixture_admin_role_fails_loudly_for_split_hosts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "source-host")
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")

    with pytest.raises(RuntimeError, match="fixture materialization requires"):
        build_sql_server_fixture_admin_role()


def test_build_oracle_fixture_admin_role_fails_loudly_for_split_hosts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_ORACLE_HOST", "source-host")
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "sandbox-host")

    with pytest.raises(RuntimeError, match="fixture materialization requires"):
        build_oracle_fixture_admin_role()


def test_build_sql_server_connection_string_targets_canonical_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "sa")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "secret")

    connection_string = build_sql_server_connection_string(login_timeout=1)

    assert f"DATABASE={SQL_SERVER_MIGRATION_DATABASE};" in connection_string
    assert "DRIVER={FreeTDS};" in connection_string
    assert "LoginTimeout=1;" in connection_string


def test_build_sql_server_admin_connection_string_uses_sandbox_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1500")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox-user")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "secret")

    connection_string = build_sql_server_admin_connection_string(database="master")

    assert "SERVER=sandbox-host,1500;" in connection_string
    assert "DATABASE=master;" in connection_string
    assert "UID=sandbox-user;" in connection_string
    assert "DRIVER={FreeTDS};" in connection_string


def test_build_sql_server_admin_connection_string_defaults_to_master(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox-user")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "secret")

    connection_string = build_sql_server_admin_connection_string()

    assert "DATABASE=master;" in connection_string


def test_build_sql_server_connection_string_escapes_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "sa")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "pa;ss}word")

    connection_string = build_sql_server_connection_string()

    assert "PWD={pa;ss}}word};" in connection_string


def test_build_sql_server_sandbox_manifest_uses_canonical_source_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "source-host")
    monkeypatch.setenv("SOURCE_MSSQL_USER", "source-user")
    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox-user")

    manifest = build_sql_server_sandbox_manifest()

    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]
    assert source["database"] == SQL_SERVER_MIGRATION_DATABASE
    assert source["password_env"] == "SOURCE_MSSQL_PASSWORD"
    assert "driver" not in source
    assert sandbox["password_env"] == "SANDBOX_MSSQL_PASSWORD"
    assert "driver" not in sandbox


def test_sql_server_sandbox_is_available_uses_sandbox_admin_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, bool]] = []

    class _Connection:
        def close(self) -> None:
            return None

    class _Pyodbc:
        Error = Exception

        def connect(self, conn_str: str, autocommit: bool) -> _Connection:
            calls.append((conn_str, autocommit))
            return _Connection()

    monkeypatch.setenv("SANDBOX_MSSQL_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_MSSQL_PORT", "1500")
    monkeypatch.setenv("SANDBOX_MSSQL_USER", "sandbox-user")
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "sandbox-pass")
    monkeypatch.setenv("SANDBOX_MSSQL_ADMIN_DATABASE", "sandbox-admin")

    assert sql_server_sandbox_is_available(_Pyodbc()) is True
    assert calls[0][1] is True
    assert "SERVER=sandbox-host,1500;" in calls[0][0]
    assert "DATABASE=sandbox-admin;" in calls[0][0]
    assert "DRIVER={FreeTDS};" in calls[0][0]


def test_build_oracle_sandbox_manifest_uses_source_and_sandbox_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_ORACLE_HOST", "source-host")
    monkeypatch.setenv("SOURCE_ORACLE_SERVICE", "SOURCEPDB")
    monkeypatch.setenv("SOURCE_ORACLE_USER", "source-user")
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "SANDBOXPDB")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "sandbox-user")

    manifest = build_oracle_sandbox_manifest()

    source = manifest["runtime"]["source"]["connection"]
    sandbox = manifest["runtime"]["sandbox"]["connection"]
    assert source["service"] == "SOURCEPDB"
    assert source["schema"] == ORACLE_MIGRATION_SCHEMA
    assert source["password_env"] == "SOURCE_ORACLE_PASSWORD"
    assert sandbox["service"] == "SANDBOXPDB"
    assert sandbox["password_env"] == "SANDBOX_ORACLE_PASSWORD"


def test_build_oracle_dsn_uses_source_service(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_ORACLE_HOST", "source-host")
    monkeypatch.setenv("SOURCE_ORACLE_PORT", "1522")
    monkeypatch.setenv("SOURCE_ORACLE_SERVICE", "SOURCEPDB")

    assert build_oracle_dsn() == "source-host:1522/SOURCEPDB"


def test_build_oracle_sandbox_dsn_uses_sandbox_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1523")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "SANDBOXPDB")

    assert build_oracle_sandbox_dsn() == "sandbox-host:1523/SANDBOXPDB"


def test_require_env_skips_with_role_and_missing_var_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SOURCE_MSSQL_HOST", raising=False)
    monkeypatch.delenv("SOURCE_MSSQL_PASSWORD", raising=False)

    with pytest.raises(
        pytest.skip.Exception,
        match="source env missing: SOURCE_MSSQL_HOST, SOURCE_MSSQL_PASSWORD",
    ):
        require_env("source", ("SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PASSWORD"))


def test_require_env_accepts_set_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_MSSQL_HOST", "localhost")
    monkeypatch.setenv("SOURCE_MSSQL_PASSWORD", "secret")

    require_env("source", ("SOURCE_MSSQL_HOST", "SOURCE_MSSQL_PASSWORD"))


def test_role_env_constants_do_not_include_legacy_names() -> None:
    assert SQL_SERVER_SOURCE_ENV == (
        "SOURCE_MSSQL_HOST",
        "SOURCE_MSSQL_PORT",
        "SOURCE_MSSQL_DB",
        "SOURCE_MSSQL_SCHEMA",
        "SOURCE_MSSQL_USER",
        "SOURCE_MSSQL_PASSWORD",
    )
    assert SQL_SERVER_SANDBOX_ENV == (
        "SANDBOX_MSSQL_HOST",
        "SANDBOX_MSSQL_PORT",
        "SANDBOX_MSSQL_USER",
        "SANDBOX_MSSQL_PASSWORD",
    )
    assert ORACLE_SOURCE_ENV == (
        "SOURCE_ORACLE_HOST",
        "SOURCE_ORACLE_PORT",
        "SOURCE_ORACLE_SERVICE",
        "SOURCE_ORACLE_SCHEMA",
        "SOURCE_ORACLE_USER",
        "SOURCE_ORACLE_PASSWORD",
    )
    assert ORACLE_SANDBOX_ENV == (
        "SANDBOX_ORACLE_HOST",
        "SANDBOX_ORACLE_PORT",
        "SANDBOX_ORACLE_SERVICE",
        "SANDBOX_ORACLE_USER",
        "SANDBOX_ORACLE_PASSWORD",
    )


def test_oracle_is_available_uses_source_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, object]] = []

    class _Connection:
        def close(self) -> None:
            return None

    class _OracleDb:
        Error = Exception
        AUTH_MODE_SYSDBA = 2
        AUTH_MODE_DEFAULT = 1

        def connect(self, **kwargs: object) -> _Connection:
            calls.append(kwargs)
            return _Connection()

    monkeypatch.setenv("SOURCE_ORACLE_HOST", "source-host")
    monkeypatch.setenv("SOURCE_ORACLE_PORT", "1522")
    monkeypatch.setenv("SOURCE_ORACLE_SERVICE", "SOURCEPDB")
    monkeypatch.setenv("SOURCE_ORACLE_SCHEMA", "MIGRATIONTEST")
    monkeypatch.setenv("SOURCE_ORACLE_USER", "source-user")
    monkeypatch.setenv("SOURCE_ORACLE_PASSWORD", "source-password")

    assert oracle_is_available(_OracleDb()) is True
    assert calls[0]["user"] == "source-user"
    assert calls[0]["password"] == "source-password"
    assert calls[0]["dsn"] == "source-host:1522/SOURCEPDB"


def test_oracle_sandbox_is_available_uses_sandbox_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class _Connection:
        def close(self) -> None:
            return None

    class _OracleDb:
        Error = Exception
        AUTH_MODE_SYSDBA = 2
        AUTH_MODE_DEFAULT = 1

        def connect(self, **kwargs: object) -> _Connection:
            calls.append(kwargs)
            return _Connection()

    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1523")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "SANDBOXPDB")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "sandbox-user")
    monkeypatch.setenv("SANDBOX_ORACLE_PASSWORD", "sandbox-password")

    assert oracle_sandbox_is_available(_OracleDb()) is True
    assert calls[0]["user"] == "sandbox-user"
    assert calls[0]["password"] == "sandbox-password"
    assert calls[0]["dsn"] == "sandbox-host:1523/SANDBOXPDB"


def test_build_oracle_sandbox_admin_connect_kwargs_uses_sandbox_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _OracleDb:
        AUTH_MODE_SYSDBA = 2
        AUTH_MODE_DEFAULT = 1

    monkeypatch.setenv("SANDBOX_ORACLE_HOST", "sandbox-host")
    monkeypatch.setenv("SANDBOX_ORACLE_PORT", "1523")
    monkeypatch.setenv("SANDBOX_ORACLE_SERVICE", "SANDBOXPDB")
    monkeypatch.setenv("SANDBOX_ORACLE_USER", "sandbox-user")
    monkeypatch.setenv("SANDBOX_ORACLE_PASSWORD", "sandbox-password")

    kwargs = build_oracle_sandbox_admin_connect_kwargs(_OracleDb())

    assert kwargs["user"] == "sandbox-user"
    assert kwargs["password"] == "sandbox-password"
    assert kwargs["dsn"] == "sandbox-host:1523/SANDBOXPDB"
