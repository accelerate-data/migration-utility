"""Tests for DB-ops technology adapters."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from shared.dbops import get_dbops
from shared.runtime_config_models import RuntimeConnection, RuntimeRole


def test_sql_server_dbops_materialize_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SA_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="WarehouseOne",
            password_env="SA_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["MSSQL_DB"] == "WarehouseOne"
    assert env["MSSQL_SCHEMA"] == "MigrationTest"
    assert env["SA_PASSWORD"] == "secret"


def test_sql_server_dbops_materialize_env_uses_runtime_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SA_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="WarehouseOne",
            schema="FixtureSchema",
            password_env="SA_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["MSSQL_DB"] == "WarehouseOne"
    assert env["MSSQL_SCHEMA"] == "FixtureSchema"
    assert env["SA_PASSWORD"] == "secret"


def test_oracle_dbops_materialize_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_PWD", "secret")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host="localhost",
            port="1521",
            service="SANDBOXPDB",
            schema="BRONZE",
            password_env="ORACLE_PWD",
        ),
    )
    adapter = get_dbops("oracle").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["ORACLE_SERVICE"] == "SANDBOXPDB"
    assert env["ORACLE_SCHEMA"] == "BRONZE"
    assert env["ORACLE_PWD"] == "secret"


def test_oracle_ensure_source_schema_checks_named_schema_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host="localhost",
            port="1521",
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    monkeypatch.setenv("ORACLE_PWD", "secret")
    adapter = get_dbops("oracle").from_role(role)
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    adapter.ensure_source_schema("BRONZE")

    cursor.execute.assert_called_once()
    conn.close.assert_called_once()


def test_oracle_create_source_table_qualifies_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host="localhost",
            port="1521",
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    monkeypatch.setenv("ORACLE_PWD", "secret")
    adapter = get_dbops("oracle").from_role(role)
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]
    adapter.ensure_source_schema = MagicMock()  # type: ignore[method-assign]

    adapter.create_source_table("BRONZE", "Customer", [])

    ddl = cursor.execute.call_args.args[0]
    assert 'CREATE TABLE "BRONZE"."Customer" (' in ddl


def test_dbops_fixture_script_paths_are_repo_relative() -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="MigrationTest"),
    )
    adapter = get_dbops("sql_server").from_role(role)
    assert adapter.fixture_script_path(Path("/repo")) == Path("/repo/tests/integration/sql_server/fixtures/materialize.sh")


def test_get_dbops_rejects_unknown_technology() -> None:
    with pytest.raises(ValueError, match="Unsupported DB operations technology"):
        get_dbops("postgres")


def test_sql_server_dbops_closes_connection_after_schema_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SA_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="MigrationTest",
            password_env="SA_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    adapter.ensure_source_schema("bronze")

    conn.close.assert_called_once()


def test_oracle_map_type_does_not_treat_point_as_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host="localhost",
            port="1521",
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    monkeypatch.setenv("ORACLE_PWD", "secret")
    adapter = get_dbops("oracle").from_role(role)

    assert adapter._map_type("POINT") == "VARCHAR2(4000)"  # type: ignore[attr-defined]


def test_oracle_ensure_source_schema_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """ensure_source_schema raises ValueError when the schema does not exist."""
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            host="localhost",
            port="1521",
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    monkeypatch.setenv("ORACLE_PWD", "secret")
    adapter = get_dbops("oracle").from_role(role)
    cursor = MagicMock()
    cursor.fetchone.return_value = (0,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="does not exist"):
        adapter.ensure_source_schema("MISSING_SCHEMA")

    conn.close.assert_called_once()


def test_dbops_rejects_unsafe_identifier() -> None:
    """Identifier validation catches injection attempts."""
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="MigrationTest"),
    )
    adapter = get_dbops("sql_server").from_role(role)

    with pytest.raises(ValueError, match="Unsafe SQL identifier"):
        adapter.ensure_source_schema('bronze"; DROP TABLE --')


def test_sql_server_dbops_connect_escapes_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SA_PASSWORD", "pa;ss}word")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="WarehouseOne",
            password_env="SA_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    mock_pyodbc = MagicMock()
    monkeypatch.setattr("shared.dbops.sql_server._import_pyodbc", lambda: mock_pyodbc)

    adapter._connect()  # type: ignore[attr-defined]

    conn_str = mock_pyodbc.connect.call_args.args[0]
    assert "PWD={pa;ss}}word};" in conn_str
