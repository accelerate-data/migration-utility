"""Tests for DB-ops technology adapters."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from shared.dbops import get_dbops
from shared.runtime_config_models import RuntimeConnection, RuntimeRole


def test_sql_server_dbops_materialize_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="WarehouseOne",
            user="sa",
            password_env="SANDBOX_MSSQL_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["SOURCE_MSSQL_DB"] == "WarehouseOne"
    assert env["SOURCE_MSSQL_SCHEMA"] == "MigrationTest"
    assert "driver" not in {key.lower() for key in env}
    assert env["SANDBOX_MSSQL_USER"] == "sa"
    assert env["SANDBOX_MSSQL_PASSWORD"] == "secret"


def test_sql_server_dbops_materialize_env_uses_runtime_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SANDBOX_MSSQL_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="WarehouseOne",
            schema="FixtureSchema",
            user="sa",
            password_env="SANDBOX_MSSQL_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["SOURCE_MSSQL_DB"] == "WarehouseOne"
    assert env["SOURCE_MSSQL_SCHEMA"] == "FixtureSchema"
    assert "driver" not in {key.lower() for key in env}
    assert env["SANDBOX_MSSQL_PASSWORD"] == "secret"


def test_sql_server_materialize_script_ignores_legacy_driver_env() -> None:
    script = Path(__file__).resolve().parents[2] / "integration/sql_server/fixtures/materialize.sh"
    script_text = script.read_text(encoding="utf-8")

    legacy_driver_env = "MSSQL" + "_DRIVER"
    assert f'os.environ.get("{legacy_driver_env}"' not in script_text
    assert "SQL_SERVER_ODBC_DRIVER" in script_text


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
    monkeypatch.setenv("TARGET_MSSQL_PASSWORD", "secret")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="MigrationTest",
            password_env="TARGET_MSSQL_PASSWORD",
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
    monkeypatch.setenv("TARGET_MSSQL_PASSWORD", "pa;ss}word")
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(
            host="localhost",
            port="1433",
            database="WarehouseOne",
            password_env="TARGET_MSSQL_PASSWORD",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    mock_pyodbc = MagicMock()
    monkeypatch.setattr("shared.dbops.sql_server._import_pyodbc", lambda: mock_pyodbc)

    adapter._connect()  # type: ignore[attr-defined]

    conn_str = mock_pyodbc.connect.call_args.args[0]
    assert "DRIVER={FreeTDS};" in conn_str
    assert "PWD={pa;ss}}word};" in conn_str


def test_sql_server_fetch_source_rows_applies_limit_filter_and_columns() -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="WarehouseOne"),
    )
    adapter = get_dbops("sql_server").from_role(role)
    cursor = MagicMock()
    cursor.description = [("id",), ("name",)]
    cursor.fetchall.return_value = [(1, "Alice")]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    columns, rows = adapter.fetch_source_rows(
        "silver",
        "Customer",
        limit=25,
        predicate="id > 10",
        columns=["id", "name"],
    )

    assert columns == ["id", "name"]
    assert rows == [(1, "Alice")]
    cursor.execute.assert_called_once_with(
        "SELECT TOP (?) [id], [name] FROM [silver].[Customer] WHERE (id > 10) ORDER BY [id], [name]",
        25,
    )
    conn.close.assert_called_once()


def test_sql_server_truncate_and_insert_rows_use_parameterized_statements() -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="WarehouseOne"),
    )
    adapter = get_dbops("sql_server").from_role(role)
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    adapter.truncate_table("bronze", "Customer")
    inserted = adapter.insert_rows("bronze", "Customer", ["id", "name"], [(1, "Alice")])

    assert inserted == 1
    assert cursor.execute.call_args_list[0].args == ("TRUNCATE TABLE [bronze].[Customer]",)
    cursor.executemany.assert_called_once_with(
        "INSERT INTO [bronze].[Customer] ([id], [name]) VALUES (?, ?)",
        [(1, "Alice")],
    )
    assert conn.close.call_count == 2


def test_sql_server_fetch_source_rows_uses_explicit_order_columns() -> None:
    role = RuntimeRole(
        technology="sql_server",
        dialect="tsql",
        connection=RuntimeConnection(database="WarehouseOne"),
    )
    adapter = get_dbops("sql_server").from_role(role)
    cursor = MagicMock()
    cursor.description = [("id",), ("doc",)]
    cursor.fetchall.return_value = [(1, "value")]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    adapter.fetch_source_rows(
        "silver",
        "Document",
        limit=25,
        columns=["id", "doc"],
        order_by_columns=["id"],
    )

    cursor.execute.assert_called_once_with(
        "SELECT TOP (?) [id], [doc] FROM [silver].[Document] ORDER BY [id]",
        25,
    )


def test_oracle_fetch_source_rows_applies_limit_filter_and_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_PWD", "secret")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    adapter = get_dbops("oracle").from_role(role)
    cursor = MagicMock()
    cursor.description = [("ID",), ("NAME",)]
    cursor.fetchall.return_value = [(1, "Alice")]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    columns, rows = adapter.fetch_source_rows(
        "SH",
        "CUSTOMER",
        limit=25,
        predicate="ID > 10",
        columns=["ID", "NAME"],
    )

    assert columns == ["ID", "NAME"]
    assert rows == [(1, "Alice")]
    cursor.execute.assert_called_once_with(
        'SELECT "ID", "NAME" FROM "SH"."CUSTOMER" WHERE (ID > 10) ORDER BY "ID", "NAME" FETCH FIRST :limit ROWS ONLY',
        {"limit": 25},
    )
    conn.close.assert_called_once()


def test_oracle_truncate_and_insert_rows_use_parameterized_statements(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_PWD", "secret")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    adapter = get_dbops("oracle").from_role(role)
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    adapter.truncate_table("BRONZE", "CUSTOMER")
    inserted = adapter.insert_rows("BRONZE", "CUSTOMER", ["ID", "NAME"], [(1, "Alice")])

    assert inserted == 1
    assert cursor.execute.call_args_list[0].args == ('TRUNCATE TABLE "BRONZE"."CUSTOMER"',)
    cursor.executemany.assert_called_once_with(
        'INSERT INTO "BRONZE"."CUSTOMER" ("ID", "NAME") VALUES (:1, :2)',
        [(1, "Alice")],
    )
    assert conn.commit.call_count == 2
    assert conn.close.call_count == 2


def test_oracle_fetch_source_rows_uses_explicit_order_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORACLE_PWD", "secret")
    role = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(
            service="TARGETPDB",
            user="system",
            password_env="ORACLE_PWD",
        ),
    )
    adapter = get_dbops("oracle").from_role(role)
    cursor = MagicMock()
    cursor.description = [("ID",), ("DOC",)]
    cursor.fetchall.return_value = [(1, "value")]
    conn = MagicMock()
    conn.cursor.return_value = cursor
    adapter._connect = MagicMock(return_value=conn)  # type: ignore[attr-defined]

    adapter.fetch_source_rows(
        "SH",
        "DOCUMENT",
        limit=25,
        columns=["ID", "DOC"],
        order_by_columns=["ID"],
    )

    cursor.execute.assert_called_once_with(
        'SELECT "ID", "DOC" FROM "SH"."DOCUMENT" ORDER BY "ID" FETCH FIRST :limit ROWS ONLY',
        {"limit": 25},
    )
