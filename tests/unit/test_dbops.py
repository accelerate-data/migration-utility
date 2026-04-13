"""Tests for DB-ops technology adapters."""

from __future__ import annotations

import os
from pathlib import Path

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
            database="MigrationTest",
        ),
    )
    adapter = get_dbops("sql_server").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["MSSQL_DB"] == "MigrationTest"
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
        ),
    )
    adapter = get_dbops("oracle").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env["ORACLE_SERVICE"] == "SANDBOXPDB"
    assert env["ORACLE_SCHEMA"] == "BRONZE"
    assert env["ORACLE_PWD"] == "secret"


def test_duckdb_dbops_materialize_env() -> None:
    role = RuntimeRole(
        technology="duckdb",
        dialect="duckdb",
        connection=RuntimeConnection(path=".runtime/duckdb/source.duckdb"),
    )
    adapter = get_dbops("duckdb").from_role(role)
    env = adapter.materialize_migration_test_env()
    assert env == {"DUCKDB_PATH": ".runtime/duckdb/source.duckdb"}


def test_dbops_fixture_script_paths_are_repo_relative() -> None:
    role = RuntimeRole(
        technology="duckdb",
        dialect="duckdb",
        connection=RuntimeConnection(path="target.duckdb"),
    )
    adapter = get_dbops("duckdb").from_role(role)
    assert adapter.fixture_script_path(Path("/repo")) == Path("/repo/scripts/sql/duckdb/materialize-migration-test.sh")


def test_get_dbops_rejects_unknown_technology() -> None:
    with pytest.raises(ValueError, match="Unsupported DB operations technology"):
        get_dbops("postgres")
