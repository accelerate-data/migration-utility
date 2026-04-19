"""SQL Server sandbox configuration boundary tests."""

from __future__ import annotations

import pytest

from shared.sandbox.sql_server import SqlServerSandbox


class TestSqlServerConfigModuleBoundary:
    def test_from_env_lives_in_config_module(self) -> None:
        assert (
            SqlServerSandbox.from_env.__func__.__module__
            == "shared.sandbox.sql_server_config"
        )

    def test_from_env_remains_available_from_public_facade(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("SQLSERVER_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("SQLSERVER_SANDBOX_PASSWORD", "sandbox-secret")

        backend = SqlServerSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "sql_server",
                        "dialect": "tsql",
                        "connection": {
                            "host": "source-host",
                            "port": "14330",
                            "database": "SourceDb",
                            "user": "source_user",
                            "password_env": "SQLSERVER_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "sql_server",
                        "dialect": "tsql",
                        "connection": {
                            "host": "sandbox-host",
                            "port": "14331",
                            "user": "sandbox_user",
                            "password_env": "SQLSERVER_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )

        assert isinstance(backend, SqlServerSandbox)
        assert backend.host == "sandbox-host"
        assert backend.port == "14331"
        assert backend.password == "sandbox-secret"
        assert backend.source_host == "source-host"
        assert backend.source_database == "SourceDb"
        assert backend.source_password == "source-secret"


class TestSqlServerConnectionModuleBoundary:
    def test_connection_methods_live_in_connection_module(self) -> None:
        assert (
            SqlServerSandbox._connect.__module__
            == "shared.sandbox.sql_server_connection"
        )
        assert (
            SqlServerSandbox._connect_source.__module__
            == "shared.sandbox.sql_server_connection"
        )
