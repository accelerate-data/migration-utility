"""Backend registry, name generation, identifier validation, and from_env tests."""

from __future__ import annotations

import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox import get_backend
from shared.sandbox.base import generate_sandbox_name
from shared.sandbox.oracle import (
    OracleSandbox,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
)
from shared.sandbox.sql_server import (
    SqlServerSandbox,
    _import_pyodbc,
    _validate_identifier,
    _validate_sandbox_db_name,
)


# ── Backend registry ─────────────────────────────────────────────────────────


class TestBackendRegistry:
    def test_sql_server_returns_correct_class(self) -> None:
        cls = get_backend("sql_server")
        assert cls is SqlServerSandbox

    def test_oracle_returns_oracle_sandbox(self) -> None:
        cls = get_backend("oracle")
        assert cls is OracleSandbox

    def test_unknown_technology_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported technology"):
            get_backend("snowflake_streaming")


# ── Sandbox database name generation ─────────────────────────────────────────


class TestSandboxDbNameGeneration:
    def test_name_has_correct_prefix(self) -> None:
        name = generate_sandbox_name()
        assert name.startswith("__test_")

    def test_name_is_unique(self) -> None:
        names = {generate_sandbox_name() for _ in range(10)}
        assert len(names) == 10

    def test_name_passes_validation(self) -> None:
        name = generate_sandbox_name()
        _validate_sandbox_db_name(name)  # should not raise


# ── Identifier validation ────────────────────────────────────────────────────


class TestIdentifierValidation:
    def test_simple_name(self) -> None:
        _validate_identifier("dbo")

    def test_dotted_name(self) -> None:
        _validate_identifier("silver.DimProduct")

    def test_bracketed_name(self) -> None:
        _validate_identifier("[dbo].[Product]")

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("dbo; DROP TABLE x")

    def test_rejects_quote(self) -> None:
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("dbo'--")

    def test_bracketed_hyphen(self) -> None:
        _validate_identifier("[my-table]")

    def test_bracketed_dotted_hyphen(self) -> None:
        _validate_identifier("[dbo].[my-table]")

    def test_bare_hyphen_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("my-table")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Unsafe SQL identifier"):
            _validate_identifier("")


# ── from_env validation (#9) ─────────────────────────────────────────────────


class TestFromEnv:
    def test_from_env_missing_runtime_roles_raises(self) -> None:
        with pytest.raises(ValueError, match="runtime.source"):
            SqlServerSandbox.from_env({})

    def test_from_env_requires_explicit_password_env(self) -> None:
        manifest = {
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "localhost",
                        "port": "1433",
                        "database": "manifestDB",
                        "password_env": "SQL_SOURCE_PASSWORD",
                    },
                },
                "sandbox": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {"host": "localhost", "port": "1433", "user": "sa"},
                },
            }
        }
        with patch.dict(os.environ, {"SQL_SOURCE_PASSWORD": "source-pass"}, clear=True):
            with pytest.raises(ValueError, match="runtime.sandbox.connection.password_env"):
                SqlServerSandbox.from_env(manifest)

    def test_from_env_requires_explicit_source_database(self) -> None:
        manifest = {
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "localhost",
                        "port": "1433",
                        "password_env": "SQL_SOURCE_PASSWORD",
                    },
                },
                "sandbox": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "localhost",
                        "port": "1433",
                        "user": "admin",
                        "password_env": "SQL_SANDBOX_PASSWORD",
                    },
                },
            }
        }
        with patch.dict(os.environ, {"SQL_SOURCE_PASSWORD": "source-pass", "SQL_SANDBOX_PASSWORD": "pass"}, clear=True):
            with pytest.raises(ValueError, match="runtime.source.connection.database"):
                SqlServerSandbox.from_env(manifest)

    def test_from_env_allows_distinct_source_and_sandbox_hosts(self) -> None:
        manifest = {
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "source-host",
                        "port": "1433",
                        "database": "manifestDB",
                        "user": "source_user",
                        "password_env": "SQL_SOURCE_PASSWORD",
                    },
                },
                "sandbox": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "sandbox-host",
                        "port": "1433",
                        "user": "admin",
                        "password_env": "SQL_SANDBOX_PASSWORD",
                    },
                },
            }
        }
        with patch.dict(
            os.environ,
            {"SQL_SOURCE_PASSWORD": "source-pass", "SQL_SANDBOX_PASSWORD": "pass"},
            clear=True,
        ):
            backend = SqlServerSandbox.from_env(manifest)
        assert backend.source_host == "source-host"
        assert backend.host == "sandbox-host"

    def test_from_env_uses_explicit_runtime_roles(self) -> None:
        manifest = {
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "localhost",
                        "port": "1433",
                        "database": "manifestDB",
                        "user": "source_user",
                        "password_env": "SQL_SOURCE_PASSWORD",
                    },
                },
                "sandbox": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "localhost",
                        "port": "1433",
                        "user": "admin",
                        "driver": "FreeTDS",
                        "password_env": "SQL_SANDBOX_PASSWORD",
                    },
                },
            }
        }
        with patch.dict(
            os.environ,
            {"SQL_SOURCE_PASSWORD": "source-pass", "SQL_SANDBOX_PASSWORD": "pass"},
            clear=True,
        ):
            backend = SqlServerSandbox.from_env(manifest)
        assert backend.user == "admin"
        assert backend.driver == "FreeTDS"
        assert backend.source_user == "source_user"

    def test_connect_cant_open_lib_raises_runtime_error(self) -> None:
        backend = SqlServerSandbox(
            host="localhost", port="1433", password="pass",
        )
        with patch("shared.sandbox.sql_server._pyodbc") as mock_pyodbc:
            mock_pyodbc.Error = type("Error", (Exception,), {})
            mock_pyodbc.connect.side_effect = mock_pyodbc.Error(
                "[unixODBC][Driver Manager]Can't open lib 'FreeTDS'"
            )
            with pytest.raises(RuntimeError, match="brew install freetds"):
                with backend._connect():
                    pass
