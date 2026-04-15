"""Unit tests for shared.db_connect connection factories."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


class TestSqlServerConnect:
    """Tests for sql_server_connect()."""

    def test_default_driver_is_freetds(self) -> None:
        env = {"SOURCE_MSSQL_HOST": "localhost", "SOURCE_MSSQL_PASSWORD": "pass"}
        mock_pyodbc = MagicMock()
        with patch.dict(os.environ, env, clear=True):
            with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
                from shared.db_connect import sql_server_connect
                sql_server_connect("testdb")
        conn_str = mock_pyodbc.connect.call_args[0][0]
        assert "DRIVER={FreeTDS};" in conn_str

    def test_cant_open_lib_raises_runtime_error(self) -> None:
        env = {"SOURCE_MSSQL_HOST": "localhost", "SOURCE_MSSQL_PASSWORD": "pass"}
        mock_pyodbc = MagicMock()
        mock_pyodbc.Error = type("Error", (Exception,), {})
        mock_pyodbc.connect.side_effect = mock_pyodbc.Error(
            "[01000] [unixODBC][Driver Manager]Can't open lib 'FreeTDS'"
        )
        with patch.dict(os.environ, env, clear=True):
            with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
                from importlib import reload
                import shared.db_connect as mod
                reload(mod)
                with pytest.raises(RuntimeError, match="brew install freetds"):
                    mod.sql_server_connect("testdb")

    def test_other_pyodbc_errors_propagate(self) -> None:
        env = {"SOURCE_MSSQL_HOST": "localhost", "SOURCE_MSSQL_PASSWORD": "pass"}
        mock_pyodbc = MagicMock()
        mock_pyodbc.Error = type("Error", (Exception,), {})
        mock_pyodbc.connect.side_effect = mock_pyodbc.Error("Login failed")
        with patch.dict(os.environ, env, clear=True):
            with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
                from importlib import reload
                import shared.db_connect as mod
                reload(mod)
                with pytest.raises(mock_pyodbc.Error, match="Login failed"):
                    mod.sql_server_connect("testdb")

    def test_password_is_odbc_escaped_in_connection_string(self) -> None:
        env = {"SOURCE_MSSQL_HOST": "localhost", "SOURCE_MSSQL_PASSWORD": "pa;ss}word"}
        mock_pyodbc = MagicMock()
        with patch.dict(os.environ, env, clear=True):
            with patch.dict("sys.modules", {"pyodbc": mock_pyodbc}):
                from importlib import reload
                import shared.db_connect as mod

                reload(mod)
                mod.sql_server_connect("testdb")

        conn_str = mock_pyodbc.connect.call_args[0][0]
        assert "PWD={pa;ss}}word};" in conn_str
