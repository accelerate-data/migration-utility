"""Oracle sandbox configuration boundary tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox.oracle import OracleSandbox


class TestOracleConfigModuleBoundary:
    def test_from_env_lives_in_config_module(self) -> None:
        assert (
            OracleSandbox.from_env.__func__.__module__
            == "shared.sandbox.oracle_config"
        )

    def test_from_env_remains_available_from_public_facade(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "sandbox-secret")

        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "source-host",
                            "port": "15210",
                            "service": "SRCPDB",
                            "user": "source_user",
                            "schema": "SH",
                            "password_env": "ORACLE_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "sandbox-host",
                            "port": "15211",
                            "service": "FREE",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )

        assert isinstance(backend, OracleSandbox)
        assert backend.host == "sandbox-host"
        assert backend.port == "15211"
        assert backend.cdb_service == "FREE"
        assert backend.password == "sandbox-secret"
        assert backend.source_host == "source-host"
        assert backend.source_service == "SRCPDB"
        assert backend.source_password == "source-secret"
        assert backend.source_schema == "SH"


class TestOracleConnectionModuleBoundary:
    def test_connection_methods_live_in_connection_module(self) -> None:
        assert (
            OracleSandbox._connect_cdb.__module__
            == "shared.sandbox.oracle_connection"
        )
        assert (
            OracleSandbox._connect_sandbox.__module__
            == "shared.sandbox.oracle_connection"
        )
        assert (
            OracleSandbox._connect_source.__module__
            == "shared.sandbox.oracle_connection"
        )

    def test_connect_sandbox_closes_connection_when_session_setup_fails(self) -> None:
        backend = OracleSandbox(
            host="localhost",
            port="1521",
            cdb_service="FREEPDB1",
            password="pw",
            admin_user="sys",
            source_schema="SH",
        )
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("nls failed")
        conn.cursor.return_value.__enter__.return_value = cursor

        with patch("shared.sandbox.oracle_connection._import_oracledb") as import_mock:
            import_mock.return_value.AUTH_MODE_SYSDBA = object()
            import_mock.return_value.AUTH_MODE_DEFAULT = object()
            import_mock.return_value.connect.return_value = conn

            with pytest.raises(RuntimeError, match="nls failed"):
                with backend._connect_sandbox("SBX_ABC123000000"):
                    pass

        conn.close.assert_called_once()
