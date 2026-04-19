"""Oracle sandbox connection context-manager tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox.oracle import OracleSandbox


def _make_backend(**overrides: object) -> OracleSandbox:
    defaults = dict(
        host="localhost",
        port="1521",
        cdb_service="FREE",
        password="pw",
        admin_user="sys",
        source_schema="SH",
        source_service="FREEPDB1",
    )
    defaults.update(overrides)
    return OracleSandbox(**defaults)


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


class TestOracleConnectionLifecycle:
    def test_connect_sandbox_closes_connection_when_session_setup_fails(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
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


class TestConnectCdb:
    """_connect_cdb() connects to {host}:{port}/{cdb_service} as SYSDBA."""

    def test_connect_cdb_uses_cdb_service(self) -> None:
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_connection._import_oracledb") as ora:
            sysdba = object()
            ora.return_value.AUTH_MODE_SYSDBA = sysdba
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_cdb() as c:
                assert c is conn

        ora.return_value.connect.assert_called_once_with(
            user="sys",
            password="pw",
            dsn="localhost:1521/FREE",
            mode=sysdba,
        )
        conn.close.assert_called_once()

    def test_connect_cdb_does_not_set_nls(self) -> None:
        """CDB connection is for DDL only - no NLS session setup."""
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_connection._import_oracledb") as ora:
            ora.return_value.AUTH_MODE_SYSDBA = object()
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_cdb():
                pass

        cursor.execute.assert_not_called()


class TestConnectSandbox:
    """_connect_sandbox(name) connects to {host}:{port}/{sandbox_name}."""

    def test_connect_sandbox_uses_pdb_name_as_service(self) -> None:
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_connection._import_oracledb") as ora:
            sysdba = object()
            ora.return_value.AUTH_MODE_SYSDBA = sysdba
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_sandbox("SBX_ABC123000000") as c:
                assert c is conn

        ora.return_value.connect.assert_called_once_with(
            user="sys",
            password="pw",
            dsn="localhost:1521/SBX_ABC123000000",
            mode=sysdba,
        )

    def test_connect_sandbox_sets_nls_formats(self) -> None:
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_connection._import_oracledb") as ora:
            ora.return_value.AUTH_MODE_SYSDBA = object()
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_sandbox("SBX_ABC123000000"):
                pass

        nls_calls = [c.args[0] for c in cursor.execute.call_args_list]
        assert "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'" in nls_calls
        assert "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'" in nls_calls

    def test_connect_sandbox_closes_on_exit(self) -> None:
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_connection._import_oracledb") as ora:
            ora.return_value.AUTH_MODE_SYSDBA = object()
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_sandbox("SBX_ABC123000000"):
                pass

        conn.close.assert_called_once()


class TestConnectSource:
    def test_connect_source_uses_source_connection_settings(self) -> None:
        backend = _make_backend(
            source_host="source-host",
            source_port="15210",
            source_service="SRCPDB",
            source_user="sh",
            source_password="source-pw",
        )
        conn = MagicMock()

        with patch("shared.sandbox.oracle_connection._import_oracledb") as ora:
            ora.return_value.connect.return_value = conn

            with backend._connect_source() as actual:
                assert actual is conn

        ora.return_value.connect.assert_called_once_with(
            user="sh",
            password="source-pw",
            dsn="source-host:15210/SRCPDB",
        )
        conn.close.assert_called_once()
