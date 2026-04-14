"""Oracle-specific validation, sandbox, and lifecycle tests."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox.base import generate_sandbox_name
from shared.sandbox.oracle import (
    OracleSandbox,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
)


# ── Oracle identifier validation ──────────────────────────────────────────────


class TestOracleIdentifierValidation:
    def test_simple_name(self) -> None:
        _validate_oracle_identifier("CHANNELS")  # should not raise

    def test_underscore_prefix(self) -> None:
        _validate_oracle_identifier("_MY_TABLE")

    def test_dollar_sign(self) -> None:
        _validate_oracle_identifier("SYS$TABLE")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("")

    def test_rejects_semicolon(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("TABLE; DROP TABLE CHANNELS--")

    def test_rejects_single_quote(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("O'REILLY")

    def test_rejects_double_quote(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier('"quoted"')

    def test_rejects_over_128_chars(self) -> None:
        long_name = "A" * 129
        with pytest.raises(ValueError, match="exceeds 128 chars"):
            _validate_oracle_identifier(long_name)

    def test_accepts_128_chars(self) -> None:
        name = "A" * 128
        _validate_oracle_identifier(name)  # should not raise

    def test_rejects_dot(self) -> None:
        with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
            _validate_oracle_identifier("SH.CHANNELS")


# ── Oracle sandbox name generation + validation ───────────────────────────────


class TestOracleSandboxName:
    def test_name_has_correct_prefix(self) -> None:
        name = generate_sandbox_name()
        assert name.startswith("__test_")

    def test_name_is_unique(self) -> None:
        names = {generate_sandbox_name() for _ in range(10)}
        assert len(names) == 10

    def test_name_passes_validation(self) -> None:
        name = generate_sandbox_name()
        _validate_oracle_sandbox_name(name)  # should not raise

    def test_name_hex_length(self) -> None:
        name = generate_sandbox_name()
        hex_part = name[len("__test_"):]
        assert len(hex_part) == 12
        assert all(c in "0123456789abcdef" for c in hex_part)

    def test_rejects_name_without_prefix(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("myschema")

    def test_rejects_name_with_special_chars(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("__test_abc; DROP")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            _validate_oracle_sandbox_name("")


# ── OracleSandbox.from_env ────────────────────────────────────────────────────


class TestOracleSandboxFromEnv:
    def test_raises_when_oracle_pwd_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ORACLE_SANDBOX_PASSWORD", raising=False)
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        with pytest.raises(ValueError, match="runtime.sandbox.connection.password_env"):
            OracleSandbox.from_env(
                {
                    "runtime": {
                        "source": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sh",
                                "schema": "SH",
                                "password_env": "ORACLE_SOURCE_PASSWORD",
                            },
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sys",
                            },
                        },
                    }
                }
            )

    def test_raises_when_source_schema_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        with pytest.raises(ValueError, match="runtime.source.connection.schema"):
            OracleSandbox.from_env(
                {
                    "runtime": {
                        "source": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sh",
                                "password_env": "ORACLE_SOURCE_PASSWORD",
                            },
                        },
                        "sandbox": {
                            "technology": "oracle",
                            "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sys",
                                "password_env": "ORACLE_SANDBOX_PASSWORD",
                            },
                        },
                    }
                }
            )

    def test_uses_explicit_runtime_roles(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                            "connection": {
                                "host": "localhost",
                                "port": "1521",
                                "service": "FREEPDB1",
                                "user": "sh",
                                "schema": "SH",
                                "password_env": "ORACLE_SOURCE_PASSWORD",
                            },
                        },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "FREEPDB1",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )
        assert backend.source_schema == "SH"
        assert backend.service == "FREEPDB1"
        assert backend.admin_user == "sys"
        assert backend.source_user == "sh"

    def test_allows_distinct_source_and_sandbox_services(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ORACLE_SOURCE_PASSWORD", "source-secret")
        monkeypatch.setenv("ORACLE_SANDBOX_PASSWORD", "secret")
        backend = OracleSandbox.from_env(
            {
                "runtime": {
                    "source": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "SRCPDB",
                            "user": "sh",
                            "schema": "SH",
                            "password_env": "ORACLE_SOURCE_PASSWORD",
                        },
                    },
                    "sandbox": {
                        "technology": "oracle",
                        "dialect": "oracle",
                        "connection": {
                            "host": "localhost",
                            "port": "1521",
                            "service": "SANDBOXPDB",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )
        assert backend.source_service == "SRCPDB"
        assert backend.service == "SANDBOXPDB"


# ── Oracle connection lifecycle ──────────────────────────────────────────────


class TestOracleConnectionLifecycle:
    def test_connect_closes_connection_when_session_setup_fails(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("nls failed")
        conn.cursor.return_value.__enter__.return_value = cursor

        with patch("shared.sandbox.oracle._import_oracledb") as import_mock:
            import_mock.return_value.AUTH_MODE_SYSDBA = object()
            import_mock.return_value.AUTH_MODE_DEFAULT = object()
            import_mock.return_value.connect.return_value = conn

            with pytest.raises(RuntimeError, match="nls failed"):
                with backend._connect():
                    pass

        conn.close.assert_called_once()


class TestOracleSandboxUpCleanup:
    def test_sandbox_up_calls_sandbox_down_on_failure(self) -> None:
        """sandbox_up cleans up the orphaned schema when cloning raises."""
        backend = OracleSandbox(
            host="localhost", port="1521", service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )

        db_error_cls = type("DatabaseError", (Exception,), {})

        @contextmanager
        def _fail_connect():
            raise db_error_cls("connection failed")
            yield  # noqa: unreachable — keeps it a generator

        with patch("shared.sandbox.oracle._import_oracledb") as ora_mock, \
             patch.object(backend, "_connect", side_effect=_fail_connect), \
             patch.object(backend, "_connect_source", side_effect=_fail_connect), \
             patch.object(backend, "sandbox_down") as mock_down:
            ora_mock.return_value.DatabaseError = db_error_cls
            result = backend.sandbox_up(schemas=["SH"])

        assert result.status == "error"
        mock_down.assert_called_once()
        assert result.sandbox_database == mock_down.call_args.args[0]


class TestExecuteScenarioOracle:
    def test_execute_scenario_quotes_procedure_name(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cursor = MagicMock()
        cursor.description = [("ID",)]
        cursor.fetchall.return_value = [(1,)]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_connect():
            yield conn

        with patch.object(backend, "_connect", side_effect=_fake_connect), \
             patch.object(backend, "_ensure_view_tables", return_value=[]), \
             patch.object(backend, "_seed_fixtures"):
            result = backend.execute_scenario(
                sandbox_db="__test_abc123",
                scenario={
                    "name": "quoted_proc",
                    "procedure": "Proc$Load",
                    "target_table": "CHANNELS",
                    "given": [],
                },
            )

        assert result.status == "ok"
        execute_calls = [call.args[0] for call in cursor.execute.call_args_list]
        assert 'BEGIN "__test_abc123"."Proc$Load"; END;' in execute_calls


class TestCompareTwoSqlOracle:
    """Unit tests for OracleSandbox.compare_two_sql."""

    def test_invalid_sql_returns_syntax_error(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )

        result = backend.compare_two_sql(
            sandbox_db="__test_abc123",
            sql_a='SELECT ( FROM "SH"."CHANNELS"',
            sql_b='SELECT "CHANNEL_ID" FROM "SH"."CHANNELS"',
            fixtures=[],
        )

        assert result["status"] == "error"
        assert result["errors"][0]["code"] == "SQL_SYNTAX_ERROR"
