"""Oracle-specific validation, sandbox, and lifecycle tests."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from shared.output_models.sandbox import ErrorEntry, SandboxDownOutput
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
        assert backend.cdb_service == "FREEPDB1"
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
        assert backend.cdb_service == "SANDBOXPDB"


# ── Oracle connection lifecycle ──────────────────────────────────────────────


class TestOracleConnectionLifecycle:
    def test_connect_sandbox_closes_connection_when_session_setup_fails(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        conn = MagicMock()
        cursor = MagicMock()
        cursor.execute.side_effect = RuntimeError("nls failed")
        conn.cursor.return_value.__enter__.return_value = cursor

        with patch("shared.sandbox.oracle_services._import_oracledb") as import_mock:
            import_mock.return_value.AUTH_MODE_SYSDBA = object()
            import_mock.return_value.AUTH_MODE_DEFAULT = object()
            import_mock.return_value.connect.return_value = conn

            with pytest.raises(RuntimeError, match="nls failed"):
                with backend._connect_sandbox("__test_abc123"):
                    pass

        conn.close.assert_called_once()


class TestOracleSandboxUpCleanup:
    def test_public_lifecycle_methods_delegate_to_lifecycle_service(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        backend._lifecycle = MagicMock()
        backend._lifecycle.sandbox_up.return_value = "up-result"
        backend._lifecycle.sandbox_reset.return_value = "reset-result"
        backend._lifecycle.sandbox_down.return_value = "down-result"
        backend._lifecycle.sandbox_status.return_value = "status-result"

        assert backend.sandbox_up(["SH"]) == "up-result"
        assert backend.sandbox_reset("__test_existing", ["SH"]) == "reset-result"
        assert backend.sandbox_down("__test_existing") == "down-result"
        assert backend.sandbox_status("__test_existing", ["SH"]) == "status-result"

        backend._lifecycle.sandbox_up.assert_called_once_with(["SH"])
        backend._lifecycle.sandbox_reset.assert_called_once_with("__test_existing", ["SH"])
        backend._lifecycle.sandbox_down.assert_called_once_with("__test_existing")
        backend._lifecycle.sandbox_status.assert_called_once_with("__test_existing", ["SH"])

    def test_public_execution_methods_delegate_to_execution_service(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        backend._execution = MagicMock()
        backend._execution.execute_scenario.return_value = "scenario-result"
        backend._execution.execute_select.return_value = "select-result"
        backend._comparison = MagicMock()
        backend._comparison.compare_two_sql.return_value = "compare-result"

        scenario = {
            "name": "case",
            "target_table": "CHANNELS",
            "procedure": "LOAD_CHANNELS",
            "given": [],
        }
        fixtures: list[dict[str, object]] = []

        assert backend.execute_scenario("__test_existing", scenario) == "scenario-result"
        assert backend.execute_select("__test_existing", "SELECT 1 FROM dual", fixtures) == "select-result"
        assert backend.compare_two_sql(
            "__test_existing", "SELECT 1 FROM dual", "SELECT 1 FROM dual", fixtures,
        ) == "compare-result"

        backend._execution.execute_scenario.assert_called_once_with(
            "__test_existing", scenario,
        )
        backend._execution.execute_select.assert_called_once_with(
            "__test_existing", "SELECT 1 FROM dual", fixtures,
        )
        backend._comparison.compare_two_sql.assert_called_once_with(
            "__test_existing", "SELECT 1 FROM dual", "SELECT 1 FROM dual", fixtures,
        )

    def test_sandbox_up_calls_sandbox_down_on_failure(self) -> None:
        """sandbox_up cleans up the orphaned PDB when cloning raises."""
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )

        db_error_cls = type("DatabaseError", (Exception,), {})

        with patch("shared.sandbox.oracle_lifecycle._import_oracledb") as ora_mock, \
             patch.object(
                 backend, "_create_sandbox_pdb",
                 side_effect=db_error_cls("pdb create failed"),
             ), \
             patch.object(backend, "sandbox_down") as mock_down:
            ora_mock.return_value.DatabaseError = db_error_cls
            result = backend.sandbox_up(schemas=["SH"])

        assert result.status == "error"
        mock_down.assert_called_once()
        assert result.sandbox_database == mock_down.call_args.args[0]

    def test_sandbox_reset_recreates_same_pdb_name(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (1,)
        sandbox_conn = MagicMock()
        sandbox_conn.cursor.return_value = sandbox_cursor
        source_cursor = MagicMock()
        source_conn = MagicMock()
        source_conn.cursor.return_value = source_cursor

        @contextmanager
        def _fake_sandbox_connect(name: str) -> object:
            yield sandbox_conn

        @contextmanager
        def _fake_source_connect() -> object:
            yield source_conn

        with patch.object(backend, "_drop_sandbox_pdb") as mock_drop, \
             patch.object(backend, "_create_sandbox_pdb") as mock_create, \
             patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox_connect), \
             patch.object(backend, "_connect_source", side_effect=_fake_source_connect), \
             patch.object(backend, "_clone_tables", return_value=(["CUSTOMERS"], [])), \
             patch.object(backend, "_clone_views", return_value=([], [])), \
             patch.object(backend, "_clone_procedures", return_value=(["LOAD_CUSTOMERS"], [])):
            result = backend.sandbox_reset("__test_existing", schemas=["SH"])

        assert result.status == "ok"
        assert result.sandbox_database == "__test_existing"
        mock_drop.assert_called_once_with("__test_existing")
        mock_create.assert_called_once_with("__test_existing")

    def test_sandbox_reset_validates_source_schema_before_drop(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        mock_down = MagicMock(return_value=MagicMock(status="ok", errors=[]))

        with patch.object(backend, "sandbox_down", mock_down):
            with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
                backend.sandbox_reset("__test_existing", schemas=["bad.schema"])

        mock_down.assert_not_called()

    def test_sandbox_reset_reports_drop_failure_without_cloning(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        down_result = SandboxDownOutput(
            sandbox_database="__test_existing",
            status="error",
            errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message="drop failed")],
        )

        with patch.object(backend._lifecycle, "sandbox_down", return_value=down_result), \
             patch.object(backend._lifecycle, "_sandbox_clone_into") as mock_clone:
            result = backend.sandbox_reset("__test_existing", schemas=["SH"])

        assert result.status == "error"
        assert result.errors[0].code == "SANDBOX_RESET_FAILED"
        mock_clone.assert_not_called()


class TestOracleSandboxStatus:
    def test_sandbox_status_existing_pdb_reports_content_counts(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cdb_cursor = MagicMock()
        cdb_cursor.fetchone.return_value = (1,)  # V$PDBS exists
        cdb_conn = MagicMock()
        cdb_conn.cursor.return_value = cdb_cursor

        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.side_effect = [(2,), (1,), (3,)]
        sandbox_conn = MagicMock()
        sandbox_conn.cursor.return_value = sandbox_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        @contextmanager
        def _fake_sandbox(name: str):
            yield sandbox_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb), \
             patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox):
            result = backend.sandbox_status("__test_existing")

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is True
        assert result.tables_count == 2
        assert result.views_count == 1
        assert result.procedures_count == 3

    def test_sandbox_status_existing_empty_pdb_reports_no_content(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cdb_cursor = MagicMock()
        cdb_cursor.fetchone.return_value = (1,)  # V$PDBS exists
        cdb_conn = MagicMock()
        cdb_conn.cursor.return_value = cdb_cursor

        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.side_effect = [(0,), (0,), (0,)]
        sandbox_conn = MagicMock()
        sandbox_conn.cursor.return_value = sandbox_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        @contextmanager
        def _fake_sandbox(name: str):
            yield sandbox_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb), \
             patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox):
            result = backend.sandbox_status("__test_existing")

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is False


class TestExecuteScenarioOracle:
    def test_execute_scenario_quotes_procedure_name(self) -> None:
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cursor = MagicMock()
        cursor.description = [("ID",)]
        cursor.fetchall.return_value = [(1,)]
        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_sandbox(name: str):
            yield conn

        with patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox), \
             patch.object(backend._fixtures, "ensure_view_tables", return_value=[]), \
             patch.object(backend._fixtures, "seed_fixtures"):
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
            host="localhost", port="1521", cdb_service="FREEPDB1",
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


# ── Phase 1: PDB lifecycle on _OracleSandboxCore ─────────────────────────────


def _make_backend(**overrides: object) -> OracleSandbox:
    """Build an OracleSandbox with sensible defaults for unit tests."""
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


class TestCdbServiceRename:
    """self.service is renamed to self.cdb_service."""

    def test_cdb_service_stored(self) -> None:
        backend = _make_backend()
        assert backend.cdb_service == "FREE"

    def test_no_service_attribute(self) -> None:
        backend = _make_backend()
        assert not hasattr(backend, "service")

    def test_from_env_stores_cdb_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
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
                            "service": "FREE",
                            "user": "sys",
                            "password_env": "ORACLE_SANDBOX_PASSWORD",
                        },
                    },
                }
            }
        )
        assert backend.cdb_service == "FREE"
        assert not hasattr(backend, "service")


class TestConnectCdb:
    """_connect_cdb() connects to {host}:{port}/{cdb_service} as SYSDBA."""

    def test_connect_cdb_uses_cdb_service(self) -> None:
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_services._import_oracledb") as ora:
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
        """CDB connection is for DDL only — no NLS session setup."""
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_services._import_oracledb") as ora:
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

        with patch("shared.sandbox.oracle_services._import_oracledb") as ora:
            sysdba = object()
            ora.return_value.AUTH_MODE_SYSDBA = sysdba
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_sandbox("__test_abc123") as c:
                assert c is conn

        ora.return_value.connect.assert_called_once_with(
            user="sys",
            password="pw",
            dsn="localhost:1521/__test_abc123",
            mode=sysdba,
        )

    def test_connect_sandbox_sets_nls_formats(self) -> None:
        backend = _make_backend()
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("shared.sandbox.oracle_services._import_oracledb") as ora:
            ora.return_value.AUTH_MODE_SYSDBA = object()
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_sandbox("__test_abc123"):
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

        with patch("shared.sandbox.oracle_services._import_oracledb") as ora:
            ora.return_value.AUTH_MODE_SYSDBA = object()
            ora.return_value.AUTH_MODE_DEFAULT = object()
            ora.return_value.connect.return_value = conn

            with backend._connect_sandbox("__test_abc123"):
                pass

        conn.close.assert_called_once()


class TestCreateSandboxPdb:
    """_create_sandbox_pdb issues CREATE PLUGGABLE DATABASE + OPEN."""

    def test_create_pdb_ddl_statements(self) -> None:
        backend = _make_backend()
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb):
            backend._create_sandbox_pdb("__test_abc123")

        executed = [c.args[0] for c in cdb_cursor.execute.call_args_list]
        # Must have CREATE PLUGGABLE DATABASE
        create_stmts = [s for s in executed if "CREATE PLUGGABLE DATABASE" in s]
        assert len(create_stmts) == 1
        assert '"__test_abc123"' in create_stmts[0]
        assert "ADMIN USER" in create_stmts[0]
        # Must have ALTER ... OPEN
        open_stmts = [s for s in executed if "OPEN" in s]
        assert len(open_stmts) == 1
        assert '"__test_abc123"' in open_stmts[0]


class TestDropSandboxPdb:
    """_drop_sandbox_pdb issues CLOSE + DROP PLUGGABLE DATABASE."""

    def test_drop_pdb_ddl_statements(self) -> None:
        backend = _make_backend()
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb):
            backend._drop_sandbox_pdb("__test_abc123")

        executed = [c.args[0] for c in cdb_cursor.execute.call_args_list]
        close_stmts = [s for s in executed if "CLOSE" in s]
        assert len(close_stmts) == 1
        assert '"__test_abc123"' in close_stmts[0]
        drop_stmts = [s for s in executed if "DROP PLUGGABLE DATABASE" in s]
        assert len(drop_stmts) == 1
        assert '"__test_abc123"' in drop_stmts[0]
        assert "INCLUDING DATAFILES" in drop_stmts[0]

    def test_drop_pdb_ignores_not_found(self) -> None:
        """Silently ignores errors if PDB doesn't exist."""
        backend = _make_backend()
        db_error_cls = type("DatabaseError", (Exception,), {})
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_cursor.execute.side_effect = db_error_cls("PDB does not exist")
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb), \
             patch("shared.sandbox.oracle_services._import_oracledb") as ora:
            ora.return_value.DatabaseError = db_error_cls
            # Should not raise
            backend._drop_sandbox_pdb("__test_abc123")
