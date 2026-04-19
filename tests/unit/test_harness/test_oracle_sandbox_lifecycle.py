"""Oracle sandbox lifecycle tests."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from shared.output_models.sandbox import ErrorEntry, SandboxDownOutput
from shared.sandbox.oracle import OracleSandbox


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


class TestOracleLifecycleFacade:
    def test_public_lifecycle_methods_delegate_to_lifecycle_service(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
        backend._lifecycle = MagicMock()
        backend._lifecycle.sandbox_up.return_value = "up-result"
        backend._lifecycle.sandbox_reset.return_value = "reset-result"
        backend._lifecycle.sandbox_down.return_value = "down-result"
        backend._lifecycle.sandbox_status.return_value = "status-result"

        assert backend.sandbox_up(["SH"]) == "up-result"
        assert backend.sandbox_reset("SBX_000000000001", ["SH"]) == "reset-result"
        assert backend.sandbox_down("SBX_000000000001") == "down-result"
        assert backend.sandbox_status("SBX_000000000001", ["SH"]) == "status-result"

        backend._lifecycle.sandbox_up.assert_called_once_with(["SH"])
        backend._lifecycle.sandbox_reset.assert_called_once_with("SBX_000000000001", ["SH"])
        backend._lifecycle.sandbox_down.assert_called_once_with("SBX_000000000001")
        backend._lifecycle.sandbox_status.assert_called_once_with("SBX_000000000001", ["SH"])


class TestOracleSandboxUpCleanup:
    def test_sandbox_up_calls_sandbox_down_on_failure(self) -> None:
        """sandbox_up cleans up the orphaned PDB when cloning raises."""
        backend = _make_backend(cdb_service="FREEPDB1")

        db_error_cls = type("DatabaseError", (Exception,), {})

        with patch("shared.sandbox.oracle_lifecycle._import_oracledb") as ora_mock, \
             patch.object(
                 backend,
                 "_create_sandbox_pdb",
                 side_effect=db_error_cls("pdb create failed"),
             ), \
             patch.object(backend, "sandbox_down") as mock_down:
            ora_mock.return_value.DatabaseError = db_error_cls
            result = backend.sandbox_up(schemas=["SH"])

        assert result.status == "error"
        mock_down.assert_called_once()
        assert result.sandbox_database == mock_down.call_args.args[0]

    def test_sandbox_reset_recreates_same_pdb_name(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
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
            result = backend.sandbox_reset("SBX_000000000001", schemas=["SH"])

        assert result.status == "ok"
        assert result.sandbox_database == "SBX_000000000001"
        mock_drop.assert_called_once_with("SBX_000000000001")
        mock_create.assert_called_once_with("SBX_000000000001")

    def test_sandbox_reset_validates_source_schema_before_drop(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
        mock_down = MagicMock(return_value=MagicMock(status="ok", errors=[]))

        with patch.object(backend, "sandbox_down", mock_down):
            with pytest.raises(ValueError, match="Unsafe Oracle identifier"):
                backend.sandbox_reset("SBX_000000000001", schemas=["bad.schema"])

        mock_down.assert_not_called()

    def test_sandbox_reset_reports_drop_failure_without_cloning(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
        down_result = SandboxDownOutput(
            sandbox_database="SBX_000000000001",
            status="error",
            errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message="drop failed")],
        )

        with patch.object(backend._lifecycle, "sandbox_down", return_value=down_result), \
             patch.object(backend._lifecycle, "_sandbox_clone_into") as mock_clone:
            result = backend.sandbox_reset("SBX_000000000001", schemas=["SH"])

        assert result.status == "error"
        assert result.errors[0].code == "SANDBOX_RESET_FAILED"
        mock_clone.assert_not_called()


class TestOracleSandboxStatus:
    def test_sandbox_status_existing_pdb_reports_content_counts(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
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
            result = backend.sandbox_status("SBX_000000000001")

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is True
        assert result.tables_count == 2
        assert result.views_count == 1
        assert result.procedures_count == 3

    def test_sandbox_status_existing_empty_pdb_reports_no_content(self) -> None:
        backend = _make_backend(cdb_service="FREEPDB1")
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
            result = backend.sandbox_status("SBX_000000000001")

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is False


class TestCreateSandboxPdb:
    """_create_sandbox_pdb issues CREATE PLUGGABLE DATABASE + OPEN."""

    def test_create_pdb_ddl_statements(self) -> None:
        backend = _make_backend()
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_cursor.fetchone.return_value = ("/opt/oracle/oradata/FREE/FREEPDB1/system01.dbf",)
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb):
            backend._create_sandbox_pdb("SBX_ABC123ABC123")

        executed = [c.args[0] for c in cdb_cursor.execute.call_args_list]
        assert "DBA_DATA_FILES" in executed[0]
        create_stmts = [s for s in executed if "CREATE PLUGGABLE DATABASE" in s]
        assert len(create_stmts) == 1
        assert "SBX_ABC123ABC123" in create_stmts[0]
        assert "ADMIN USER" in create_stmts[0]
        assert "CREATE_FILE_DEST" in create_stmts[0]
        assert "/opt/oracle/oradata/FREE" in create_stmts[0]
        open_stmts = [s for s in executed if "OPEN" in s]
        assert len(open_stmts) == 1
        assert "SBX_ABC123ABC123" in open_stmts[0]

    def test_create_pdb_raises_when_no_datafiles(self) -> None:
        backend = _make_backend()
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_cursor.fetchone.return_value = None
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb), \
             pytest.raises(RuntimeError, match="DBA_DATA_FILES is empty"):
            backend._create_sandbox_pdb("SBX_ABC123ABC123")

    def test_create_pdb_rejects_invalid_name(self) -> None:
        backend = _make_backend()
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            backend._create_sandbox_pdb("bad_name")


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
            backend._drop_sandbox_pdb("SBX_ABC123ABC123")

        executed = [c.args[0] for c in cdb_cursor.execute.call_args_list]
        close_stmts = [s for s in executed if "CLOSE" in s]
        assert len(close_stmts) == 1
        assert "SBX_ABC123ABC123" in close_stmts[0]
        assert '"SBX_ABC123ABC123"' not in close_stmts[0]
        drop_stmts = [s for s in executed if "DROP PLUGGABLE DATABASE" in s]
        assert len(drop_stmts) == 1
        assert "SBX_ABC123ABC123" in drop_stmts[0]
        assert "INCLUDING DATAFILES" in drop_stmts[0]

    def test_drop_pdb_ignores_not_found(self) -> None:
        """Silently ignores ORA-65011 (PDB does not exist)."""
        backend = _make_backend()
        db_error_cls = type("DatabaseError", (Exception,), {})
        ora_err = MagicMock()
        ora_err.code = 65011
        exc = db_error_cls(ora_err)
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_cursor.execute.side_effect = exc
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb), \
             patch("shared.sandbox.oracle_lifecycle_core._import_oracledb") as ora:
            ora.return_value.DatabaseError = db_error_cls
            backend._drop_sandbox_pdb("SBX_ABC123ABC123")

    def test_drop_pdb_propagates_unexpected_error(self) -> None:
        """Propagates errors that are not ORA-65011 or ORA-65020."""
        backend = _make_backend()
        db_error_cls = type("DatabaseError", (Exception,), {})
        ora_err = MagicMock()
        ora_err.code = 604  # ORA-00604: error occurred at recursive SQL level
        exc = db_error_cls(ora_err)
        cdb_conn = MagicMock()
        cdb_cursor = MagicMock()
        cdb_cursor.execute.side_effect = exc
        cdb_conn.cursor.return_value = cdb_cursor

        @contextmanager
        def _fake_cdb():
            yield cdb_conn

        with patch.object(backend, "_connect_cdb", side_effect=_fake_cdb), \
             patch("shared.sandbox.oracle_lifecycle_core._import_oracledb") as ora:
            ora.return_value.DatabaseError = db_error_cls
            with pytest.raises(db_error_cls):
                backend._drop_sandbox_pdb("SBX_ABC123ABC123")

    def test_drop_pdb_rejects_invalid_name(self) -> None:
        backend = _make_backend()
        with pytest.raises(ValueError, match="Invalid Oracle sandbox schema name"):
            backend._drop_sandbox_pdb("bad_name")
