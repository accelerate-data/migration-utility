"""SQL Server sandbox lifecycle tests."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from shared.output_models.sandbox import ErrorEntry, SandboxDownOutput
from shared.sandbox.sql_server import _import_pyodbc

from .conftest import _make_backend, _mock_connect_factory


class TestSqlServerLifecycleFacade:
    def test_public_lifecycle_methods_delegate_to_lifecycle_service(self) -> None:
        backend = _make_backend()
        backend._lifecycle = MagicMock()
        backend._lifecycle.sandbox_up.return_value = "up-result"
        backend._lifecycle.sandbox_reset.return_value = "reset-result"
        backend._lifecycle.sandbox_down.return_value = "down-result"
        backend._lifecycle.sandbox_status.return_value = "status-result"

        assert backend.sandbox_up(["dbo"]) == "up-result"
        assert backend.sandbox_reset("SBX_000000000001", ["dbo"]) == "reset-result"
        assert backend.sandbox_down("SBX_000000000001") == "down-result"
        assert backend.sandbox_status("SBX_000000000001", ["dbo"]) == "status-result"

        backend._lifecycle.sandbox_up.assert_called_once_with(["dbo"])
        backend._lifecycle.sandbox_reset.assert_called_once_with(
            "SBX_000000000001", ["dbo"],
        )
        backend._lifecycle.sandbox_down.assert_called_once_with("SBX_000000000001")
        backend._lifecycle.sandbox_status.assert_called_once_with(
            "SBX_000000000001", ["dbo"],
        )


class TestSqlServerSandboxUp:
    """Test sandbox_up generates correct SQL via mocked _connect."""

    def test_sandbox_up_creates_database(self) -> None:
        backend = _make_backend()

        source_cursor = MagicMock()
        source_cursor.fetchall.side_effect = [
            [("dbo", "Product"), ("silver", "DimProduct")],
            [],
            [("ProductID", "int", None, 10, 0, None, "NO")],
            [],
            [("DimProductID", "int", None, 10, 0, None, "NO")],
            [("silver", "vw_customer")],
            [("CREATE VIEW [silver].[vw_customer] AS SELECT 1 AS id",)],
            [("dbo", "usp_load", "CREATE PROCEDURE dbo.usp_load AS BEGIN SELECT 1 END")],
        ]

        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchall.side_effect = [
            [(0,)],  # _create_schemas: schema "dbo" does not exist
            [(0,)],  # _create_schemas: schema "silver" does not exist
        ]
        default_cursor = MagicMock()
        default_cursor.fetchall.return_value = [(None,)]  # DB_ID check: does not exist

        admin_connect = _mock_connect_factory(
            source_cursor=source_cursor,
            sandbox_cursor=sandbox_cursor,
            default_cursor=default_cursor,
        )
        source_connect = _mock_connect_factory(source_cursor=source_cursor)

        with patch.object(backend, "_connect", side_effect=admin_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect,
        ):
            result = backend.sandbox_up(
                schemas=["dbo", "silver"],
            )

        assert result.status in ("ok", "partial")
        assert result.sandbox_database.startswith("SBX_")
        assert result.tables_cloned == ["dbo.Product", "silver.DimProduct"]
        assert result.views_cloned == ["silver.vw_customer"]
        assert result.procedures_cloned == ["dbo.usp_load"]
        assert not hasattr(result, "run_id")

        calls = [str(c) for c in default_cursor.execute.call_args_list]
        create_db_calls = [c for c in calls if "CREATE DATABASE" in c]
        assert len(create_db_calls) == 1

    def test_sandbox_reset_recreates_same_database_name(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchall.return_value = [(1,)]
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchall.return_value = [(0,)]  # _create_schemas: schema does not exist
        source_cursor = MagicMock()

        fake_connect = _mock_connect_factory(
            source_cursor=source_cursor,
            sandbox_cursor=sandbox_cursor,
            default_cursor=default_cursor,
        )
        source_connect = _mock_connect_factory(source_cursor=source_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect), \
             patch.object(backend, "_connect_source", side_effect=source_connect), \
             patch.object(backend, "_clone_tables", return_value=(["dbo.Customer"], [])), \
             patch.object(backend, "_clone_views", return_value=([], [])), \
             patch.object(backend, "_clone_procedures", return_value=(["dbo.usp_load"], [])):
            result = backend.sandbox_reset("SBX_000000000001", schemas=["dbo"])

        assert result.status == "ok"
        assert result.sandbox_database == "SBX_000000000001"
        execute_calls = [call.args[0] for call in default_cursor.execute.call_args_list]
        assert any("DROP DATABASE [SBX_000000000001]" in sql for sql in execute_calls)
        assert any("CREATE DATABASE [SBX_000000000001]" in sql for sql in execute_calls)

    def test_sandbox_reset_reports_drop_failure_without_cloning(self) -> None:
        backend = _make_backend()
        down_result = SandboxDownOutput(
            sandbox_database="SBX_000000000001",
            status="error",
            errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message="drop failed")],
        )

        with patch.object(backend, "sandbox_down", return_value=down_result), \
             patch.object(backend._lifecycle, "_sandbox_clone_into") as mock_clone:
            result = backend.sandbox_reset("SBX_000000000001", schemas=["dbo"])

        assert result.status == "error"
        assert result.errors[0].code == "SANDBOX_RESET_FAILED"
        mock_clone.assert_not_called()

    def test_sandbox_up_calls_sandbox_down_on_failure(self) -> None:
        """sandbox_up cleans up the orphaned DB when cloning raises."""
        backend = _make_backend()

        pyodbc = _import_pyodbc()
        default_cursor = MagicMock()

        @contextmanager
        def _admin_connect(*, database=None):
            if database and database.startswith("SBX_"):
                raise pyodbc.Error("connection failed to sandbox db")
            yield MagicMock(cursor=MagicMock(return_value=default_cursor))

        @contextmanager
        def _source_connect(*, database=None):
            yield MagicMock()

        with patch.object(backend, "_connect", side_effect=_admin_connect), \
             patch.object(backend, "_connect_source", side_effect=_source_connect), \
             patch.object(backend, "sandbox_down") as mock_down:
            result = backend.sandbox_up(schemas=["dbo"])

        assert result.status == "error"
        mock_down.assert_called_once()
        assert result.sandbox_database == mock_down.call_args.args[0]


class TestSqlServerSandboxStatus:
    """Test sandbox_status checks database existence via mocked _connect."""

    def test_sandbox_status_exists(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchall.side_effect = [
            [(1,)],  # DB_ID returns non-None
            [(2,)],
            [(1,)],
            [(3,)],
        ]

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="SBX_ABC123000000")

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is True
        assert result.tables_count == 2
        assert result.views_count == 1
        assert result.procedures_count == 3
        assert not hasattr(result, "run_id")

    def test_sandbox_status_existing_empty_reports_no_content(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchall.side_effect = [
            [(1,)],  # DB_ID returns non-None
            [(0,)],
            [(0,)],
            [(0,)],
        ]

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="SBX_ABC123000000", schemas=["silver"])

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is False
        calls = default_cursor.execute.call_args_list
        assert calls[1].args[1:] == ("silver",)

    def test_sandbox_status_not_found(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchall.return_value = [(None,)]  # DB_ID returns None

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="SBX_ABC123000000")

        assert result.status == "not_found"
        assert result.exists is False
        assert result.has_content is False
        assert result.tables_count == 0


class TestSqlServerSandboxDown:
    def test_sandbox_down_drops_database(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_down(sandbox_db="SBX_ABC123000000")

        assert result.status == "ok"
        assert not hasattr(result, "run_id")

        calls = [str(c) for c in default_cursor.execute.call_args_list]
        drop_calls = [c for c in calls if "DROP DATABASE" in c]
        assert len(drop_calls) == 1
