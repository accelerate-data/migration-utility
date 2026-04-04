"""Unit tests for the test-harness CLI and sandbox backends."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from shared.loader_io import clear_manifest_sandbox, read_manifest, write_manifest_sandbox
from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend
from shared.sandbox.sql_server import (
    SqlServerSandbox,
    _detect_remote_exec_target,
    _get_identity_columns,
    _serialize_rows,
    _validate_identifier,
    _validate_run_id,
)

FIXTURES = Path(__file__).parent / "fixtures" / "test_harness"


# ── Backend registry ─────────────────────────────────────────────────────────


class TestBackendRegistry:
    def test_sql_server_returns_correct_class(self) -> None:
        cls = get_backend("sql_server")
        assert cls is SqlServerSandbox

    def test_fabric_warehouse_returns_sql_server(self) -> None:
        cls = get_backend("fabric_warehouse")
        assert cls is SqlServerSandbox

    def test_unknown_technology_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported technology"):
            get_backend("snowflake_streaming")


# ── Sandbox database naming ──────────────────────────────────────────────────


class TestSandboxDbName:
    def test_name_format(self) -> None:
        name = SandboxBackend.sandbox_db_name("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        assert name == "__test_a1b2c3d4_e5f6_7890_abcd_ef1234567890"

    def test_name_without_dashes(self) -> None:
        name = SandboxBackend.sandbox_db_name("abc123")
        assert name == "__test_abc123"


# ── Run ID validation ────────────────────────────────────────────────────────


class TestRunIdValidation:
    def test_valid_uuid(self) -> None:
        _validate_run_id("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

    def test_valid_alphanumeric(self) -> None:
        _validate_run_id("test_run_123")

    def test_rejects_sql_injection(self) -> None:
        with pytest.raises(ValueError, match="Invalid run_id"):
            _validate_run_id("x'; DROP DATABASE master; --")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Invalid run_id"):
            _validate_run_id("")

    def test_rejects_special_chars(self) -> None:
        with pytest.raises(ValueError, match="Invalid run_id"):
            _validate_run_id("run id with spaces")


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
    def test_from_env_missing_host_raises(self) -> None:
        env = {"SA_PASSWORD": "pass", "MSSQL_DB": "db"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MSSQL_HOST"):
                SqlServerSandbox.from_env({})

    def test_from_env_missing_password_raises(self) -> None:
        env = {"MSSQL_HOST": "localhost", "MSSQL_DB": "db"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="SA_PASSWORD"):
                SqlServerSandbox.from_env({})

    def test_from_env_missing_database_raises(self) -> None:
        env = {"MSSQL_HOST": "localhost", "SA_PASSWORD": "pass"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="MSSQL_DB"):
                SqlServerSandbox.from_env({})

    def test_from_env_prefers_manifest_source_database(self) -> None:
        env = {"MSSQL_HOST": "localhost", "SA_PASSWORD": "pass", "MSSQL_DB": "envDB"}
        with patch.dict(os.environ, env, clear=True):
            backend = SqlServerSandbox.from_env({"source_database": "manifestDB"})
        assert backend.database == "manifestDB"

    def test_from_env_reads_user_and_driver(self) -> None:
        env = {
            "MSSQL_HOST": "localhost", "SA_PASSWORD": "pass", "MSSQL_DB": "db",
            "MSSQL_USER": "admin", "MSSQL_DRIVER": "FreeTDS",
        }
        with patch.dict(os.environ, env, clear=True):
            backend = SqlServerSandbox.from_env({})
        assert backend.user == "admin"
        assert backend.driver == "FreeTDS"


# ── SQL Server backend (mocked _connect) ─────────────────────────────────────


def _make_backend() -> SqlServerSandbox:
    return SqlServerSandbox(
        host="localhost",
        port="1433",
        database="TestDB",
        password="TestPass123",
    )


def _mock_connect_factory(
    *,
    source_cursor: MagicMock | None = None,
    sandbox_cursor: MagicMock | None = None,
    default_cursor: MagicMock | None = None,
) -> Callable[..., Any]:
    """Return a _connect side_effect that routes by database keyword arg."""
    @contextmanager
    def _fake_connect(*, database: str | None = None):
        conn = MagicMock()
        if database and database.startswith("__test_") and sandbox_cursor is not None:
            conn.cursor.return_value = sandbox_cursor
        elif source_cursor is not None and database and not database.startswith("__test_"):
            conn.cursor.return_value = source_cursor
        elif default_cursor is not None:
            conn.cursor.return_value = default_cursor
        else:
            raise AssertionError(f"Unexpected _connect call: database={database!r}")
        yield conn
    return _fake_connect


class TestSqlServerSandboxUp:
    """Test sandbox_up generates correct SQL via mocked _connect."""

    def test_sandbox_up_creates_database(self) -> None:
        backend = _make_backend()

        source_cursor = MagicMock()
        source_cursor.fetchall.side_effect = [
            [("dbo", "Product"), ("silver", "DimProduct")],
            [("dbo", "usp_load", "CREATE PROCEDURE dbo.usp_load AS BEGIN SELECT 1 END")],
        ]

        sandbox_cursor = MagicMock()
        default_cursor = MagicMock()

        fake_connect = _mock_connect_factory(
            source_cursor=source_cursor,
            sandbox_cursor=sandbox_cursor,
            default_cursor=default_cursor,
        )

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_up(
                run_id="test-run-id",
                schemas=["dbo", "silver"],
            )

        assert result["status"] in ("ok", "partial")
        assert result["run_id"] == "test-run-id"
        assert "__test_" in result["sandbox_database"]
        assert result["tables_cloned"] == ["dbo.Product", "silver.DimProduct"]
        assert result["procedures_cloned"] == ["dbo.usp_load"]

        calls = [str(c) for c in default_cursor.execute.call_args_list]
        create_db_calls = [c for c in calls if "CREATE DATABASE" in c]
        assert len(create_db_calls) == 1



# ── Sandbox down (mocked) ────────────────────────────────────────────────────


class TestSqlServerSandboxStatus:
    """Test sandbox_status checks database existence via mocked _connect."""

    def test_sandbox_status_exists(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchone.return_value = (1,)  # DB_ID returns non-None

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(run_id="test-run-id")

        assert result["status"] == "ok"
        assert result["exists"] is True
        assert result["run_id"] == "test-run-id"

    def test_sandbox_status_not_found(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchone.return_value = (None,)  # DB_ID returns None

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(run_id="test-run-id")

        assert result["status"] == "not_found"
        assert result["exists"] is False


class TestSqlServerSandboxDown:
    def test_sandbox_down_drops_database(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_down(run_id="test-run-id")

        assert result["status"] == "ok"
        assert result["run_id"] == "test-run-id"

        calls = [str(c) for c in default_cursor.execute.call_args_list]
        drop_calls = [c for c in calls if "DROP DATABASE" in c]
        assert len(drop_calls) == 1


# ── Execute scenario (mocked) ────────────────────────────────────────────────


class TestSqlServerExecuteScenario:
    def test_detect_remote_exec_target_cross_database(self) -> None:
        definition = """
        CREATE PROCEDURE [silver].[usp_load_dimproduct]
        AS
        BEGIN
            EXEC OtherDB.dbo.usp_load;
        END
        """

        assert _detect_remote_exec_target(definition) == {
            "kind": "cross-database",
            "target": "OtherDB.dbo.usp_load",
        }

    def test_detect_remote_exec_target_linked_server(self) -> None:
        definition = """
        CREATE PROCEDURE [silver].[usp_load_dimproduct]
        AS
        BEGIN
            EXEC [LinkedServer].db.dbo.usp_load;
        END
        """

        assert _detect_remote_exec_target(definition) == {
            "kind": "linked-server",
            "target": "[LinkedServer].db.dbo.usp_load",
        }

    def test_execute_remote_exec_returns_clear_error(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            """
            CREATE PROCEDURE [silver].[usp_load_dimproduct]
            AS
            BEGIN
                EXEC OtherDB.dbo.usp_load;
            END
            """,
        )

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_remote_exec",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[silver].[usp_load_dimproduct]",
            "given": [],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "error"
        assert result["errors"] == [{
            "code": "REMOTE_EXEC_UNSUPPORTED",
            "message": (
                "Sandbox cannot execute cross-database procedure call "
                "OtherDB.dbo.usp_load from [silver].[usp_load_dimproduct]. "
                "The sandbox only clones objects from the source database."
            ),
        }]
        sandbox_cursor.execute.assert_called_once_with(
            "SELECT OBJECT_DEFINITION(OBJECT_ID(?))",
            "[silver].[usp_load_dimproduct]",
        )

    def test_execute_captures_ground_truth(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.description = [("id",), ("name",)]
        sandbox_cursor.fetchall.return_value = [(1, "Widget")]
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load_dimproduct] AS BEGIN SELECT 1 END",
        )

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_insert_new_product",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[dbo].[usp_load_dimproduct]",
            "given": [
                {
                    "table": "[dbo].[Product]",
                    "rows": [{"id": 1, "name": "Widget"}],
                }
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        assert result["row_count"] == 1
        assert result["ground_truth_rows"] == [{"id": 1, "name": "Widget"}]
        assert result["scenario_name"] == "test_insert_new_product"

    def test_execute_missing_required_key_raises(self) -> None:
        backend = _make_backend()
        scenario = {"name": "incomplete", "target_table": "[dbo].[T]"}

        with pytest.raises(KeyError, match="procedure"):
            backend.execute_scenario(run_id="test-run", scenario=scenario)


# ── IDENTITY_INSERT handling ──────────────────────────────────────────────────


class TestIdentityInsert:
    """Test IDENTITY_INSERT toggling in execute_scenario fixture insertion."""

    def test_identity_insert_enabled_when_fixture_has_identity_column(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        # First call: OBJECT_DEFINITION lookup (proc check)
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        # identity column lookup returns SalesOrderID as identity
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[SalesOrderHeader]",)],  # sys.tables (trigger disable)
            [("SalesOrderID",)],       # _get_identity_columns
            [(1, "Widget", 100)],       # SELECT * FROM target
        ]
        sandbox_cursor.description = [("SalesOrderID",), ("Name",), ("Amount",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_identity",
            "target_table": "[silver].[FactSales]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {
                    "table": "[bronze].[SalesOrderHeader]",
                    "rows": [{"SalesOrderID": 1, "Status": 5}],
                }
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"

        # Verify SET IDENTITY_INSERT ON/OFF were called
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        identity_on = [c for c in execute_calls if "IDENTITY_INSERT" in c and "ON" in c and "OFF" not in c]
        identity_off = [c for c in execute_calls if "IDENTITY_INSERT" in c and "OFF" in c]
        assert len(identity_on) == 1, f"Expected 1 IDENTITY_INSERT ON, got {identity_on}"
        assert len(identity_off) == 1, f"Expected 1 IDENTITY_INSERT OFF, got {identity_off}"

    def test_no_identity_insert_when_no_identity_columns(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[Product]",)],  # sys.tables (trigger disable)
            [],                         # _get_identity_columns: no identity cols
            [(1, "Widget")],            # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_no_identity",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {
                    "table": "[bronze].[Product]",
                    "rows": [{"name": "Widget"}],
                }
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        identity_calls = [c for c in execute_calls if "IDENTITY_INSERT" in c]
        assert len(identity_calls) == 0, f"Unexpected IDENTITY_INSERT calls: {identity_calls}"

    def test_identity_insert_toggles_per_table(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        # Two tables: first has identity, second does not
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[SalesOrderHeader]",), ("[bronze].[SalesOrderDetail]",)],  # sys.tables (trigger disable)
            [("OrderID",)],             # _get_identity_columns for table 1
            [],                         # _get_identity_columns for table 2
            [(1, "result")],            # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("value",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_multi_table",
            "target_table": "[silver].[FactSales]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {
                    "table": "[bronze].[SalesOrderHeader]",
                    "rows": [{"OrderID": 1, "Status": 5}],
                },
                {
                    "table": "[bronze].[SalesOrderDetail]",
                    "rows": [{"LineItem": "A", "Qty": 10}],
                },
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        # Only table 1 should have IDENTITY_INSERT
        identity_on = [c for c in execute_calls if "IDENTITY_INSERT" in c and "SalesOrderHeader" in c and "ON" in c and "OFF" not in c]
        identity_off = [c for c in execute_calls if "IDENTITY_INSERT" in c and "SalesOrderHeader" in c and "OFF" in c]
        assert len(identity_on) == 1
        assert len(identity_off) == 1
        # Table 2 should NOT have IDENTITY_INSERT
        detail_identity = [c for c in execute_calls if "IDENTITY_INSERT" in c and "SalesOrderDetail" in c]
        assert len(detail_identity) == 0


# ── FK constraint disabling ───────────────────────────────────────────────────


class TestFkConstraintDisabling:
    """Test FK constraints are disabled/re-enabled around fixture insertion."""

    def test_fk_nocheck_wraps_fixture_insertion(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[Product]",)],  # sys.tables (trigger disable)
            [],                         # _get_identity_columns
            [(1, "Widget")],            # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_fk_disable",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {
                    "table": "[bronze].[Product]",
                    "rows": [{"id": 1, "name": "Widget"}],
                }
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]

        # NOCHECK before inserts
        nocheck = [c for c in execute_calls if "NOCHECK CONSTRAINT ALL" in c]
        assert len(nocheck) == 1, f"Expected 1 NOCHECK, got {nocheck}"
        assert "[bronze].[Product]" in nocheck[0]

        # CHECK re-enabled before EXEC
        check = [c for c in execute_calls if "CHECK CONSTRAINT ALL" in c and "NOCHECK" not in c]
        assert len(check) == 1, f"Expected 1 CHECK, got {check}"

        # Verify ordering: NOCHECK < executemany (INSERT) < CHECK < EXEC
        nocheck_idx = next(i for i, c in enumerate(execute_calls) if "NOCHECK" in c)
        check_idx = next(i for i, c in enumerate(execute_calls) if "CHECK CONSTRAINT ALL" in c and "NOCHECK" not in c)
        exec_idx = next(i for i, c in enumerate(execute_calls) if "EXEC [dbo]" in c)
        assert nocheck_idx < check_idx < exec_idx

        # executemany is called separately — verify it happened
        assert sandbox_cursor.executemany.called

    def test_fk_nocheck_skipped_for_empty_fixtures(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        sandbox_cursor.fetchall.side_effect = [
            [("[silver].[DimProduct]",)],   # sys.tables (trigger disable — before FK)
            [(1, "Widget")],                # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_empty",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {"table": "[bronze].[Product]", "rows": []},
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        nocheck = [c for c in execute_calls if "NOCHECK" in c]
        assert len(nocheck) == 0, "No NOCHECK for empty fixture rows"


# ── Trigger disabling ─────────────────────────────────────────────────────────


class TestTriggerDisabling:
    """Test triggers are disabled on all sandbox tables."""

    def test_triggers_disabled_on_all_sandbox_tables(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        # Calls: sys.tables, identity_cols, SELECT * result
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[Product]",), ("[silver].[DimProduct]",), ("[dbo].[Config]",)],  # sys.tables (trigger disable)
            [],                                          # _get_identity_columns
            [(1, "Widget")],                             # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_triggers",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {
                    "table": "[bronze].[Product]",
                    "rows": [{"id": 1, "name": "Widget"}],
                }
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]

        disable_trigger = [c for c in execute_calls if "DISABLE TRIGGER ALL" in c]
        assert len(disable_trigger) == 3, (
            f"Expected DISABLE TRIGGER on all 3 sandbox tables, got {disable_trigger}"
        )

    def test_triggers_disabled_before_exec(self) -> None:
        """DISABLE TRIGGER must happen before EXEC procedure."""
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        sandbox_cursor.fetchall.side_effect = [
            [("[silver].[T1]",)],                        # sys.tables (trigger disable)
            [],                                          # _get_identity_columns
            [(1, "Widget")],                             # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_order",
            "target_table": "[silver].[T1]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {"table": "[bronze].[S1]", "rows": [{"id": 1}]},
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        disable_idx = next(i for i, c in enumerate(execute_calls) if "DISABLE TRIGGER" in c)
        exec_idx = next(i for i, c in enumerate(execute_calls) if "EXEC [dbo]" in c)
        assert disable_idx < exec_idx


# ── MONEY decoding ────────────────────────────────────────────────────────────


class TestSerializeRows:
    """Test _serialize_rows handles Decimal (MONEY/SMALLMONEY), bytes, and primitives."""

    def test_primitives_pass_through(self) -> None:
        rows = [{"id": 1, "name": "Widget", "active": True, "deleted": None, "rate": 3.14}]
        result = _serialize_rows(rows)
        assert result == rows

    def test_decimal_to_string(self) -> None:
        """pyodbc returns MONEY/SMALLMONEY as Decimal with ODBC Driver 17/18."""
        rows = [{"price": Decimal("10.5000"), "tax": Decimal("0.8500")}]
        result = _serialize_rows(rows)
        assert result == [{"price": "10.5000", "tax": "0.8500"}]

    def test_decimal_negative(self) -> None:
        rows = [{"balance": Decimal("-42.1234")}]
        result = _serialize_rows(rows)
        assert result == [{"balance": "-42.1234"}]

    def test_decimal_zero(self) -> None:
        rows = [{"amount": Decimal("0.0000")}]
        result = _serialize_rows(rows)
        assert result == [{"amount": "0.0000"}]

    def test_bytes_hex_encoded(self) -> None:
        rows = [{"data": b"\xde\xad\xbe\xef"}]
        result = _serialize_rows(rows)
        assert result == [{"data": "deadbeef"}]

    def test_datetime_to_string(self) -> None:
        from datetime import datetime
        dt = datetime(2024, 1, 15, 10, 30, 0)
        rows = [{"created": dt}]
        result = _serialize_rows(rows)
        assert result == [{"created": "2024-01-15 10:30:00"}]

    def test_mixed_types(self) -> None:
        rows = [{"id": 1, "price": Decimal("25.9900"), "name": "Widget", "blob": b"\x00\x01"}]
        result = _serialize_rows(rows)
        assert result == [{"id": 1, "price": "25.9900", "name": "Widget", "blob": "0001"}]


# ── Schema validation ────────────────────────────────────────────────────────


class TestSchemaValidation:
    def test_execute_output_valid(self, assert_valid_schema) -> None:
        data = {
            "schema_version": "1.0",
            "run_id": "abc-123",
            "scenario_name": "test_scenario",
            "status": "ok",
            "ground_truth_rows": [{"id": 1, "name": "Widget"}],
            "row_count": 1,
            "errors": [],
        }
        assert_valid_schema(data, "test_harness_execute_output.json")

    def test_execute_output_error(self, assert_valid_schema) -> None:
        data = {
            "schema_version": "1.0",
            "run_id": "abc-123",
            "scenario_name": "test_scenario",
            "status": "error",
            "ground_truth_rows": [],
            "row_count": 0,
            "errors": [{"code": "SCENARIO_FAILED", "message": "connection refused"}],
        }
        assert_valid_schema(data, "test_harness_execute_output.json")

    def test_sandbox_up_output_ok(self, assert_valid_schema) -> None:
        data = {
            "run_id": "abc-123",
            "sandbox_database": "__test_abc_123",
            "status": "ok",
            "tables_cloned": ["dbo.Product"],
            "procedures_cloned": ["dbo.usp_load"],
            "errors": [],
        }
        assert_valid_schema(data, "sandbox_up_output.json")

    def test_sandbox_up_output_error(self, assert_valid_schema) -> None:
        data = {
            "run_id": "abc-123",
            "sandbox_database": "__test_abc_123",
            "status": "error",
            "tables_cloned": [],
            "procedures_cloned": [],
            "errors": [{"code": "SANDBOX_UP_FAILED", "message": "connection refused"}],
        }
        assert_valid_schema(data, "sandbox_up_output.json")

    def test_sandbox_down_output_ok(self, assert_valid_schema) -> None:
        data = {
            "run_id": "abc-123",
            "sandbox_database": "__test_abc_123",
            "status": "ok",
        }
        assert_valid_schema(data, "sandbox_down_output.json")

    def test_sandbox_down_output_error(self, assert_valid_schema) -> None:
        data = {
            "run_id": "abc-123",
            "sandbox_database": "__test_abc_123",
            "status": "error",
            "errors": [{"code": "SANDBOX_DOWN_FAILED", "message": "timeout"}],
        }
        assert_valid_schema(data, "sandbox_down_output.json")

    def test_sandbox_status_output_exists(self, assert_valid_schema) -> None:
        data = {
            "run_id": "abc-123",
            "sandbox_database": "__test_abc_123",
            "status": "ok",
            "exists": True,
        }
        assert_valid_schema(data, "sandbox_status_output.json")

    def test_sandbox_status_output_not_found(self, assert_valid_schema) -> None:
        data = {
            "run_id": "abc-123",
            "sandbox_database": "__test_abc_123",
            "status": "not_found",
            "exists": False,
        }
        assert_valid_schema(data, "sandbox_status_output.json")

    def test_test_spec_per_item_valid(self, assert_valid_schema) -> None:
        data = {
            "item_id": "silver.dimproduct",
            "status": "ok",
            "coverage": "complete",
            "branch_manifest": [
                {
                    "id": "merge_matched_update",
                    "statement_index": 0,
                    "description": "MERGE WHEN MATCHED → UPDATE",
                    "scenarios": ["test_merge_matched"],
                }
            ],
            "unit_tests": [
                {
                    "name": "test_merge_matched",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[bronze].[Product]",
                            "rows": [{"product_id": 1}],
                        }
                    ],
                    "expect": {
                        "rows": [{"product_key": 1}],
                    },
                }
            ],
            "uncovered_branches": [],
            "warnings": [],
            "validation": {"passed": True, "issues": []},
            "errors": [],
        }
        assert_valid_schema(data, "test_spec.json")

    def test_test_spec_output_valid(self, assert_valid_schema) -> None:
        data = {
            "schema_version": "1.0",
            "run_id": "abc-123",
            "results": [
                {
                    "item_id": "silver.dimproduct",
                    "status": "ok",
                    "coverage": "complete",
                    "branch_manifest": [
                        {
                            "id": "merge_matched_update",
                            "statement_index": 0,
                            "description": "MERGE WHEN MATCHED → UPDATE",
                            "scenarios": ["test_merge_matched"],
                        }
                    ],
                    "unit_tests": [
                        {
                            "name": "test_merge_matched",
                            "target_table": "[silver].[DimProduct]",
                            "procedure": "[silver].[usp_load_DimProduct]",
                            "given": [
                                {
                                    "table": "[bronze].[Product]",
                                    "rows": [{"product_id": 1}],
                                }
                            ],
                            "expect": {
                                "rows": [{"product_key": 1}],
                            },
                        }
                    ],
                    "uncovered_branches": [],
                    "warnings": [],
                    "validation": {"passed": True, "issues": []},
                    "errors": [],
                }
            ],
            "summary": {"total": 1, "ok": 1, "partial": 0, "error": 0},
        }
        assert_valid_schema(data, "test_spec_output.json")


# ── CLI manifest routing ─────────────────────────────────────────────────────


class TestCLIManifestRouting:
    def test_load_manifest_returns_technology(self, tmp_path: Path) -> None:
        shutil.copy(FIXTURES / "manifest.json", tmp_path / "manifest.json")
        from shared.test_harness import _load_manifest

        manifest = _load_manifest(tmp_path)
        assert manifest["technology"] == "sql_server"
        assert manifest["source_database"] == "TestDB"
        assert manifest["extracted_schemas"] == ["dbo", "silver"]

    def test_load_manifest_missing_raises(self, tmp_path: Path) -> None:
        from click.exceptions import Exit

        from shared.test_harness import _load_manifest

        with pytest.raises(Exit):
            _load_manifest(tmp_path)


# ── Manifest sandbox persistence ──────────────────────────────────────────────


def _write_fixture_manifest(dest: Path) -> None:
    """Copy the standard test manifest fixture to dest."""
    shutil.copy(FIXTURES / "manifest.json", dest / "manifest.json")


class TestWriteManifestSandbox:
    def test_persist_sandbox_to_manifest(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "run-123", "__test_run_123")

        manifest = read_manifest(tmp_path)
        assert manifest["sandbox"] == {"run_id": "run-123", "database": "__test_run_123"}
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"
        assert manifest["extracted_schemas"] == ["dbo", "silver"]

    def test_persist_overwrites_existing_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "old-run", "__test_old_run")
        write_manifest_sandbox(tmp_path, "new-run", "__test_new_run")

        manifest = read_manifest(tmp_path)
        assert manifest["sandbox"]["run_id"] == "new-run"


class TestClearManifestSandbox:
    def test_clear_removes_sandbox_key(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "run-123", "__test_run_123")
        clear_manifest_sandbox(tmp_path)

        manifest = read_manifest(tmp_path)
        assert "sandbox" not in manifest
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"

    def test_clear_noop_when_no_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        clear_manifest_sandbox(tmp_path)

        manifest = read_manifest(tmp_path)
        assert "sandbox" not in manifest


class TestResolveRunId:
    def test_explicit_run_id_takes_precedence(self, tmp_path: Path) -> None:
        from shared.test_harness import _resolve_run_id

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "manifest-run", "__test_manifest_run")

        assert _resolve_run_id("explicit-run", tmp_path) == "explicit-run"

    def test_falls_back_to_manifest(self, tmp_path: Path) -> None:
        from shared.test_harness import _resolve_run_id

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "manifest-run", "__test_manifest_run")

        assert _resolve_run_id(None, tmp_path) == "manifest-run"

    def test_missing_run_id_and_no_sandbox_exits(self, tmp_path: Path) -> None:
        from click.exceptions import Exit

        from shared.test_harness import _resolve_run_id

        _write_fixture_manifest(tmp_path)

        with pytest.raises(Exit):
            _resolve_run_id(None, tmp_path)


# ── E2E CLI invocation ────────────────────────────────────────────────────────


def _cli_env(tmp_path: Path) -> dict[str, str]:
    """Env vars needed for SqlServerSandbox.from_env in CLI tests."""
    return {
        "MSSQL_HOST": "localhost",
        "MSSQL_PORT": "1433",
        "SA_PASSWORD": "TestPass123",
        "MSSQL_DB": "TestDB",
    }


class TestCLISandboxUpPersists:
    """E2E: invoke sandbox-up via CliRunner and verify manifest.json is updated."""

    def test_sandbox_up_writes_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = {
            "run_id": "e2e-run",
            "sandbox_database": "__test_e2e_run",
            "status": "ok",
            "tables_cloned": ["dbo.Product"],
            "procedures_cloned": [],
            "errors": [],
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-up", "--run-id", "e2e-run", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["sandbox"] == {"run_id": "e2e-run", "database": "__test_e2e_run"}

    def test_sandbox_up_error_does_not_write_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = {
            "run_id": "e2e-run",
            "sandbox_database": "__test_e2e_run",
            "status": "error",
            "tables_cloned": [],
            "procedures_cloned": [],
            "errors": [{"code": "CONNECT_FAILED", "message": "timeout"}],
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-up", "--run-id", "e2e-run", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "sandbox" not in manifest


class TestCLISandboxDownClears:
    """E2E: invoke sandbox-down via CliRunner and verify manifest.json is cleared."""

    def test_sandbox_down_clears_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "e2e-run", "__test_e2e_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = {
            "run_id": "e2e-run",
            "sandbox_database": "__test_e2e_run",
            "status": "ok",
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "sandbox" not in manifest

    def test_sandbox_down_reads_run_id_from_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "manifest-run", "__test_manifest_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = {
            "run_id": "manifest-run",
            "sandbox_database": "__test_manifest_run",
            "status": "ok",
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_down.assert_called_once_with(run_id="manifest-run")


class TestCLIStatusFallback:
    """E2E: invoke sandbox-status without --run-id, verify manifest fallback."""

    def test_sandbox_status_uses_manifest_run_id(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "manifest-run", "__test_manifest_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_status.return_value = {
            "run_id": "manifest-run",
            "sandbox_database": "__test_manifest_run",
            "status": "ok",
            "exists": True,
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_status.assert_called_once_with(run_id="manifest-run")

    def test_sandbox_status_no_run_id_no_manifest_exits(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["errors"][0]["code"] == "MISSING_RUN_ID"


# ── execute-spec CLI ─────────────────────────────────────────────────────────


def _write_test_spec(path: Path, unit_tests: list[dict[str, Any]]) -> Path:
    """Write a minimal test spec JSON file and return its path."""
    spec = {
        "item_id": "silver.dimproduct",
        "status": "ok",
        "coverage": "complete",
        "branch_manifest": [],
        "unit_tests": unit_tests,
        "uncovered_branches": [],
        "warnings": [],
        "validation": {"passed": True, "issues": []},
        "errors": [],
    }
    spec_path = path / "test-specs" / "silver.dimproduct.json"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec_path.write_text(json.dumps(spec, indent=2))
    return spec_path


class TestCLIExecuteSpec:
    """E2E: invoke execute-spec via CliRunner, verify expect.rows written back."""

    def test_execute_spec_writes_expect_rows(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "e2e-run", "__test_e2e_run")

        unit_tests = [
            {
                "name": "test_merge_matched",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load_DimProduct]",
                "given": [
                    {"table": "[bronze].[Product]", "rows": [{"id": 1}]},
                ],
            }
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.return_value = {
            "schema_version": "1.0",
            "run_id": "e2e-run",
            "scenario_name": "test_merge_matched",
            "status": "ok",
            "ground_truth_rows": [{"ProductKey": 1, "Name": "Widget"}],
            "row_count": 1,
            "errors": [],
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 0

        # Verify spec file was updated with expect.rows
        updated_spec = json.loads(spec_path.read_text())
        assert updated_spec["unit_tests"][0]["expect"] == {
            "rows": [{"ProductKey": 1, "Name": "Widget"}],
        }

    def test_execute_spec_partial_failure(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "e2e-run", "__test_e2e_run")

        unit_tests = [
            {
                "name": "test_ok",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
            {
                "name": "test_fail",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 2}]}],
            },
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.side_effect = [
            {
                "scenario_name": "test_ok",
                "status": "ok",
                "ground_truth_rows": [{"id": 1}],
                "row_count": 1,
                "errors": [],
            },
            {
                "scenario_name": "test_fail",
                "status": "error",
                "ground_truth_rows": [],
                "row_count": 0,
                "errors": [{"code": "SCENARIO_FAILED", "message": "insert failed"}],
            },
        ]

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 0  # partial success → exit 0
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 1

    def test_execute_spec_all_fail_exits_1(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "e2e-run", "__test_e2e_run")

        unit_tests = [
            {
                "name": "test_fail",
                "target_table": "[silver].[DimProduct]",
                "procedure": "[silver].[usp_load]",
                "given": [{"table": "[bronze].[Product]", "rows": [{"id": 1}]}],
            },
        ]
        spec_path = _write_test_spec(tmp_path, unit_tests)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.execute_scenario.return_value = {
            "scenario_name": "test_fail",
            "status": "error",
            "ground_truth_rows": [],
            "row_count": 0,
            "errors": [{"code": "SCENARIO_FAILED", "message": "connection refused"}],
        }

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, [
                "execute-spec",
                "--spec", str(spec_path),
                "--project-root", str(tmp_path),
            ])

        assert result.exit_code == 1

    def test_execute_spec_output_schema(self, assert_valid_schema) -> None:
        data = {
            "schema_version": "1.0",
            "run_id": "abc-123",
            "spec_path": "test-specs/silver.dimproduct.json",
            "total": 2,
            "ok": 1,
            "failed": 1,
            "results": [
                {
                    "scenario_name": "test_merge_matched",
                    "status": "ok",
                    "row_count": 1,
                    "errors": [],
                },
                {
                    "scenario_name": "test_merge_not_matched",
                    "status": "error",
                    "row_count": 0,
                    "errors": [{"code": "SCENARIO_FAILED", "message": "insert failed"}],
                },
            ],
        }
        assert_valid_schema(data, "execute_spec_output.json")


# ── dbt YAML conversion ──────────────────────────────────────────────────────


class TestDbtConversion:
    """Test bracket-to-dbt mapping, model name derivation, and YAML output."""

    def test_bracket_to_source(self) -> None:
        from shared.test_harness import _bracket_to_dbt

        assert _bracket_to_dbt("[bronze].[SalesOrderHeader]") == "source('bronze', 'SalesOrderHeader')"

    def test_bracket_to_ref(self) -> None:
        from shared.test_harness import _bracket_to_dbt

        assert _bracket_to_dbt("[silver].[stg_DimProduct]") == "ref('stg_DimProduct')"

    def test_dbo_maps_to_source(self) -> None:
        from shared.test_harness import _bracket_to_dbt

        assert _bracket_to_dbt("[dbo].[Product]") == "source('dbo', 'Product')"

    def test_gold_maps_to_ref(self) -> None:
        from shared.test_harness import _bracket_to_dbt

        assert _bracket_to_dbt("[gold].[FactSales]") == "ref('FactSales')"

    def test_passthrough_non_bracket(self) -> None:
        from shared.test_harness import _bracket_to_dbt

        assert _bracket_to_dbt("silver.DimProduct") == "silver.DimProduct"

    def test_derive_model_name(self) -> None:
        from shared.test_harness import _derive_model_name

        assert _derive_model_name("[silver].[DimProduct]") == "stg_dimproduct"

    def test_derive_model_name_dot_fallback(self) -> None:
        from shared.test_harness import _derive_model_name

        assert _derive_model_name("silver.DimProduct") == "stg_dimproduct"

    def test_convert_spec_to_dbt(self) -> None:
        from shared.test_harness import convert_spec_to_dbt

        spec_data = {
            "item_id": "silver.dimproduct",
            "unit_tests": [
                {
                    "name": "test_merge_matched",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load_DimProduct]",
                    "given": [
                        {
                            "table": "[bronze].[Product]",
                            "rows": [{"ProductID": 1, "Name": "Widget"}],
                        }
                    ],
                    "expect": {
                        "rows": [{"ProductKey": 1, "ProductName": "Widget"}],
                    },
                }
            ],
        }

        result = convert_spec_to_dbt(spec_data)
        assert len(result["unit_tests"]) == 1

        test = result["unit_tests"][0]
        assert test["name"] == "test_merge_matched"
        assert test["model"] == "stg_dimproduct"
        assert test["given"][0]["input"] == "source('bronze', 'Product')"
        assert test["given"][0]["rows"] == [{"ProductID": 1, "Name": "Widget"}]
        assert test["expect"]["rows"] == [{"ProductKey": 1, "ProductName": "Widget"}]

    def test_convert_spec_uses_existing_model_field(self) -> None:
        from shared.test_harness import convert_spec_to_dbt

        spec_data = {
            "unit_tests": [
                {
                    "name": "test_custom_model",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load]",
                    "model": "custom_model_name",
                    "given": [
                        {"table": "[bronze].[Product]", "rows": [{"id": 1}]},
                    ],
                }
            ],
        }

        result = convert_spec_to_dbt(spec_data)
        assert result["unit_tests"][0]["model"] == "custom_model_name"

    def test_convert_dbt_cli_writes_yaml(self, tmp_path: Path) -> None:
        import yaml as yaml_mod

        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = {
            "item_id": "silver.dimproduct",
            "unit_tests": [
                {
                    "name": "test_merge",
                    "target_table": "[silver].[DimProduct]",
                    "procedure": "[silver].[usp_load]",
                    "given": [
                        {"table": "[bronze].[Product]", "rows": [{"id": 1}]},
                    ],
                    "expect": {"rows": [{"key": 1}]},
                }
            ],
        }
        spec_path = tmp_path / "spec.json"
        spec_path.write_text(json.dumps(spec))
        output_path = tmp_path / "output.yml"

        runner = CliRunner()
        result = runner.invoke(app, [
            "convert-dbt",
            "--spec", str(spec_path),
            "--output", str(output_path),
        ])

        assert result.exit_code == 0
        assert output_path.exists()

        dbt_data = yaml_mod.safe_load(output_path.read_text())
        assert len(dbt_data["unit_tests"]) == 1
        assert dbt_data["unit_tests"][0]["model"] == "stg_dimproduct"
        assert dbt_data["unit_tests"][0]["given"][0]["input"] == "source('bronze', 'Product')"


# ── Corrupt JSON tests ──────────────────────────────────────────────────


class TestCorruptJsonHandling:
    """Verify CLI commands handle corrupt JSON inputs gracefully."""

    def test_sandbox_up_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-up with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, ["sandbox-up", "--run-id", "test-run", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_sandbox_status_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-status with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, ["sandbox-status", "--run-id", "test-run", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_execute_spec_corrupt_json_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with corrupt test-spec JSON exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "corrupt-spec.json"
        spec.write_text("{not valid json", encoding="utf-8")
        (tmp_path / "manifest.json").write_text('{"dialect":"tsql","technology":"sql_server"}', encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--run-id", "test-run", "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_execute_spec_missing_required_fields_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with valid JSON but missing unit_tests exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "empty-spec.json"
        spec.write_text('{"model": "stg_test"}', encoding="utf-8")
        (tmp_path / "manifest.json").write_text('{"dialect":"tsql","technology":"sql_server"}', encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--run-id", "test-run", "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_convert_dbt_corrupt_spec_exit_1(self, tmp_path: Path) -> None:
        """convert-dbt with corrupt test-spec JSON exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "corrupt-spec.json"
        spec.write_text("{not valid json", encoding="utf-8")
        output_path = tmp_path / "output.yml"
        runner = CliRunner()
        result = runner.invoke(app, ["convert-dbt", "--spec", str(spec), "--output", str(output_path)])
        assert result.exit_code == 1
