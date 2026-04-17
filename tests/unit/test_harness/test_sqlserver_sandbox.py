"""SQL Server sandbox operations tests (mocked _connect)."""

from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from shared.output_models.sandbox import (
    ErrorEntry,
    ExecuteSpecOutput,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.output_models.test_specs import TestSpec, TestSpecOutput
from shared.sandbox.base import serialize_rows as _serialize_rows
from shared.sandbox.sql_server import (
    SqlServerSandbox,
    _detect_remote_exec_target,
    _get_identity_columns,
    _get_not_null_defaults,
    _import_pyodbc,
    _validate_identifier,
    _validate_sandbox_db_name,
)


from .conftest import _make_backend, _mock_connect_factory


# ── SQL Server backend (mocked _connect) ─────────────────────────────────────


class TestSqlServerSandboxUp:
    """Test sandbox_up generates correct SQL via mocked _connect."""

    def test_public_lifecycle_methods_delegate_to_lifecycle_service(self) -> None:
        backend = _make_backend()
        backend._lifecycle = MagicMock()
        backend._lifecycle.sandbox_up.return_value = "up-result"
        backend._lifecycle.sandbox_reset.return_value = "reset-result"
        backend._lifecycle.sandbox_down.return_value = "down-result"
        backend._lifecycle.sandbox_status.return_value = "status-result"

        assert backend.sandbox_up(["dbo"]) == "up-result"
        assert backend.sandbox_reset("__test_existing", ["dbo"]) == "reset-result"
        assert backend.sandbox_down("__test_existing") == "down-result"
        assert backend.sandbox_status("__test_existing", ["dbo"]) == "status-result"

        backend._lifecycle.sandbox_up.assert_called_once_with(["dbo"])
        backend._lifecycle.sandbox_reset.assert_called_once_with("__test_existing", ["dbo"])
        backend._lifecycle.sandbox_down.assert_called_once_with("__test_existing")
        backend._lifecycle.sandbox_status.assert_called_once_with("__test_existing", ["dbo"])

    def test_public_execution_methods_delegate_to_execution_service(self) -> None:
        backend = _make_backend()
        backend._execution = MagicMock()
        backend._execution.execute_scenario.return_value = "scenario-result"
        backend._execution.execute_select.return_value = "select-result"
        backend._comparison = MagicMock()
        backend._comparison.compare_two_sql.return_value = "compare-result"

        scenario = {
            "name": "case",
            "target_table": "[dbo].[T]",
            "procedure": "[dbo].[p]",
            "given": [],
        }
        fixtures: list[dict[str, object]] = []

        assert backend.execute_scenario("__test_existing", scenario) == "scenario-result"
        assert backend.execute_select("__test_existing", "SELECT 1", fixtures) == "select-result"
        assert backend.compare_two_sql(
            "__test_existing", "SELECT 1", "SELECT 1", fixtures,
        ) == "compare-result"

        backend._execution.execute_scenario.assert_called_once_with(
            "__test_existing", scenario,
        )
        backend._execution.execute_select.assert_called_once_with(
            "__test_existing", "SELECT 1", fixtures,
        )
        backend._comparison.compare_two_sql.assert_called_once_with(
            "__test_existing", "SELECT 1", "SELECT 1", fixtures,
        )

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
            [("dbo", "usp_load", "CREATE PROCEDURE dbo.usp_load AS BEGIN SELECT 1 END")],
        ]
        source_cursor.fetchone.return_value = ("CREATE VIEW [silver].[vw_customer] AS SELECT 1 AS id",)

        sandbox_cursor = MagicMock()
        default_cursor = MagicMock()

        admin_connect = _mock_connect_factory(
            source_cursor=source_cursor,
            sandbox_cursor=sandbox_cursor,
            default_cursor=default_cursor,
        )
        source_connect = _mock_connect_factory(source_cursor=source_cursor)

        with patch.object(backend, "_connect", side_effect=admin_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.sandbox_up(
                schemas=["dbo", "silver"],
            )

        assert result.status in ("ok", "partial")
        assert result.sandbox_database.startswith("__test_")
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
        default_cursor.fetchone.return_value = (1,)
        sandbox_cursor = MagicMock()
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
            result = backend.sandbox_reset("__test_existing", schemas=["dbo"])

        assert result.status == "ok"
        assert result.sandbox_database == "__test_existing"
        execute_calls = [call.args[0] for call in default_cursor.execute.call_args_list]
        assert any("DROP DATABASE [__test_existing]" in sql for sql in execute_calls)
        assert any("CREATE DATABASE [__test_existing]" in sql for sql in execute_calls)

    def test_sandbox_reset_reports_drop_failure_without_cloning(self) -> None:
        backend = _make_backend()
        down_result = SandboxDownOutput(
            sandbox_database="__test_existing",
            status="error",
            errors=[ErrorEntry(code="SANDBOX_DOWN_FAILED", message="drop failed")],
        )

        with patch.object(backend, "sandbox_down", return_value=down_result), \
             patch.object(backend, "_sandbox_clone_into") as mock_clone:
            result = backend.sandbox_reset("__test_existing", schemas=["dbo"])

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
            if database and database.startswith("__test_"):
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


# ── Sandbox down (mocked) ────────────────────────────────────────────────────


class TestSqlServerSandboxStatus:
    """Test sandbox_status checks database existence via mocked _connect."""

    def test_sandbox_status_exists(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchone.side_effect = [
            (1,),  # DB_ID returns non-None
            (2,),
            (1,),
            (3,),
        ]

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="__test_abc123")

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
        default_cursor.fetchone.side_effect = [
            (1,),  # DB_ID returns non-None
            (0,),
            (0,),
            (0,),
        ]

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="__test_abc123", schemas=["silver"])

        assert result.status == "ok"
        assert result.exists is True
        assert result.has_content is False
        calls = default_cursor.execute.call_args_list
        assert calls[1].args[1:] == ("silver",)

    def test_sandbox_status_not_found(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchone.return_value = (None,)  # DB_ID returns None

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="__test_abc123")

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
            result = backend.sandbox_down(sandbox_db="__test_abc123")

        assert result.status == "ok"
        assert not hasattr(result, "run_id")

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
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_remote_exec",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[silver].[usp_load_dimproduct]",
            "given": [],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "error"
        assert len(result.errors) == 1
        assert result.errors[0].code == "REMOTE_EXEC_UNSUPPORTED"
        assert "cross-database procedure call" in result.errors[0].message
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
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

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

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
        assert result.row_count == 1
        assert result.ground_truth_rows == [{"id": 1, "name": "Widget"}]
        assert result.scenario_name == "test_insert_new_product"
        assert not hasattr(result, "run_id")

    def test_execute_missing_required_key_raises(self) -> None:
        backend = _make_backend()
        scenario = {"name": "incomplete", "target_table": "[dbo].[T]"}

        with pytest.raises(KeyError, match="procedure"):
            backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)


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
            [],                         # _get_not_null_defaults
            [("SalesOrderID",)],       # _get_identity_columns
            [(1, "Widget", 100)],       # SELECT * FROM target
        ]
        sandbox_cursor.description = [("SalesOrderID",), ("Name",), ("Amount",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

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

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"

        # Verify SET IDENTITY_INSERT ON/OFF were called
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        identity_on = [c for c in execute_calls if "IDENTITY_INSERT" in c and "ON" in c and "OFF" not in c]
        identity_off = [c for c in execute_calls if "IDENTITY_INSERT" in c and "OFF" in c]
        assert len(identity_on) == 1, f"Expected 1 IDENTITY_INSERT ON, got {identity_on}"
        assert len(identity_off) == 1, f"Expected 1 IDENTITY_INSERT OFF, got {identity_off}"

    def test_fixture_table_names_are_bracket_quoted_in_ddl(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[Order Detail]",)],
            [],
            [("OrderID",)],
            [(1, "result")],
        ]
        sandbox_cursor.description = [("id",), ("value",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_quoted_fixture_table",
            "target_table": "[silver].[FactSales]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {
                    "table": "bronze.Order Detail",
                    "rows": [{"OrderID": 1}],
                }
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
        execute_calls = [call.args[0] for call in sandbox_cursor.execute.call_args_list]
        executemany_calls = [call.args[0] for call in sandbox_cursor.executemany.call_args_list]
        assert "ALTER TABLE [bronze].[Order Detail] NOCHECK CONSTRAINT ALL" in execute_calls
        assert "SET IDENTITY_INSERT [bronze].[Order Detail] ON" in execute_calls
        assert any(sql.startswith("INSERT INTO [bronze].[Order Detail] ") for sql in executemany_calls)
        assert "SET IDENTITY_INSERT [bronze].[Order Detail] OFF" in execute_calls

    def test_no_identity_insert_when_no_identity_columns(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[Product]",)],  # sys.tables (trigger disable)
            [],                         # _get_not_null_defaults
            [],                         # _get_identity_columns: no identity cols
            [(1, "Widget")],            # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

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

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
        execute_calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        identity_calls = [c for c in execute_calls if "IDENTITY_INSERT" in c]
        assert len(identity_calls) == 0, f"Unexpected IDENTITY_INSERT calls: {identity_calls}"


class TestNotNullDefaultsQuery:
    """Regression coverage for the NOT NULL defaults lookup SQL."""

    def test_plain_schema_table_groups_or_predicates_before_and_filters(self) -> None:
        cursor = MagicMock()
        cursor.fetchall.return_value = []

        _get_not_null_defaults(cursor, "dbo.Product")

        sql = cursor.execute.call_args.args[0]
        assert sql.index("WHERE (") < sql.index(") AND c.IS_NULLABLE = 'NO'")
        assert sql.index("OR '[' + c.TABLE_SCHEMA + '].[' + c.TABLE_NAME + ']' = ?") < sql.index(") AND c.IS_NULLABLE = 'NO'")

    def test_identity_insert_toggles_per_table(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.fetchone.return_value = (
            "CREATE PROCEDURE [dbo].[usp_load] AS BEGIN SELECT 1 END",
        )
        # Two tables: first has identity, second does not
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[SalesOrderHeader]",), ("[bronze].[SalesOrderDetail]",)],  # sys.tables (trigger disable)
            [],                         # _get_not_null_defaults for table 1
            [("OrderID",)],             # _get_identity_columns for table 1
            [],                         # _get_not_null_defaults for table 2
            [],                         # _get_identity_columns for table 2
            [(1, "result")],            # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("value",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

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

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
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
            [],                         # _get_not_null_defaults
            [],                         # _get_identity_columns
            [(1, "Widget")],            # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

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

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
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
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_empty",
            "target_table": "[silver].[DimProduct]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {"table": "[bronze].[Product]", "rows": []},
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
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
        # Calls: sys.tables, not_null_defaults, identity_cols, SELECT * result
        sandbox_cursor.fetchall.side_effect = [
            [("[bronze].[Product]",), ("[silver].[DimProduct]",), ("[dbo].[Config]",)],  # sys.tables (trigger disable)
            [],                                          # _get_not_null_defaults
            [],                                          # _get_identity_columns
            [(1, "Widget")],                             # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

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

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
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
            [],                                          # _get_not_null_defaults
            [],                                          # _get_identity_columns
            [(1, "Widget")],                             # SELECT * FROM target
        ]
        sandbox_cursor.description = [("id",), ("name",)]

        fake_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=MagicMock(), sandbox_cursor=sandbox_cursor)

        scenario = {
            "name": "test_order",
            "target_table": "[silver].[T1]",
            "procedure": "[dbo].[usp_load]",
            "given": [
                {"table": "[bronze].[S1]", "rows": [{"id": 1}]},
            ],
        }

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            result = backend.execute_scenario(sandbox_db="__test_abc123", scenario=scenario)

        assert result.status == "ok"
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


class TestOutputModels:
    """Validate Pydantic output models for sandbox and test-harness commands."""

    def test_execute_output_valid(self) -> None:
        result = TestHarnessExecuteOutput.model_validate({
            "schema_version": "1.0",
            "scenario_name": "test_scenario",
            "status": "ok",
            "ground_truth_rows": [{"id": 1, "name": "Widget"}],
            "row_count": 1,
            "errors": [],
        })
        assert result.status == "ok"
        assert result.row_count == 1
        assert result.ground_truth_rows == [{"id": 1, "name": "Widget"}]

    def test_execute_output_error(self) -> None:
        result = TestHarnessExecuteOutput.model_validate({
            "schema_version": "1.0",
            "scenario_name": "test_scenario",
            "status": "error",
            "ground_truth_rows": [],
            "row_count": 0,
            "errors": [{"code": "SCENARIO_FAILED", "message": "connection refused"}],
        })
        assert result.status == "error"
        assert result.errors[0].code == "SCENARIO_FAILED"

    def test_sandbox_up_output_ok(self) -> None:
        result = SandboxUpOutput.model_validate({
            "sandbox_database": "__test_abc_123",
            "status": "ok",
            "tables_cloned": ["dbo.Product"],
            "views_cloned": ["dbo.vProduct"],
            "procedures_cloned": ["dbo.usp_load"],
            "errors": [],
        })
        assert result.status == "ok"
        assert result.tables_cloned == ["dbo.Product"]
        assert result.views_cloned == ["dbo.vProduct"]

    def test_sandbox_up_output_error(self) -> None:
        result = SandboxUpOutput.model_validate({
            "sandbox_database": "__test_abc_123",
            "status": "error",
            "tables_cloned": [],
            "views_cloned": [],
            "procedures_cloned": [],
            "errors": [{"code": "SANDBOX_UP_FAILED", "message": "connection refused"}],
        })
        assert result.status == "error"
        assert result.errors[0].code == "SANDBOX_UP_FAILED"

    def test_sandbox_down_output_ok(self) -> None:
        result = SandboxDownOutput.model_validate({
            "sandbox_database": "__test_abc_123",
            "status": "ok",
        })
        assert result.status == "ok"
        assert result.errors == []

    def test_sandbox_down_output_error(self) -> None:
        result = SandboxDownOutput.model_validate({
            "sandbox_database": "__test_abc_123",
            "status": "error",
            "errors": [{"code": "SANDBOX_DOWN_FAILED", "message": "timeout"}],
        })
        assert result.status == "error"
        assert result.errors[0].message == "timeout"

    def test_sandbox_status_output_exists(self) -> None:
        result = SandboxStatusOutput.model_validate({
            "sandbox_database": "__test_abc_123",
            "status": "ok",
            "exists": True,
            "has_content": True,
            "tables_count": 3,
            "views_count": 1,
            "procedures_count": 2,
        })
        assert result.exists is True
        assert result.has_content is True

    def test_sandbox_status_output_not_found(self) -> None:
        result = SandboxStatusOutput.model_validate({
            "sandbox_database": "__test_abc_123",
            "status": "not_found",
            "exists": False,
        })
        assert result.exists is False
        assert result.status == "not_found"

    def test_execute_spec_output_valid(self) -> None:
        result = ExecuteSpecOutput.model_validate({
            "schema_version": "1.0",
            "sandbox_database": "__test_abc_123",
            "spec_path": "test-specs/silver.dimproduct.json",
            "total": 2,
            "ok": 1,
            "failed": 1,
            "results": [
                {"scenario_name": "test_a", "status": "ok", "row_count": 3, "errors": []},
                {"scenario_name": "test_b", "status": "error", "row_count": 0,
                 "errors": [{"code": "SCENARIO_FAILED", "message": "timeout"}]},
            ],
        })
        assert result.total == 2
        assert result.results[0].status == "ok"
        assert result.results[1].errors[0].code == "SCENARIO_FAILED"

    # ── Negative cases: extra="forbid" rejects unknown fields ────────────

    def test_sandbox_up_rejects_extra_field(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="extra_field"):
            SandboxUpOutput.model_validate({
                "sandbox_database": "__test_abc_123",
                "status": "ok",
                "tables_cloned": [],
                "views_cloned": [],
                "procedures_cloned": [],
                "errors": [],
                "extra_field": "unexpected",
            })

    def test_execute_output_rejects_invalid_status(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            TestHarnessExecuteOutput.model_validate({
                "schema_version": "1.0",
                "scenario_name": "test",
                "status": "unknown",
                "ground_truth_rows": [],
                "row_count": 0,
                "errors": [],
            })

    def test_sandbox_down_rejects_missing_required(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            SandboxDownOutput.model_validate({"status": "ok"})

    def test_error_entry_rejects_extra_field(self) -> None:
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="severity"):
            ErrorEntry.model_validate({"code": "ERR", "message": "msg", "severity": "high"})

    def test_test_spec_per_item_valid(self) -> None:
        spec = TestSpec.model_validate({
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
        })
        assert spec.item_id == "silver.dimproduct"
        assert spec.status == "ok"
        assert spec.coverage == "complete"
        assert len(spec.branch_manifest) == 1
        assert spec.branch_manifest[0].id == "merge_matched_update"
        assert spec.branch_manifest[0].statement_index == 0
        assert len(spec.unit_tests) == 1
        assert spec.unit_tests[0].name == "test_merge_matched"
        assert spec.unit_tests[0].expect is not None
        assert spec.unit_tests[0].expect.rows == [{"product_key": 1}]
        assert spec.validation.passed is True

    def test_test_spec_output_valid(self) -> None:
        output = TestSpecOutput.model_validate({
            "schema_version": "1.0",
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
        })
        assert isinstance(output, TestSpecOutput)
        assert output.schema_version == "1.0"
        assert len(output.results) == 1
        assert output.summary.total == 1
        assert output.summary.ok == 1
