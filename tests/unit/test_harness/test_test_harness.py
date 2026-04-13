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
from shared.sandbox.base import SandboxBackend, generate_sandbox_name
from shared.sandbox.oracle import (
    OracleSandbox,
    _validate_oracle_identifier,
    _validate_oracle_sandbox_name,
)
from shared.sandbox.duckdb import DuckDbSandbox
from shared.sandbox.base import serialize_rows as _serialize_rows
from shared.output_models.sandbox import (
    ErrorEntry,
    ExecuteSpecOutput,
    ExecuteSpecResult,
    SandboxDownOutput,
    SandboxStatusOutput,
    SandboxUpOutput,
    TestHarnessExecuteOutput,
)
from shared.output_models.test_specs import TestSpec, TestSpecOutput
from shared.sandbox.sql_server import (
    SqlServerSandbox,
    _detect_remote_exec_target,
    _get_identity_columns,
    _get_not_null_defaults,
    _validate_identifier,
    _validate_sandbox_db_name,
)

FIXTURES = Path(__file__).parent / "fixtures"


# ── Backend registry ─────────────────────────────────────────────────────────


class TestBackendRegistry:
    def test_sql_server_returns_correct_class(self) -> None:
        cls = get_backend("sql_server")
        assert cls is SqlServerSandbox

    def test_oracle_returns_oracle_sandbox(self) -> None:
        cls = get_backend("oracle")
        assert cls is OracleSandbox

    def test_duckdb_returns_duckdb_sandbox(self) -> None:
        cls = get_backend("duckdb")
        assert cls is DuckDbSandbox

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
        assert backend.database == "master"
        assert backend.user == "admin"
        assert backend.driver == "FreeTDS"
        assert backend.source_user == "source_user"

    def test_connect_cant_open_lib_raises_runtime_error(self) -> None:
        backend = SqlServerSandbox(
            host="localhost", port="1433", database="testdb", password="pass",
        )
        with patch("shared.sandbox.sql_server._pyodbc") as mock_pyodbc:
            mock_pyodbc.Error = type("Error", (Exception,), {})
            mock_pyodbc.connect.side_effect = mock_pyodbc.Error(
                "[unixODBC][Driver Manager]Can't open lib 'FreeTDS'"
            )
            with pytest.raises(RuntimeError, match="brew install freetds"):
                with backend._connect():
                    pass


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
    """Return a _connect side_effect that routes by database keyword arg.

    Routing:
    - database starts with '__test_' → sandbox_cursor (if set)
    - named non-sandbox database → source_cursor (if set)
    - database=None (source default) → default_cursor if set, else noop cursor
    - fallback → noop cursor that returns None for fetchone (not a view)

    The noop fallback keeps existing tests that don't configure every cursor
    from breaking when _ensure_view_tables opens a source connection.
    """
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
            noop = MagicMock()
            noop.fetchone.return_value = None  # "not a view" for _ensure_view_tables
            conn.cursor.return_value = noop
        yield conn
    return _fake_connect


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



# ── Sandbox down (mocked) ────────────────────────────────────────────────────


class TestSqlServerSandboxStatus:
    """Test sandbox_status checks database existence via mocked _connect."""

    def test_sandbox_status_exists(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchone.return_value = (1,)  # DB_ID returns non-None

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="__test_abc123")

        assert result.status == "ok"
        assert result.exists is True
        assert not hasattr(result, "run_id")

    def test_sandbox_status_not_found(self) -> None:
        backend = _make_backend()
        default_cursor = MagicMock()
        default_cursor.fetchone.return_value = (None,)  # DB_ID returns None

        fake_connect = _mock_connect_factory(default_cursor=default_cursor)

        with patch.object(backend, "_connect", side_effect=fake_connect):
            result = backend.sandbox_status(sandbox_db="__test_abc123")

        assert result.status == "not_found"
        assert result.exists is False


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
        })
        assert result.exists is True

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
        from shared.output_models.test_specs import TestSpec

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
        from shared.output_models.test_specs import TestSpecOutput

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


# ── CLI manifest routing ─────────────────────────────────────────────────────


class TestCLIManifestRouting:
    def test_load_manifest_returns_technology(self, tmp_path: Path) -> None:
        shutil.copy(FIXTURES / "manifest.json", tmp_path / "manifest.json")
        from shared.test_harness import _load_manifest

        manifest = _load_manifest(tmp_path)
        assert manifest["technology"] == "sql_server"
        assert manifest["runtime"]["source"]["connection"]["database"] == "TestDB"
        assert manifest["extraction"]["schemas"] == ["dbo", "silver"]

    def test_load_manifest_missing_raises(self, tmp_path: Path) -> None:
        from click.exceptions import Exit

        from shared.test_harness import _load_manifest

        with pytest.raises(Exit):
            _load_manifest(tmp_path)

    def test_load_manifest_accepts_runtime_only_technology(self, tmp_path: Path) -> None:
        from shared.test_harness import _load_manifest

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "runtime": {
                        "sandbox": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/duckdb/sandbox.duckdb"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        manifest = _load_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["technology"] == "duckdb"

    def test_create_backend_prefers_runtime_sandbox_technology(self) -> None:
        from shared.test_harness_support.manifest import _create_backend

        backend_cls = MagicMock()
        backend_cls.from_env.return_value = DuckDbSandbox(
            source_path=".runtime/duckdb/source.duckdb",
            sandbox_path=".runtime/duckdb/sandbox.duckdb",
        )
        with patch("shared.test_harness_support.manifest.get_backend", return_value=backend_cls) as mock_get_backend:
            backend = _create_backend(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "MigrationTest"},
                        },
                        "sandbox": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {
                                "path": ".runtime/duckdb/sandbox.duckdb",
                            },
                        },
                    },
                }
            )

        mock_get_backend.assert_called_once_with("duckdb")
        assert isinstance(backend, DuckDbSandbox)


# ── Manifest sandbox persistence ──────────────────────────────────────────────


def _write_fixture_manifest(dest: Path) -> None:
    """Copy the standard test manifest fixture to dest."""
    shutil.copy(FIXTURES / "manifest.json", dest / "manifest.json")


class TestWriteManifestSandbox:
    def test_persist_sandbox_to_manifest(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_run_123")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_run_123"
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"
        assert manifest["extraction"]["schemas"] == ["dbo", "silver"]

    def test_persist_overwrites_existing_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_old_run")
        write_manifest_sandbox(tmp_path, "__test_new_run")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_new_run"

    def test_missing_runtime_sandbox_raises(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TestDB"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="runtime.sandbox"):
            write_manifest_sandbox(tmp_path, "__test_run_123")

    def test_preserves_existing_duckdb_sandbox_role(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "duckdb",
                    "dialect": "duckdb",
                    "runtime": {
                        "source": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/duckdb/source.duckdb"},
                        },
                        "sandbox": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/duckdb/template.duckdb"},
                        },
                    },
                }
            ),
            encoding="utf-8",
        )

        write_manifest_sandbox(tmp_path, ".runtime/duckdb/sandbox.duckdb")

        manifest = read_manifest(tmp_path)
        assert manifest["runtime"]["sandbox"]["technology"] == "duckdb"
        assert manifest["runtime"]["sandbox"]["connection"]["path"] == ".runtime/duckdb/sandbox.duckdb"


class TestClearManifestSandbox:
    def test_clear_removes_sandbox_key(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_run_123")
        clear_manifest_sandbox(tmp_path)

        manifest = read_manifest(tmp_path)
        assert "sandbox" not in manifest.get("runtime", {})
        # Original fields are preserved
        assert manifest["technology"] == "sql_server"

    def test_clear_noop_when_no_sandbox(self, tmp_path: Path) -> None:
        _write_fixture_manifest(tmp_path)
        clear_manifest_sandbox(tmp_path)

        manifest = read_manifest(tmp_path)
        assert "sandbox" not in manifest.get("runtime", {})


class TestResolveSandboxDb:
    def test_reads_database_from_manifest(self, tmp_path: Path) -> None:
        from shared.test_harness import _resolve_sandbox_db

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_manifest_run")

        sandbox_db, manifest = _resolve_sandbox_db(tmp_path)
        assert sandbox_db == "__test_manifest_run"
        assert "technology" in manifest

    def test_missing_sandbox_exits(self, tmp_path: Path) -> None:
        from click.exceptions import Exit

        from shared.test_harness import _resolve_sandbox_db

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TestDB"},
                        }
                    },
                    "extraction": {
                        "schemas": ["dbo", "silver"],
                        "extracted_at": "2026-03-31T00:00:00Z",
                    },
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(Exit):
            _resolve_sandbox_db(tmp_path)

    def test_manifest_read_error_uses_strict_loader(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        from click.exceptions import Exit

        from shared.test_harness_support import manifest as manifest_helpers

        with patch.object(manifest_helpers, "read_manifest", side_effect=PermissionError("permission denied")):
            with pytest.raises(Exit) as exc_info:
                manifest_helpers._resolve_sandbox_db(tmp_path)

        assert exc_info.value.exit_code == 2
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "error"
        assert output["errors"][0]["code"] == "MANIFEST_READ_ERROR"


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
        backend_mock.sandbox_up.return_value = SandboxUpOutput(
            sandbox_database="__test_e2e_run",
            status="ok",
            tables_cloned=["dbo.Product"],
            views_cloned=[],
            procedures_cloned=[],
            errors=[],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_e2e_run"

    def test_sandbox_up_error_does_not_write_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_up.return_value = SandboxUpOutput(
            sandbox_database="__test_e2e_run",
            status="error",
            tables_cloned=[],
            views_cloned=[],
            procedures_cloned=[],
            errors=[ErrorEntry(code="CONNECT_FAILED", message="timeout")],
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["runtime"]["sandbox"]["connection"]["database"] == "__test_template"


class TestCLISandboxDownClears:
    """E2E: invoke sandbox-down via CliRunner and verify manifest.json is cleared."""

    def test_sandbox_down_clears_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_e2e_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="__test_e2e_run", status="ok",
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "sandbox" not in manifest.get("runtime", {})

    def test_sandbox_down_reads_sandbox_db_from_manifest(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_manifest_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_down.return_value = SandboxDownOutput(
            sandbox_database="__test_manifest_run", status="ok",
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-down", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_down.assert_called_once_with(sandbox_db="__test_manifest_run")


class TestCLIStatusFallback:
    """E2E: invoke sandbox-status, verify manifest-based sandbox_db resolution."""

    def test_sandbox_status_uses_manifest_sandbox_db(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_manifest_run")
        runner = CliRunner()

        backend_mock = MagicMock()
        backend_mock.sandbox_status.return_value = SandboxStatusOutput(
            sandbox_database="__test_manifest_run", status="ok", exists=True,
        )

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch("shared.test_harness._create_backend", return_value=backend_mock),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 0
        backend_mock.sandbox_status.assert_called_once_with(sandbox_db="__test_manifest_run")

    def test_sandbox_status_no_sandbox_in_manifest_exits(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "runtime": {
                        "source": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "TestDB"},
                        }
                    },
                    "extraction": {
                        "schemas": ["dbo", "silver"],
                        "extracted_at": "2026-03-31T00:00:00Z",
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()

        with (
            patch("shared.test_harness.resolve_project_root", return_value=tmp_path),
            patch.dict(os.environ, _cli_env(tmp_path)),
        ):
            result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["errors"][0]["code"] == "SANDBOX_NOT_CONFIGURED"


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
        write_manifest_sandbox(tmp_path, "__test_e2e_run")

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
        backend_mock.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_merge_matched",
            status="ok",
            ground_truth_rows=[{"ProductKey": 1, "Name": "Widget"}],
            row_count=1,
            errors=[],
        )

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
        write_manifest_sandbox(tmp_path, "__test_e2e_run")

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
            TestHarnessExecuteOutput(
                scenario_name="test_ok",
                status="ok",
                ground_truth_rows=[{"id": 1}],
                row_count=1,
                errors=[],
            ),
            TestHarnessExecuteOutput(
                scenario_name="test_fail",
                status="error",
                ground_truth_rows=[],
                row_count=0,
                errors=[ErrorEntry(code="SCENARIO_FAILED", message="insert failed")],
            ),
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

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["ok"] == 1
        assert output["failed"] == 1

    def test_execute_spec_all_fail_exits_1(self, tmp_path: Path) -> None:
        from typer.testing import CliRunner

        from shared.test_harness import app

        _write_fixture_manifest(tmp_path)
        write_manifest_sandbox(tmp_path, "__test_e2e_run")

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
        backend_mock.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_fail",
            status="error",
            ground_truth_rows=[],
            row_count=0,
            errors=[ErrorEntry(code="SCENARIO_FAILED", message="connection refused")],
        )

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

    def test_execute_spec_output_model(self) -> None:
        result = ExecuteSpecOutput.model_validate({
            "schema_version": "1.0",
            "sandbox_database": "__test_abc123",
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
        })
        assert result.ok == 1
        assert result.failed == 1
        assert len(result.results) == 2


# ── Corrupt JSON tests ──────────────────────────────────────────────


class TestCorruptJsonHandling:
    """Verify CLI commands handle corrupt JSON inputs gracefully."""

    def test_sandbox_up_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-up with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        result = runner.invoke(app, ["sandbox-up", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_sandbox_status_corrupt_manifest_exit_1(self, tmp_path: Path) -> None:
        """sandbox-status with corrupt manifest.json exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        (tmp_path / "manifest.json").write_text("{truncated", encoding="utf-8")
        runner = CliRunner()
        # sandbox-status reads runtime.sandbox from manifest, which will fail
        result = runner.invoke(app, ["sandbox-status", "--project-root", str(tmp_path)])
        assert result.exit_code == 1

    def test_execute_spec_corrupt_json_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with corrupt test-spec JSON exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "corrupt-spec.json"
        spec.write_text("{not valid json", encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "dialect": "tsql",
                    "technology": "sql_server",
                    "runtime": {
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "__test_abc123"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1

    def test_execute_spec_missing_required_fields_exit_1(self, tmp_path: Path) -> None:
        """execute-spec with valid JSON but missing unit_tests exits 1."""
        from typer.testing import CliRunner

        from shared.test_harness import app

        spec = tmp_path / "empty-spec.json"
        spec.write_text('{"model": "stg_test"}', encoding="utf-8")
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "dialect": "tsql",
                    "technology": "sql_server",
                    "runtime": {
                        "sandbox": {
                            "technology": "sql_server",
                            "dialect": "tsql",
                            "connection": {"database": "__test_abc123"},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        runner = CliRunner()
        result = runner.invoke(app, [
            "execute-spec", "--spec", str(spec), "--project-root", str(tmp_path),
        ])
        assert result.exit_code == 1


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


# ── DuckDbSandbox ────────────────────────────────────────────────────────────


class TestDuckDbSandbox:
    def test_from_env_requires_runtime_roles(self) -> None:
        with pytest.raises(ValueError, match="runtime.source.connection.path"):
            DuckDbSandbox.from_env(
                {
                    "runtime": {
                        "sandbox": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/duckdb/sandbox.duckdb"},
                        }
                    }
                }
            )

    def test_sandbox_up_copies_source_file(self, tmp_path: Path) -> None:
        import duckdb

        source_path = tmp_path / "source.duckdb"
        sandbox_path = tmp_path / "sandbox.duckdb"
        conn = duckdb.connect(str(source_path))
        conn.execute("create schema bronze")
        conn.execute("create table bronze.product(id integer, name varchar)")
        conn.close()

        backend = DuckDbSandbox(str(source_path), str(sandbox_path))
        result = backend.sandbox_up(["bronze"])

        assert result.status == "ok"
        assert sandbox_path.exists()
        assert result.tables_cloned == ["bronze.product"]
        conn = duckdb.connect(str(sandbox_path))
        try:
            rows = conn.execute("select * from bronze.product").fetchall()
        finally:
            conn.close()
        assert rows == []

    def test_execute_scenario_is_explicitly_unsupported(self, tmp_path: Path) -> None:
        source_path = tmp_path / "source.duckdb"
        sandbox_path = tmp_path / "sandbox.duckdb"
        source_path.write_bytes(b"")
        backend = DuckDbSandbox(str(source_path), str(sandbox_path))

        with pytest.raises(NotImplementedError, match="does not support procedure-based execute_scenario"):
            backend.execute_scenario(
                str(sandbox_path),
                {
                    "name": "scenario",
                    "procedure": "[silver].[usp_load_target]",
                    "target_table": "[silver].[Target]",
                    "given": [],
                },
            )

    def test_execute_select_rolls_back_fixtures(self, tmp_path: Path) -> None:
        import duckdb

        source_path = tmp_path / "source.duckdb"
        sandbox_path = tmp_path / "sandbox.duckdb"
        conn = duckdb.connect(str(source_path))
        conn.execute("create schema bronze")
        conn.execute("create table bronze.product(id integer, name varchar)")
        conn.close()
        shutil.copy2(source_path, sandbox_path)

        backend = DuckDbSandbox(str(source_path), str(sandbox_path))
        result = backend.execute_select(
            str(sandbox_path),
            'select id, name from bronze.product order by id',
            [{"table": "[bronze].[product]", "rows": [{"id": 1, "name": "Widget"}]}],
        )

        assert result.status == "ok"
        assert result.ground_truth_rows == [{"id": 1, "name": "Widget"}]

        conn = duckdb.connect(str(sandbox_path))
        try:
            rows = conn.execute("select count(*) from bronze.product").fetchone()
        finally:
            conn.close()
        assert rows == (0,)

    def test_compare_two_sql_reports_equivalence(self, tmp_path: Path) -> None:
        import duckdb

        source_path = tmp_path / "source.duckdb"
        sandbox_path = tmp_path / "sandbox.duckdb"
        conn = duckdb.connect(str(source_path))
        conn.execute("create schema bronze")
        conn.execute("create table bronze.product(id integer, name varchar)")
        conn.close()
        shutil.copy2(source_path, sandbox_path)

        backend = DuckDbSandbox(str(source_path), str(sandbox_path))
        result = backend.compare_two_sql(
            str(sandbox_path),
            "select id, name from bronze.product",
            "select id, name from bronze.product",
            [{"table": "[bronze].[product]", "rows": [{"id": 1, "name": "Widget"}]}],
        )

        assert result["status"] == "ok"
        assert result["equivalent"] is True
        assert result["a_count"] == 1
        assert result["b_count"] == 1


# ── _ensure_view_tables (SQL Server) ─────────────────────────────────────────


class TestEnsureViewTablesSqlServer:
    """Unit tests for SqlServerSandbox._ensure_view_tables."""

    def test_view_ctas_executed(self) -> None:
        """A view in the source DB is materialised as an empty table in the sandbox."""
        backend = _make_backend()
        source_cursor = MagicMock()
        source_cursor.fetchone.return_value = (1,)  # object IS a view
        source_cursor.fetchall.side_effect = [
            [],
            [("id", "int", None, 10, 0, None, "NO")],
        ]

        sandbox_cursor = MagicMock()

        sandbox_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=source_cursor)

        given = [{"table": "[silver].[vw_product]", "rows": []}]

        with patch.object(backend, "_connect", side_effect=sandbox_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            materialized = backend._ensure_view_tables("__test_abc123", given)

        assert materialized == ["silver.vw_product"]
        calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        create_calls = [c for c in calls if "CREATE TABLE [silver].[vw_product] ([id] int NOT NULL)" in c]
        assert len(create_calls) == 1

    def test_base_table_skipped(self) -> None:
        """A base table (not a view) is not CTASed — it is already cloned by _clone_tables."""
        backend = _make_backend()
        source_cursor = MagicMock()
        source_cursor.fetchone.return_value = None  # NOT a view

        sandbox_cursor = MagicMock()

        fake_connect = _mock_connect_factory(
            default_cursor=source_cursor,
            sandbox_cursor=sandbox_cursor,
        )

        given = [{"table": "[bronze].[Currency]", "rows": []}]

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=fake_connect
        ):
            materialized = backend._ensure_view_tables("__test_abc123", given)

        assert materialized == []
        sandbox_cursor.execute.assert_not_called()

    def test_stale_object_dropped_before_ctas(self) -> None:
        """If DROP raises pyodbc.Error, the exception is swallowed and CTAS still runs."""
        pyodbc = pytest.importorskip("pyodbc")
        backend = _make_backend()
        source_cursor = MagicMock()
        source_cursor.fetchone.return_value = (1,)  # IS a view
        source_cursor.fetchall.side_effect = [
            [],
            [("id", "int", None, 10, 0, None, "NO")],
        ]

        sandbox_cursor = MagicMock()
        sandbox_cursor.execute.side_effect = [
            pyodbc.Error,  # DROP TABLE IF EXISTS raises
            None,          # DROP VIEW IF EXISTS succeeds
            None,          # CREATE TABLE succeeds
        ]

        sandbox_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=source_cursor)

        given = [{"table": "[silver].[vw_stale]", "rows": []}]

        with patch.object(backend, "_connect", side_effect=sandbox_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            materialized = backend._ensure_view_tables("__test_abc123", given)

        assert materialized == ["silver.vw_stale"]
        calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        create_calls = [c for c in calls if "CREATE TABLE [silver].[vw_stale] ([id] int NOT NULL)" in c]
        assert len(create_calls) == 1


# ── _ensure_view_tables (Oracle) ──────────────────────────────────────────────


class TestEnsureViewTablesOracle:
    """Unit tests for OracleSandbox._ensure_view_tables."""

    def _make_oracle_backend(self) -> OracleSandbox:
        return OracleSandbox(
            host="localhost",
            port="1521",
            service="FREEPDB1",
            password="TestPass123",
            admin_user="sys",
            source_schema="SH",
        )

    def test_view_ctas_executed(self) -> None:
        """A view in the source schema is materialised as an empty table in the sandbox."""
        backend = self._make_oracle_backend()
        cursor = MagicMock()
        cursor.fetchone.return_value = (1,)  # object IS a view

        @contextmanager
        def _fake_connect(**kwargs):
            conn = MagicMock()
            conn.cursor.return_value = cursor
            yield conn

        given = [{"table": "VW_PRODUCT", "rows": []}]

        with patch.object(backend, "_connect", side_effect=_fake_connect), patch.object(
            backend, "_connect_source", side_effect=_fake_connect
        ):
            materialized = backend._ensure_view_tables("__test_abc123", given)

        assert materialized == ["VW_PRODUCT"]
        calls = [str(c) for c in cursor.execute.call_args_list]
        ctas_calls = [c for c in calls if "CREATE TABLE" in c]
        assert len(ctas_calls) == 1

    def test_base_table_skipped(self) -> None:
        """A base table (not a view) is not CTASed."""
        backend = self._make_oracle_backend()
        cursor = MagicMock()
        cursor.fetchone.return_value = None  # NOT a view

        @contextmanager
        def _fake_connect(**kwargs):
            conn = MagicMock()
            conn.cursor.return_value = cursor
            yield conn

        given = [{"table": "CHANNELS", "rows": []}]

        with patch.object(backend, "_connect", side_effect=_fake_connect), patch.object(
            backend, "_connect_source", side_effect=_fake_connect
        ):
            materialized = backend._ensure_view_tables("__test_abc123", given)

        assert materialized == []
        calls = [str(c) for c in cursor.execute.call_args_list]
        ctas_calls = [c for c in calls if "CREATE TABLE" in c]
        assert len(ctas_calls) == 0

    def test_stale_object_dropped_before_ctas(self) -> None:
        """If DROP raises oracledb.DatabaseError, the exception is swallowed and CTAS still runs."""
        import oracledb

        backend = self._make_oracle_backend()
        source_cursor = MagicMock()
        source_cursor.fetchone.return_value = (1,)  # IS a view
        source_cursor.fetchall.side_effect = [
            [("ID", "NUMBER", None, 10, 0, None, "N")],
        ]
        sandbox_cursor = MagicMock()
        sandbox_cursor.execute.side_effect = [
            oracledb.DatabaseError,   # DROP TABLE raises
            None,                     # CREATE TABLE succeeds
        ]

        @contextmanager
        def _fake_source_connect(**kwargs):
            conn = MagicMock()
            conn.cursor.return_value = source_cursor
            yield conn

        @contextmanager
        def _fake_sandbox_connect(**kwargs):
            conn = MagicMock()
            conn.cursor.return_value = sandbox_cursor
            yield conn

        given = [{"table": "VW_STALE", "rows": []}]

        with patch.object(backend, "_connect", side_effect=_fake_sandbox_connect), patch.object(
            backend, "_connect_source", side_effect=_fake_source_connect
        ):
            materialized = backend._ensure_view_tables("__test_abc123", given)

        assert materialized == ["VW_STALE"]
        calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        create_calls = [c for c in calls if 'CREATE TABLE "__test_abc123"."VW_STALE" ("ID" NUMBER(10,0) NOT NULL)' in c]
        assert len(create_calls) == 1


# ── execute_select ─────────────────────────────────────────────────────────


class TestExecuteSelectSqlServer:
    """Unit tests for SqlServerSandbox.execute_select."""

    def test_happy_path_returns_rows(self) -> None:
        """execute_select seeds fixtures, runs SELECT, returns rows."""
        backend = SqlServerSandbox(
            host="localhost", port="1433", database="testdb",
            password="pw", user="sa", driver="ODBC Driver 18 for SQL Server",
        )
        cursor = MagicMock()
        cursor.description = [("id",), ("name",)]
        cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_connect(*, database=None):
            yield conn

        with patch.object(backend, "_connect", side_effect=_fake_connect), \
             patch.object(backend, "_ensure_view_tables", return_value=[]), \
             patch.object(backend, "_seed_fixtures"):
            result = backend.execute_select(
                sandbox_db="__test_abc123",
                sql="SELECT id, name FROM [silver].[Customers]",
                fixtures=[],
            )

        assert result.status == "ok"
        assert result.row_count == 2
        assert len(result.ground_truth_rows) == 2
        assert result.errors == []
        conn.rollback.assert_called_once()

    def test_empty_result(self) -> None:
        """execute_select with no matching rows returns row_count=0."""
        backend = SqlServerSandbox(
            host="localhost", port="1433", database="testdb",
            password="pw", user="sa", driver="ODBC Driver 18 for SQL Server",
        )
        cursor = MagicMock()
        cursor.description = [("id",)]
        cursor.fetchall.return_value = []

        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_connect(*, database=None):
            yield conn

        with patch.object(backend, "_connect", side_effect=_fake_connect), \
             patch.object(backend, "_ensure_view_tables", return_value=[]), \
             patch.object(backend, "_seed_fixtures"):
            result = backend.execute_select(
                sandbox_db="__test_abc123",
                sql="SELECT id FROM [silver].[Empty]",
                fixtures=[],
            )

        assert result.status == "ok"
        assert result.row_count == 0
        assert result.ground_truth_rows == []

    def test_rejects_write_sql(self) -> None:
        """execute_select rejects SQL containing write operations."""
        backend = SqlServerSandbox(
            host="localhost", port="1433", database="testdb",
            password="pw", user="sa", driver="ODBC Driver 18 for SQL Server",
        )
        with pytest.raises(ValueError, match="write operation"):
            backend.execute_select(
                sandbox_db="__test_abc123",
                sql="INSERT INTO [silver].[T] VALUES (1)",
                fixtures=[],
            )


class TestExecuteSelectOracle:
    """Unit tests for OracleSandbox.execute_select."""

    def test_happy_path_returns_rows(self) -> None:
        """execute_select seeds fixtures, runs SELECT, returns rows."""
        backend = OracleSandbox(
            host="localhost", port="1521", service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cursor = MagicMock()
        cursor.description = [("ID",), ("NAME",)]
        cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_connect():
            yield conn

        with patch.object(backend, "_connect", side_effect=_fake_connect), \
             patch.object(backend, "_ensure_view_tables", return_value=[]), \
             patch.object(backend, "_seed_fixtures"):
            result = backend.execute_select(
                sandbox_db="__test_abc123",
                sql='SELECT "ID", "NAME" FROM "SH"."CHANNELS"',
                fixtures=[],
            )

        assert result.status == "ok"
        assert result.row_count == 2
        assert len(result.ground_truth_rows) == 2
        conn.rollback.assert_called_once()


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


# ── execute_spec view routing ──────────────────────────────────────────────


class TestExecuteSpecViewRouting:
    """Verify execute_spec routes view entries (no procedure) to execute_select."""

    def test_view_entry_calls_execute_select(self) -> None:
        """Test entry with sql (no procedure) calls execute_select, not execute_scenario."""
        from shared import test_harness
        from typer.testing import CliRunner
        import tempfile

        runner = CliRunner()
        spec = {
            "item_id": "silver.vw_test",
            "object_type": "view",
            "status": "ok",
            "coverage": "complete",
            "branch_manifest": [],
            "unit_tests": [
                {
                    "name": "test_view_filter",
                    "sql": "SELECT id FROM [silver].[source] WHERE active = 1",
                    "given": [
                        {"table": "[silver].[source]", "rows": [{"id": 1, "active": 1}]},
                    ],
                },
            ],
            "uncovered_branches": [],
            "warnings": [],
            "validation": {"status": "ok"},
            "errors": [],
        }

        mock_backend = MagicMock()
        mock_backend.execute_select.return_value = TestHarnessExecuteOutput(
            scenario_name="execute_select",
            status="ok",
            ground_truth_rows=[{"id": 1}],
            row_count=1,
            errors=[],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(spec, f)
            spec_path = f.name

        try:
            with patch.object(test_harness, "_create_backend", return_value=mock_backend), \
                 patch.object(test_harness, "_resolve_sandbox_db", return_value=("__test_abc", {})):
                result = runner.invoke(
                    test_harness.app,
                    ["execute-spec", "--spec", spec_path, "--project-root", "."],
                )

            # execute_select should have been called (not execute_scenario)
            mock_backend.execute_select.assert_called_once()
            mock_backend.execute_scenario.assert_not_called()

            # Verify ground truth was written back to spec
            with open(spec_path) as f:
                updated = json.load(f)
            assert updated["unit_tests"][0]["expect"]["rows"] == [{"id": 1}]
        finally:
            import os
            os.unlink(spec_path)

    def test_procedure_entry_calls_execute_scenario(self) -> None:
        """Test entry with procedure key calls execute_scenario, not execute_select."""
        from shared import test_harness
        from typer.testing import CliRunner
        import tempfile

        runner = CliRunner()
        spec = {
            "item_id": "silver.dimcustomer",
            "status": "ok",
            "coverage": "complete",
            "branch_manifest": [],
            "unit_tests": [
                {
                    "name": "test_merge_insert",
                    "target_table": "[silver].[DimCustomer]",
                    "procedure": "[dbo].[usp_load_DimCustomer]",
                    "given": [
                        {"table": "[bronze].[CustomerRaw]", "rows": [{"id": 1}]},
                    ],
                },
            ],
            "uncovered_branches": [],
            "warnings": [],
            "validation": {"status": "ok"},
            "errors": [],
        }

        mock_backend = MagicMock()
        mock_backend.execute_scenario.return_value = TestHarnessExecuteOutput(
            scenario_name="test_merge_insert",
            status="ok",
            ground_truth_rows=[{"id": 1}],
            row_count=1,
            errors=[],
        )

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(spec, f)
            spec_path = f.name

        try:
            with patch.object(test_harness, "_create_backend", return_value=mock_backend), \
                 patch.object(test_harness, "_resolve_sandbox_db", return_value=("__test_abc", {})):
                result = runner.invoke(
                    test_harness.app,
                    ["execute-spec", "--spec", spec_path, "--project-root", "."],
                )

            mock_backend.execute_scenario.assert_called_once()
            mock_backend.execute_select.assert_not_called()
        finally:
            import os
            os.unlink(spec_path)


class TestCompareSqlExitCodes:
    """Regression coverage for compare-sql CLI exit behavior."""

    def test_partial_failure_exits_non_zero(self, tmp_path: Path) -> None:
        from shared import test_harness
        from typer.testing import CliRunner

        runner = CliRunner()
        sql_a = tmp_path / "sql_a.sql"
        sql_b = tmp_path / "sql_b.sql"
        spec_path = tmp_path / "spec.json"

        sql_a.write_text("SELECT 1 AS value", encoding="utf-8")
        sql_b.write_text("SELECT 1 AS value", encoding="utf-8")
        spec_path.write_text(
            json.dumps(
                {
                    "item_id": "silver.dimproduct",
                    "status": "ok",
                    "coverage": "complete",
                    "branch_manifest": [],
                    "unit_tests": [
                        {"name": "test_ok", "given": []},
                        {"name": "test_fail", "given": []},
                    ],
                    "uncovered_branches": [],
                    "warnings": [],
                    "validation": {"status": "ok"},
                    "errors": [],
                }
            ),
            encoding="utf-8",
        )

        mock_backend = MagicMock()
        mock_backend.compare_two_sql.side_effect = [
            {
                "status": "ok",
                "equivalent": True,
                "a_count": 1,
                "b_count": 1,
                "a_minus_b": [],
                "b_minus_a": [],
                "errors": [],
            },
            {
                "status": "error",
                "equivalent": False,
                "a_count": 1,
                "b_count": 0,
                "a_minus_b": [{"value": 1}],
                "b_minus_a": [],
                "errors": [{"code": "COMPARE_FAILED", "message": "mismatch"}],
            },
        ]

        with (
            patch.object(test_harness, "resolve_project_root", return_value=tmp_path),
            patch.object(test_harness, "_resolve_sandbox_db", return_value=("__test_abc", {})),
            patch.object(test_harness, "_create_backend", return_value=mock_backend),
        ):
            result = runner.invoke(
                test_harness.app,
                [
                    "compare-sql",
                    "--sql-a-file", str(sql_a),
                    "--sql-b-file", str(sql_b),
                    "--spec", str(spec_path),
                    "--project-root", str(tmp_path),
                ],
            )

        assert result.exit_code == 1
        output = json.loads(result.output)
        assert output["passed"] == 1
        assert output["failed"] == 1

    def test_manifest_permission_error_uses_json_error_path(self, tmp_path: Path) -> None:
        from shared import test_harness
        from shared.test_harness_support import manifest as manifest_helpers
        from typer.testing import CliRunner

        runner = CliRunner()

        with (
            patch.object(test_harness, "resolve_project_root", return_value=tmp_path),
            patch.object(manifest_helpers, "read_manifest", side_effect=PermissionError("permission denied")),
        ):
            result = runner.invoke(
                test_harness.app,
                ["sandbox-up", "--project-root", str(tmp_path)],
            )

        assert result.exit_code == 2
        output = json.loads(result.output)
        assert output["status"] == "error"
        assert output["errors"][0]["code"] == "MANIFEST_READ_ERROR"
