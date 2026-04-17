"""Cross-dialect view materialization, execute_select, compare_two_sql tests."""

from __future__ import annotations

import json
import tempfile
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from shared.output_models.sandbox import TestHarnessExecuteOutput
from shared.sandbox.oracle import OracleSandbox
from shared.sandbox.sql_server import (
    SqlServerSandbox,
    _import_pyodbc,
)


from .conftest import _make_backend, _mock_connect_factory


# ── compare_two_sql (SQL Server rollback) ────────────────────────────────────


class TestCompareTwoSqlSqlServerRollback:
    """Verify compare_two_sql calls conn.rollback() on PARSEONLY failure."""

    def test_rollback_called_on_parse_error(self) -> None:
        backend = _make_backend()
        pyodbc = _import_pyodbc()

        cursor = MagicMock()
        call_count = 0

        def _execute_side_effect(sql, *args):
            nonlocal call_count
            call_count += 1
            if sql == "SET PARSEONLY ON":
                return None
            if call_count == 3:
                raise pyodbc.Error("syntax error near ...")
            return None

        cursor.execute.side_effect = _execute_side_effect
        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_connect(*, database=None):
            yield conn

        with patch.object(backend, "_connect", side_effect=_fake_connect), \
             patch.object(backend._fixtures, "ensure_view_tables", return_value=[]):
            result = backend.compare_two_sql(
                sandbox_db="SBX_ABC123000000",
                sql_a="SELECT bad syntax",
                sql_b="SELECT 1",
                fixtures=[],
            )

        assert result["status"] == "error"
        assert result["errors"][0]["code"] == "SQL_SYNTAX_ERROR"
        conn.rollback.assert_called()


# ── _ensure_view_tables (SQL Server) ─────────────────────────────────────────


class TestEnsureViewTablesSqlServer:
    """Unit tests for SqlServerSandbox._ensure_view_tables."""

    def test_view_ctas_executed(self) -> None:
        """A view in the source DB is materialised as an empty table in the sandbox."""
        backend = _make_backend()
        source_cursor = MagicMock()
        source_cursor.fetchall.side_effect = [
            [(1,)],  # object IS a view (INFORMATION_SCHEMA.VIEWS)
            [],      # _get_identity_columns
            [("id", "int", None, 10, 0, None, "NO")],  # INFORMATION_SCHEMA.COLUMNS
        ]

        sandbox_cursor = MagicMock()

        sandbox_connect = _mock_connect_factory(sandbox_cursor=sandbox_cursor)
        source_connect = _mock_connect_factory(source_cursor=source_cursor)

        given = [{"table": "[silver].[vw_product]", "rows": []}]

        with patch.object(backend, "_connect", side_effect=sandbox_connect), patch.object(
            backend, "_connect_source", side_effect=source_connect
        ):
            materialized = backend._fixtures.ensure_view_tables("SBX_ABC123000000", given)

        assert materialized == ["silver.vw_product"]
        calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        create_calls = [c for c in calls if "CREATE TABLE [silver].[vw_product] ([id] int NOT NULL)" in c]
        assert len(create_calls) == 1

    def test_base_table_skipped(self) -> None:
        """A base table (not a view) is not CTASed — it is already cloned by _clone_tables."""
        backend = _make_backend()
        source_cursor = MagicMock()
        source_cursor.fetchall.return_value = []  # NOT a view

        sandbox_cursor = MagicMock()

        fake_connect = _mock_connect_factory(
            default_cursor=source_cursor,
            sandbox_cursor=sandbox_cursor,
        )

        given = [{"table": "[bronze].[Currency]", "rows": []}]

        with patch.object(backend, "_connect", side_effect=fake_connect), patch.object(
            backend, "_connect_source", side_effect=fake_connect
        ):
            materialized = backend._fixtures.ensure_view_tables("SBX_ABC123000000", given)

        assert materialized == []
        sandbox_cursor.execute.assert_not_called()

    def test_stale_object_dropped_before_ctas(self) -> None:
        """If DROP raises pyodbc.Error, the exception is swallowed and CTAS still runs."""
        pyodbc = pytest.importorskip("pyodbc")
        backend = _make_backend()
        source_cursor = MagicMock()
        source_cursor.fetchall.side_effect = [
            [(1,)],  # IS a view (INFORMATION_SCHEMA.VIEWS)
            [],      # _get_identity_columns
            [("id", "int", None, 10, 0, None, "NO")],  # INFORMATION_SCHEMA.COLUMNS
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
            materialized = backend._fixtures.ensure_view_tables("SBX_ABC123000000", given)

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
            cdb_service="FREEPDB1",
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
        def _fake_connect(*args, **kwargs):
            conn = MagicMock()
            conn.cursor.return_value = cursor
            yield conn

        given = [{"table": "SH.VW_PRODUCT", "rows": []}]

        with patch.object(backend, "_connect_sandbox", side_effect=_fake_connect), patch.object(
            backend, "_connect_source", side_effect=_fake_connect
        ):
            materialized = backend._fixtures.ensure_view_tables("SBX_ABC123000000", given)

        assert materialized == ["SH.VW_PRODUCT"]
        calls = [str(c) for c in cursor.execute.call_args_list]
        ctas_calls = [c for c in calls if "CREATE TABLE" in c]
        assert len(ctas_calls) == 1

    def test_base_table_skipped(self) -> None:
        """A base table (not a view) is not CTASed."""
        backend = self._make_oracle_backend()
        cursor = MagicMock()
        cursor.fetchone.return_value = None  # NOT a view

        @contextmanager
        def _fake_connect(*args, **kwargs):
            conn = MagicMock()
            conn.cursor.return_value = cursor
            yield conn

        given = [{"table": "SH.CHANNELS", "rows": []}]

        with patch.object(backend, "_connect_sandbox", side_effect=_fake_connect), patch.object(
            backend, "_connect_source", side_effect=_fake_connect
        ):
            materialized = backend._fixtures.ensure_view_tables("SBX_ABC123000000", given)

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
            None,                     # DROP VIEW succeeds
            None,                     # CREATE TABLE succeeds
        ]

        @contextmanager
        def _fake_source_connect(**kwargs):
            conn = MagicMock()
            conn.cursor.return_value = source_cursor
            yield conn

        @contextmanager
        def _fake_sandbox_connect(name: str):
            conn = MagicMock()
            conn.cursor.return_value = sandbox_cursor
            yield conn

        given = [{"table": "SH.VW_STALE", "rows": []}]

        with patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox_connect), patch.object(
            backend, "_connect_source", side_effect=_fake_source_connect
        ):
            materialized = backend._fixtures.ensure_view_tables("SBX_ABC123000000", given)

        assert materialized == ["SH.VW_STALE"]
        calls = [str(c) for c in sandbox_cursor.execute.call_args_list]
        create_calls = [c for c in calls if 'CREATE TABLE "SH"."VW_STALE" ("ID" NUMBER(10,0) NOT NULL)' in c]
        assert len(create_calls) == 1

    def test_clone_procedures_quotes_procedure_name(self) -> None:
        backend = self._make_oracle_backend()
        source_cursor = MagicMock()
        source_cursor.fetchall.side_effect = [
            [("Proc$Load",)],
            [("PROCEDURE Proc$Load AS\n",), ("BEGIN NULL; END Proc$Load;",)],
        ]
        sandbox_cursor = MagicMock()

        cloned, errors = backend._clone_procedures(
            source_cursor,
            sandbox_cursor,
            "SBX_ABC123000000",
            "SH",
        )

        assert cloned == ["SH.Proc$Load"]
        assert errors == []
        ddl = sandbox_cursor.execute.call_args.args[0]
        assert 'PROCEDURE "SBX_ABC123000000"."Proc$Load"' in ddl


# ── execute_select ─────────────────────────────────────────────────────────


class TestExecuteSelectSqlServer:
    """Unit tests for SqlServerSandbox.execute_select."""

    def test_happy_path_returns_rows(self) -> None:
        """execute_select seeds fixtures, runs SELECT, returns rows."""
        backend = SqlServerSandbox(
            host="localhost", port="1433",
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
             patch.object(backend._fixtures, "ensure_view_tables", return_value=[]), \
             patch.object(backend._fixtures, "seed_fixtures"):
            result = backend.execute_select(
                sandbox_db="SBX_ABC123000000",
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
            host="localhost", port="1433",
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
             patch.object(backend._fixtures, "ensure_view_tables", return_value=[]), \
             patch.object(backend._fixtures, "seed_fixtures"):
            result = backend.execute_select(
                sandbox_db="SBX_ABC123000000",
                sql="SELECT id FROM [silver].[Empty]",
                fixtures=[],
            )

        assert result.status == "ok"
        assert result.row_count == 0
        assert result.ground_truth_rows == []

    def test_rejects_write_sql(self) -> None:
        """execute_select rejects SQL containing write operations."""
        backend = SqlServerSandbox(
            host="localhost", port="1433",
            password="pw", user="sa", driver="ODBC Driver 18 for SQL Server",
        )
        with pytest.raises(ValueError, match="write operation"):
            backend.execute_select(
                sandbox_db="SBX_ABC123000000",
                sql="INSERT INTO [silver].[T] VALUES (1)",
                fixtures=[],
            )


class TestExecuteSelectOracle:
    """Unit tests for OracleSandbox.execute_select."""

    def test_happy_path_returns_rows(self) -> None:
        """execute_select seeds fixtures, runs SELECT, returns rows."""
        backend = OracleSandbox(
            host="localhost", port="1521", cdb_service="FREEPDB1",
            password="pw", admin_user="sys", source_schema="SH",
        )
        cursor = MagicMock()
        cursor.description = [("ID",), ("NAME",)]
        cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        conn = MagicMock()
        conn.cursor.return_value = cursor

        @contextmanager
        def _fake_sandbox(name: str):
            yield conn

        with patch.object(backend, "_connect_sandbox", side_effect=_fake_sandbox), \
             patch.object(backend._fixtures, "ensure_view_tables", return_value=[]), \
             patch.object(backend._fixtures, "seed_fixtures"):
            result = backend.execute_select(
                sandbox_db="SBX_ABC123000000",
                sql='SELECT "ID", "NAME" FROM "SH"."CHANNELS"',
                fixtures=[],
            )

        assert result.status == "ok"
        assert result.row_count == 2
        assert len(result.ground_truth_rows) == 2
        conn.rollback.assert_called_once()


# ── execute_spec view routing ──────────────────────────────────────────────


class TestExecuteSpecViewRouting:
    """Verify execute_spec routes view entries (no procedure) to execute_select."""

    def test_view_entry_calls_execute_select(self) -> None:
        """Test entry with sql (no procedure) calls execute_select, not execute_scenario."""
        from shared import test_harness
        from typer.testing import CliRunner

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
                 patch.object(test_harness, "_resolve_sandbox_db", return_value=("SBX_ABC000000000", {})):
                runner.invoke(
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
                 patch.object(test_harness, "_resolve_sandbox_db", return_value=("SBX_ABC000000000", {})):
                runner.invoke(
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

    def test_partial_failure_exits_non_zero(self, tmp_path) -> None:
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
            patch.object(test_harness, "_resolve_sandbox_db", return_value=("SBX_ABC000000000", {})),
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

    def test_manifest_permission_error_uses_json_error_path(self, tmp_path) -> None:
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
