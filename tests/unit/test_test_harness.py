"""Unit tests for the test-harness CLI and sandbox backends."""

from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend
from shared.sandbox.sql_server import SqlServerSandbox, _validate_run_id

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


# ── SQL Server backend (mocked pyodbc) ───────────────────────────────────────


class TestSqlServerSandboxUp:
    """Test sandbox_up generates correct SQL via mocked pyodbc."""

    def _make_backend(self) -> SqlServerSandbox:
        return SqlServerSandbox(
            host="localhost",
            port="1433",
            database="TestDB",
            password="TestPass123",
        )

    @patch("shared.sandbox.sql_server.pyodbc")
    def test_sandbox_up_creates_database(self, mock_pyodbc: MagicMock) -> None:
        backend = self._make_backend()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Source DB query returns tables
        mock_source_conn = MagicMock()
        mock_source_cursor = MagicMock()
        mock_source_conn.cursor.return_value = mock_source_cursor
        mock_source_cursor.fetchall.side_effect = [
            # Tables query
            [("dbo", "Product"), ("silver", "DimProduct")],
            # Procedures query
            [("dbo", "usp_load", "CREATE PROCEDURE dbo.usp_load AS BEGIN SELECT 1 END")],
        ]

        # Sandbox connection for schema creation and SELECT INTO
        mock_sandbox_conn = MagicMock()
        mock_sandbox_cursor = MagicMock()
        mock_sandbox_conn.cursor.return_value = mock_sandbox_cursor

        # connect() returns different connections based on call order
        mock_pyodbc.connect.side_effect = [
            mock_conn,           # initial connection (CREATE DATABASE)
            mock_sandbox_conn,   # sandbox connection (CREATE SCHEMA, SELECT INTO)
            mock_source_conn,    # source connection (list tables + procedures)
        ]

        result = backend.sandbox_up(
            run_id="test-run-id",
            schemas=["dbo", "silver"],
            source_database="TestDB",
        )

        assert result["status"] in ("ok", "partial")
        assert result["run_id"] == "test-run-id"
        assert "__test_" in result["sandbox_database"]
        assert result["tables_cloned"] == ["dbo.Product", "silver.DimProduct"]
        assert result["procedures_cloned"] == ["dbo.usp_load"]

        # Verify CREATE DATABASE was called
        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        create_db_calls = [c for c in calls if "CREATE DATABASE" in c]
        assert len(create_db_calls) == 1

    @patch("shared.sandbox.sql_server.pyodbc")
    def test_sandbox_down_drops_database(self, mock_pyodbc: MagicMock) -> None:
        backend = self._make_backend()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        result = backend.sandbox_down(run_id="test-run-id")

        assert result["status"] == "ok"
        assert result["run_id"] == "test-run-id"

        calls = [str(c) for c in mock_cursor.execute.call_args_list]
        drop_calls = [c for c in calls if "DROP DATABASE" in c]
        assert len(drop_calls) == 1


# ── Execute scenario (mocked) ────────────────────────────────────────────────


class TestSqlServerExecuteScenario:
    def _make_backend(self) -> SqlServerSandbox:
        return SqlServerSandbox(
            host="localhost", port="1433", database="TestDB", password="pass",
        )

    @patch("shared.sandbox.sql_server.pyodbc")
    def test_execute_captures_ground_truth(self, mock_pyodbc: MagicMock) -> None:
        backend = self._make_backend()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn

        # Mock SELECT * result
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Widget")]

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

        result = backend.execute_scenario(run_id="test-run", scenario=scenario)

        assert result["status"] == "ok"
        assert result["row_count"] == 1
        assert result["ground_truth_rows"] == [{"id": 1, "name": "Widget"}]
        assert result["scenario_name"] == "test_insert_new_product"


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
                            "model": "stg_dimproduct",
                            "given": [
                                {
                                    "input": "source('bronze', 'product')",
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
