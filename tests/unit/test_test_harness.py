"""Unit tests for the test-harness CLI and sandbox backends."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from shared.sandbox import get_backend
from shared.sandbox.base import SandboxBackend
from shared.sandbox.sql_server import SqlServerSandbox, _validate_identifier, _validate_run_id

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
    def test_execute_captures_ground_truth(self) -> None:
        backend = _make_backend()
        sandbox_cursor = MagicMock()
        sandbox_cursor.description = [("id",), ("name",)]
        sandbox_cursor.fetchall.return_value = [(1, "Widget")]

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
