"""Tests for extraction: DMF queries, extract validation, error handling."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")

from shared.sqlserver_extract import _run_dmf_queries
from tests.helpers import run_setup_ddl_cli as _run_cli


def test_run_dmf_queries_escapes_single_quotes(tmp_path) -> None:
    conn = MagicMock()
    list_cursor = MagicMock()
    dmf_cursor = MagicMock()

    list_cursor.fetchall.return_value = [("o'brien", "proc'name")]
    dmf_cursor.fetchall.return_value = []

    conn.cursor.side_effect = [list_cursor, dmf_cursor]

    _run_dmf_queries(
        conn=conn,
        schemas=["dbo"],
        object_type_filter="P",
        staging_dir=tmp_path,
        filename="proc_dmf.json",
    )

    dmf_sql = dmf_cursor.execute.call_args.args[0]
    assert "'o''brien' AS referencing_schema" in dmf_sql
    assert "'proc''name' AS referencing_name" in dmf_sql


class _FakeSqlCursor:
    def __init__(self, failure_map: dict[str, Exception] | None = None) -> None:
        self.failure_map = failure_map or {}
        self.description = []
        self.last_sql = ""

    def execute(self, sql: str):
        self.last_sql = sql
        for needle, exc in self.failure_map.items():
            if needle in sql:
                raise exc
        return self

    def fetchall(self):
        return []

    def close(self) -> None:
        return None


class _FakeSqlConn:
    def __init__(self, failure_map: dict[str, Exception] | None = None) -> None:
        self.failure_map = failure_map or {}

    def cursor(self):
        return _FakeSqlCursor(self.failure_map)

    def close(self) -> None:
        return None


def test_run_extract_fails_loudly_on_manifest_read_error(tmp_path: Path) -> None:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
    from shared import setup_ddl
    from shared.setup_ddl_support import extract as setup_ddl_extract

    (tmp_path / "manifest.json").write_text(
        '{"technology": "sql_server", "dialect": "tsql"}',
        encoding="utf-8",
    )

    with patch.object(setup_ddl_extract, "read_manifest_strict", side_effect=ValueError("manifest.json is not valid JSON: boom")):
        with pytest.raises(ValueError, match="manifest.json is not valid JSON"):
            setup_ddl.run_extract(tmp_path, database="MigrationTest", schemas=["dbo"])


def test_sqlserver_optional_metadata_unsupported_feature_writes_empty_files(tmp_path: Path) -> None:
    from shared import sqlserver_extract

    conn = _FakeSqlConn(
        {
            "FROM sys.change_tracking_tables": RuntimeError("Invalid object name 'sys.change_tracking_tables'"),
            "FROM sys.sensitivity_classifications": RuntimeError("Invalid object name 'sys.sensitivity_classifications'"),
        }
    )

    with (
        patch.object(sqlserver_extract, "_sql_server_connect", return_value=conn),
        patch.object(sqlserver_extract, "_rows_to_dicts", return_value=[]),
    ):
        sqlserver_extract.run_sqlserver_extraction(tmp_path, database="MigrationTest", schemas=["dbo"])

    assert json.loads((tmp_path / "change_tracking.json").read_text(encoding="utf-8")) == []
    assert json.loads((tmp_path / "sensitivity.json").read_text(encoding="utf-8")) == []


def test_sqlserver_optional_metadata_unexpected_error_raises(tmp_path: Path) -> None:
    from shared import sqlserver_extract

    conn = _FakeSqlConn(
        {"FROM sys.change_tracking_tables": RuntimeError("permission denied")}
    )

    with (
        patch.object(sqlserver_extract, "_sql_server_connect", return_value=conn),
        patch.object(sqlserver_extract, "_rows_to_dicts", return_value=[]),
    ):
        with pytest.raises(RuntimeError, match="permission denied"):
            sqlserver_extract.run_sqlserver_extraction(tmp_path, database="MigrationTest", schemas=["dbo"])


# ── Unit: extract arg validation ─────────────────────────────────────────────


class TestExtractValidation:
    def test_missing_schemas_errors(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--project-root", str(tmp_path),
        ])
        assert result.returncode != 0

    def test_missing_database_errors_for_sql_server(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ])
        assert result.returncode != 0
        assert "database" in result.stderr.lower()

    def test_missing_manifest_errors(self, tmp_path):
        result = _run_cli([
            "extract",
            "--database", "SomeDB",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ])
        assert result.returncode != 0

    def test_database_not_required_for_oracle(self, tmp_path):
        """Oracle extract should not error on missing --database (validated differently)."""
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        # Without ORACLE_* env vars set, this will fail at connection — not at arg validation
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ])
        # Should NOT fail with exit code 1 (arg validation error)
        # It will fail with exit code 2 (connection error) or succeed
        assert "database is required" not in result.stderr.lower()
