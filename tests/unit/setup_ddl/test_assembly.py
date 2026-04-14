"""Tests for assemble-modules and assemble-tables subcommands."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")

from shared.sql_types import format_sql_type
from tests.helpers import run_setup_ddl_cli as _run_cli

from .conftest import _write_json


# ── Unit: assemble-modules ───────────────────────────────────────────────────


class TestAssembleModules:
    def test_joins_definitions_with_go(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "object_name": "usp_a", "definition": "CREATE PROC dbo.usp_a AS SELECT 1"},
            {"schema_name": "dbo", "object_name": "usp_b", "definition": "CREATE PROC dbo.usp_b AS SELECT 2"},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        project_root = tmp_path / "out"

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--project-root", str(project_root),
            "--type", "procedures",
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 2

        sql = (project_root / "ddl" / "procedures.sql").read_text()
        assert "CREATE PROC dbo.usp_a" in sql
        assert "\nGO\n" in sql

    def test_skips_null_definitions(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "object_name": "usp_a", "definition": "CREATE PROC dbo.usp_a AS SELECT 1"},
            {"schema_name": "dbo", "object_name": "usp_b", "definition": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        project_root = tmp_path / "out"

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--project-root", str(project_root),
            "--type", "procedures",
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 1

    def test_empty_input_writes_empty_file(self, tmp_path):
        input_file = tmp_path / "input.json"
        _write_json(input_file, [])
        project_root = tmp_path / "out"

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--project-root", str(project_root),
            "--type", "views",
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 0
        assert (project_root / "ddl" / "views.sql").read_text() == ""

    def test_invalid_type_rejected(self, tmp_path):
        input_file = tmp_path / "input.json"
        _write_json(input_file, [])

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--project-root", str(tmp_path / "out"),
            "--type", "tables",
        ])
        assert result.returncode != 0


# ── Unit: assemble-tables ────────────────────────────────────────────────────


class TestAssembleTables:
    def test_builds_create_table(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "id", "column_id": 1,
             "type_name": "int", "max_length": 4, "precision": 10, "scale": 0,
             "is_nullable": False, "is_identity": True, "seed_value": 1, "increment_value": 1},
            {"schema_name": "dbo", "table_name": "T1", "column_name": "name", "column_id": 2,
             "type_name": "nvarchar", "max_length": 100, "precision": 0, "scale": 0,
             "is_nullable": True, "is_identity": False, "seed_value": None, "increment_value": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        project_root = tmp_path / "out"
        project_root.mkdir(parents=True, exist_ok=True)
        (project_root / "manifest.json").write_text('{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8")

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--project-root", str(project_root),
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 1

        sql = (project_root / "ddl" / "tables.sql").read_text()
        assert "CREATE TABLE [dbo].[T1]" in sql
        assert "IDENTITY(1,1)" in sql
        assert "NVARCHAR(50)" in sql  # 100 / 2 for N-types
        assert "NOT NULL" in sql

    def test_nvarchar_max(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "data", "column_id": 1,
             "type_name": "nvarchar", "max_length": -1, "precision": 0, "scale": 0,
             "is_nullable": True, "is_identity": False, "seed_value": None, "increment_value": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        project_root = tmp_path / "out"
        project_root.mkdir(parents=True, exist_ok=True)
        (project_root / "manifest.json").write_text('{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8")

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--project-root", str(project_root),
        ])
        assert result.returncode == 0
        sql = (project_root / "ddl" / "tables.sql").read_text()
        assert "NVARCHAR(MAX)" in sql

    def test_decimal_type(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "amount", "column_id": 1,
             "type_name": "decimal", "max_length": 9, "precision": 18, "scale": 2,
             "is_nullable": False, "is_identity": False, "seed_value": None, "increment_value": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        project_root = tmp_path / "out"
        project_root.mkdir(parents=True, exist_ok=True)
        (project_root / "manifest.json").write_text('{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8")

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--project-root", str(project_root),
        ])
        assert result.returncode == 0
        sql = (project_root / "ddl" / "tables.sql").read_text()
        assert "DECIMAL(18,2)" in sql

    def test_decimal_without_precision_emits_bare_type(self) -> None:
        assert format_sql_type("decimal", max_length=9, precision=0, scale=0) == "DECIMAL"
        assert format_sql_type("numeric", max_length=9, precision=0, scale=0) == "NUMERIC"

    def test_multiple_tables_go_delimited(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "id", "column_id": 1,
             "type_name": "int", "max_length": 4, "precision": 10, "scale": 0,
             "is_nullable": False, "is_identity": False, "seed_value": None, "increment_value": None},
            {"schema_name": "dbo", "table_name": "T2", "column_name": "id", "column_id": 1,
             "type_name": "int", "max_length": 4, "precision": 10, "scale": 0,
             "is_nullable": False, "is_identity": False, "seed_value": None, "increment_value": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        project_root = tmp_path / "out"
        project_root.mkdir(parents=True, exist_ok=True)
        (project_root / "manifest.json").write_text('{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8")

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--project-root", str(project_root),
        ])
        assert result.returncode == 0
        sql = (project_root / "ddl" / "tables.sql").read_text()
        assert sql.count("CREATE TABLE") == 2
        assert "\nGO\n" in sql


# ── Corrupt JSON input tests ────────────────────────────────────────────


def test_assemble_modules_corrupt_input_exit_2(tmp_path: Path) -> None:
    """assemble-modules with corrupt JSON input exits 2."""
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not valid json", encoding="utf-8")
    project = tmp_path / "project"
    (project / "ddl").mkdir(parents=True)
    (project / "manifest.json").write_text('{"dialect":"tsql"}', encoding="utf-8")
    result = _run_cli([
        "assemble-modules",
        "--input", str(corrupt),
        "--project-root", str(project),
        "--type", "procedures",
    ])
    assert result.returncode == 2


def test_assemble_tables_corrupt_input_exit_2(tmp_path: Path) -> None:
    """assemble-tables with corrupt JSON input exits 2."""
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not valid json", encoding="utf-8")
    project = tmp_path / "project"
    (project / "ddl").mkdir(parents=True)
    (project / "manifest.json").write_text('{"dialect":"tsql"}', encoding="utf-8")
    result = _run_cli([
        "assemble-tables",
        "--input", str(corrupt),
        "--project-root", str(project),
    ])
    assert result.returncode == 2


# ── Unit: run_assemble_tables propagation ────────────────────────────────────


def test_run_assemble_tables_missing_manifest_raises(tmp_path: Path) -> None:
    """run_assemble_tables propagates ValueError when manifest.json is absent."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "lib"))
    from shared.setup_ddl import run_assemble_tables

    input_file = tmp_path / "input.json"
    input_file.write_text("[]", encoding="utf-8")
    project_root = tmp_path / "project"
    project_root.mkdir()

    with pytest.raises(ValueError):
        run_assemble_tables(input_file, project_root)
