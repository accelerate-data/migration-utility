"""SQL Server integration coverage for setup_ddl CLI."""

from __future__ import annotations

import json

import pytest

from tests.helpers import run_setup_ddl_cli as _run_cli
from tests.helpers import (
    SQL_SERVER_FIXTURE_BRONZE_CURRENCY,
    SQL_SERVER_FIXTURE_DATABASE,
    SQL_SERVER_FIXTURE_SCHEMA,
    SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY,
    SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC,
)
from tests.integration.runtime_helpers import ensure_sql_server_migration_test_materialized
from tests.integration.runtime_helpers import assert_manifest_has_runtime_roles
from tests.integration.runtime_helpers import write_source_target_sandbox_manifest

pytestmark = pytest.mark.integration


def _catalog_name(object_name: str) -> str:
    return f"{SQL_SERVER_FIXTURE_SCHEMA.lower()}.{object_name.lower()}.json"


def _ensure_sql_server_fixture_materialized() -> None:
    ensure_sql_server_migration_test_materialized()


class TestListDatabasesIntegration:
    def test_sql_server_returns_list(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "databases" in out
        assert isinstance(out["databases"], list)
        for sysdb in ("master", "tempdb", "model", "msdb"):
            assert sysdb not in out["databases"]


class TestListSchemasSqlServerIntegration:
    def test_returns_schemas_with_counts(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli([
            "list-schemas",
            "--project-root", str(tmp_path),
            "--database", SQL_SERVER_FIXTURE_DATABASE,
        ])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "schemas" in out
        assert isinstance(out["schemas"], list)
        assert len(out["schemas"]) > 0
        schema_entry = next(
            entry for entry in out["schemas"] if entry["schema"] == SQL_SERVER_FIXTURE_SCHEMA
        )
        for field in ("tables", "procedures", "views", "functions"):
            assert field in schema_entry
            assert isinstance(schema_entry[field], int)
        assert schema_entry["tables"] > 0


class TestExtractSqlServerIntegration:
    def test_produces_ddl_and_catalog(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli([
            "extract",
            "--database", SQL_SERVER_FIXTURE_DATABASE,
            "--schemas", SQL_SERVER_FIXTURE_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        assert_manifest_has_runtime_roles(tmp_path)
        assert (tmp_path / "ddl").is_dir()
        assert (tmp_path / "catalog").is_dir()
        assert (tmp_path / "manifest.json").exists()
        assert (tmp_path / "catalog" / "tables" / _catalog_name(SQL_SERVER_FIXTURE_BRONZE_CURRENCY)).exists()
        assert (tmp_path / "catalog" / "tables" / _catalog_name(SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY)).exists()
        assert (tmp_path / "catalog" / "procedures" / _catalog_name(SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC)).exists()

        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["technology"] == "sql_server"
        assert manifest["extraction"]["schemas"] == [SQL_SERVER_FIXTURE_SCHEMA]
        assert manifest["runtime"]["source"]["connection"]["database"] == SQL_SERVER_FIXTURE_DATABASE

    def test_catalog_tables_non_empty(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli([
            "extract",
            "--database", SQL_SERVER_FIXTURE_DATABASE,
            "--schemas", SQL_SERVER_FIXTURE_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        assert tables_dir.is_dir()
        table_names = {path.name for path in tables_dir.glob("*.json")}
        assert _catalog_name(SQL_SERVER_FIXTURE_BRONZE_CURRENCY) in table_names
        assert _catalog_name(SQL_SERVER_FIXTURE_SILVER_DIMCURRENCY) in table_names

    def test_procedure_catalog_has_routing_flags(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli([
            "extract",
            "--database", SQL_SERVER_FIXTURE_DATABASE,
            "--schemas", SQL_SERVER_FIXTURE_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        procs_dir = tmp_path / "catalog" / "procedures"
        if not procs_dir.is_dir():
            pytest.skip(f"No procedures in {SQL_SERVER_FIXTURE_DATABASE}.{SQL_SERVER_FIXTURE_SCHEMA}")
        proc_path = procs_dir / _catalog_name(SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC)
        if not proc_path.exists():
            pytest.skip(f"No procedure catalog file produced for {SQL_SERVER_FIXTURE_SILVER_PATTERN_PROC}")
        data = json.loads(proc_path.read_text())
        assert "mode" in data, f"Procedure catalog missing 'mode' field: {data.keys()}"

    def test_view_catalog_has_columns(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli([
            "extract",
            "--database", SQL_SERVER_FIXTURE_DATABASE,
            "--schemas", SQL_SERVER_FIXTURE_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr

        views_dir = tmp_path / "catalog" / "views"
        if not views_dir.is_dir():
            pytest.skip(f"No views in {SQL_SERVER_FIXTURE_DATABASE}.{SQL_SERVER_FIXTURE_SCHEMA}")
        view_files = list(views_dir.glob("*.json"))
        if not view_files:
            pytest.skip(f"No view catalog files produced for {SQL_SERVER_FIXTURE_SCHEMA}")
        data = json.loads(view_files[0].read_text())
        assert data.get("columns")
        for column in data["columns"]:
            assert "name" in column
            assert "sql_type" in column
            assert "is_nullable" in column

    def test_enriched_fields_preserved_on_reextract(self, tmp_path):
        _ensure_sql_server_fixture_materialized()
        write_source_target_sandbox_manifest(tmp_path, source_technology="sql_server")
        result = _run_cli([
            "extract",
            "--database", SQL_SERVER_FIXTURE_DATABASE,
            "--schemas", SQL_SERVER_FIXTURE_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr

        first_file = (
            tmp_path / "catalog" / "tables" / _catalog_name(SQL_SERVER_FIXTURE_BRONZE_CURRENCY)
        )
        if not first_file.exists():
            pytest.skip(f"No table catalog file for {SQL_SERVER_FIXTURE_BRONZE_CURRENCY}")
        data = json.loads(first_file.read_text())
        data["scoping"] = {"selected_writer": "dbo.fake_proc", "_test": True}
        first_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

        result2 = _run_cli([
            "extract",
            "--database", SQL_SERVER_FIXTURE_DATABASE,
            "--schemas", SQL_SERVER_FIXTURE_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result2.returncode == 0, result2.stderr

        data2 = json.loads(first_file.read_text())
        assert "scoping" in data2
        assert data2["scoping"].get("_test") is True
