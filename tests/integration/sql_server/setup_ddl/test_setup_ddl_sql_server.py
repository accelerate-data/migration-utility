"""SQL Server integration coverage for setup_ddl CLI."""

from __future__ import annotations

import json
import os

import pytest

from tests.helpers import run_setup_ddl_cli as _run_cli

pytestmark = pytest.mark.integration


class TestListDatabasesIntegration:
    def test_sql_server_returns_list(self, tmp_path):
        if not os.environ.get("MSSQL_HOST"):
            pytest.skip("MSSQL_HOST not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "databases" in out
        assert isinstance(out["databases"], list)
        for sysdb in ("master", "tempdb", "model", "msdb"):
            assert sysdb not in out["databases"]


class TestListSchemasSqlServerIntegration:
    def test_returns_schemas_with_counts(self, tmp_path):
        if not os.environ.get("MSSQL_HOST"):
            pytest.skip("MSSQL_HOST not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "list-schemas",
            "--project-root", str(tmp_path),
            "--database", "MigrationTest",
        ])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "schemas" in out
        assert isinstance(out["schemas"], list)
        assert len(out["schemas"]) > 0
        for entry in out["schemas"]:
            assert "schema" in entry
            for field in ("tables", "procedures", "views", "functions"):
                assert field in entry
                assert isinstance(entry[field], int)


class TestExtractSqlServerIntegration:
    def test_produces_ddl_and_catalog(self, tmp_path):
        if not os.environ.get("MSSQL_HOST"):
            pytest.skip("MSSQL_HOST not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "ddl").is_dir()
        assert (tmp_path / "catalog").is_dir()
        assert (tmp_path / "manifest.json").exists()

        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["technology"] == "sql_server"
        assert "dbo" in manifest["extraction"]["schemas"]
        assert manifest["runtime"]["source"]["connection"]["database"] == "MigrationTest"

    def test_catalog_tables_non_empty(self, tmp_path):
        if not os.environ.get("MSSQL_HOST"):
            pytest.skip("MSSQL_HOST not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        assert tables_dir.is_dir()
        assert len(list(tables_dir.glob("*.json"))) > 0

    def test_procedure_catalog_has_routing_flags(self, tmp_path):
        if not os.environ.get("MSSQL_HOST"):
            pytest.skip("MSSQL_HOST not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        procs_dir = tmp_path / "catalog" / "procedures"
        if not procs_dir.is_dir():
            pytest.skip("No procedures in MigrationTest.dbo")
        proc_files = list(procs_dir.glob("*.json"))
        if not proc_files:
            pytest.skip("No procedure catalog files produced")
        data = json.loads(proc_files[0].read_text())
        assert "mode" in data, f"Procedure catalog missing 'mode' field: {data.keys()}"

    def test_enriched_fields_preserved_on_reextract(self, tmp_path):
        if not os.environ.get("MSSQL_HOST"):
            pytest.skip("MSSQL_HOST not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr

        tables_dir = tmp_path / "catalog" / "tables"
        table_files = list(tables_dir.glob("*.json"))
        if not table_files:
            pytest.skip("No table catalog files")
        first_file = table_files[0]
        data = json.loads(first_file.read_text())
        data["scoping"] = {"selected_writer": "dbo.fake_proc", "_test": True}
        first_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

        result2 = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result2.returncode == 0, result2.stderr

        data2 = json.loads(first_file.read_text())
        assert "scoping" in data2
        assert data2["scoping"].get("_test") is True
