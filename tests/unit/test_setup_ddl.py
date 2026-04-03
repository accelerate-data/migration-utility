"""Tests for setup_ddl.py CLI.

Unit tests verify each CLI subcommand produces correct output from JSON input.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SHARED_DIR = (
    Path(__file__).parent.parent.parent
    / "lib"
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _run_cli(args: list[str], cwd: Path = SHARED_DIR) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "shared.setup_ddl", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=30,
    )


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


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

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--project-root", str(project_root),
        ])
        assert result.returncode == 0
        sql = (project_root / "ddl" / "tables.sql").read_text()
        assert "DECIMAL(18,2)" in sql

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

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--project-root", str(project_root),
        ])
        assert result.returncode == 0
        sql = (project_root / "ddl" / "tables.sql").read_text()
        assert sql.count("CREATE TABLE") == 2
        assert "\nGO\n" in sql


# ── Unit: write-manifest ─────────────────────────────────────────────────────


class TestWriteManifest:
    def test_writes_manifest(self, tmp_path):
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
            "--database", "TestDB",
            "--schemas", "bronze,silver",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["schema_version"] == "1.0"
        assert manifest["technology"] == "sql_server"
        assert manifest["dialect"] == "tsql"
        assert manifest["source_database"] == "TestDB"
        assert manifest["extracted_schemas"] == ["bronze", "silver"]
        assert "extracted_at" in manifest

    def test_fabric_warehouse_dialect(self, tmp_path):
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "fabric_warehouse",
            "--database", "TestDB",
            "--schemas", "dbo",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["dialect"] == "tsql"

    def test_invalid_technology_rejected(self, tmp_path):
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--database", "TestDB",
            "--schemas", "dbo",
        ])
        assert result.returncode != 0


# ── Unit: write-catalog ──────────────────────────────────────────────────────


class TestWriteCatalog:
    def test_writes_catalog_from_staging(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"

        # Minimal staging data
        _write_json(staging / "table_columns.json", [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "id", "column_id": 1,
             "type_name": "int", "max_length": 4, "precision": 10, "scale": 0,
             "is_nullable": False, "is_identity": False},
        ])
        _write_json(staging / "pk_unique.json", [
            {"schema_name": "dbo", "table_name": "T1", "index_name": "PK_T1",
             "is_unique": True, "is_primary_key": True, "column_name": "id", "key_ordinal": 1},
        ])
        _write_json(staging / "foreign_keys.json", [])
        _write_json(staging / "identity_columns.json", [])
        _write_json(staging / "cdc.json", [])
        _write_json(staging / "object_types.json", [
            {"schema_name": "dbo", "name": "T1", "type": "U"},
            {"schema_name": "dbo", "name": "usp_a", "type": "P"},
        ])
        _write_json(staging / "definitions.json", [
            {"schema_name": "dbo", "object_name": "usp_a",
             "definition": "CREATE PROC dbo.usp_a AS INSERT INTO dbo.T1 (id) VALUES (1)"},
        ])
        _write_json(staging / "proc_dmf.json", [
            {"referencing_schema": "dbo", "referencing_name": "usp_a",
             "referenced_schema": "dbo", "referenced_entity": "T1",
             "referenced_minor_name": "id", "referenced_class_desc": "OBJECT_OR_COLUMN",
             "is_selected": False, "is_updated": True, "is_select_all": False,
             "is_insert_all": False, "is_all_columns_found": True,
             "is_caller_dependent": False, "is_ambiguous": False},
        ])
        _write_json(staging / "view_dmf.json", [])
        _write_json(staging / "func_dmf.json", [])
        _write_json(staging / "proc_params.json", [])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["tables"] >= 1
        assert counts["procedures"] >= 1

        # Verify table catalog
        table_cat = json.loads((output / "catalog" / "tables" / "dbo.t1.json").read_text())
        assert table_cat["schema"] == "dbo"
        assert table_cat["name"] == "t1"
        assert len(table_cat["primary_keys"]) == 1
        assert table_cat["primary_keys"][0]["columns"] == ["id"]
        # Verify referenced_by is populated from flipped DMF
        procs = table_cat["referenced_by"]["procedures"]["in_scope"]
        assert any(p["name"] == "usp_a" for p in procs)

        # Verify proc catalog
        proc_cat = json.loads((output / "catalog" / "procedures" / "dbo.usp_a.json").read_text())
        assert proc_cat["schema"] == "dbo"
        assert proc_cat["name"] == "usp_a"
        tables = proc_cat["references"]["tables"]["in_scope"]
        assert any(t["name"] == "T1" and t["is_updated"] for t in tables)

    def test_identity_columns_in_signals(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"

        _write_json(staging / "table_columns.json", [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "id", "column_id": 1,
             "type_name": "int", "max_length": 4, "precision": 10, "scale": 0,
             "is_nullable": False, "is_identity": True},
        ])
        _write_json(staging / "pk_unique.json", [])
        _write_json(staging / "foreign_keys.json", [])
        _write_json(staging / "identity_columns.json", [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "id",
             "seed_value": 100, "increment_value": 5},
        ])
        _write_json(staging / "cdc.json", [])
        _write_json(staging / "object_types.json", [
            {"schema_name": "dbo", "name": "T1", "type": "U"},
        ])
        _write_json(staging / "definitions.json", [])
        _write_json(staging / "proc_dmf.json", [])
        _write_json(staging / "view_dmf.json", [])
        _write_json(staging / "func_dmf.json", [])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr

        table_cat = json.loads((output / "catalog" / "tables" / "dbo.t1.json").read_text())
        assert len(table_cat["auto_increment_columns"]) == 1
        ident = table_cat["auto_increment_columns"][0]
        assert ident["column"] == "id"
        assert ident["seed"] == 100
        assert ident["increment"] == 5

    def test_routing_flags_from_definitions(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"

        _write_json(staging / "table_columns.json", [])
        _write_json(staging / "pk_unique.json", [])
        _write_json(staging / "foreign_keys.json", [])
        _write_json(staging / "identity_columns.json", [])
        _write_json(staging / "cdc.json", [])
        _write_json(staging / "object_types.json", [
            {"schema_name": "dbo", "name": "usp_dynamic", "type": "P"},
        ])
        _write_json(staging / "definitions.json", [
            {"schema_name": "dbo", "object_name": "usp_dynamic",
             "definition": "CREATE PROC dbo.usp_dynamic AS DECLARE @sql NVARCHAR(MAX); EXEC(@sql)"},
        ])
        _write_json(staging / "proc_dmf.json", [])
        _write_json(staging / "view_dmf.json", [])
        _write_json(staging / "func_dmf.json", [])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr

        proc_cat = json.loads((output / "catalog" / "procedures" / "dbo.usp_dynamic.json").read_text())
        assert proc_cat.get("needs_llm") is True
        assert proc_cat.get("mode") == "llm_required"
        assert proc_cat.get("routing_reasons") == ["dynamic_sql_variable"]


# ── Unit: schema/name fields in catalog output ───────────────────────────────


class TestCatalogSchemaNameFields:
    """Verify that write_table_catalog and write_object_catalog include schema and name."""

    def test_table_catalog_has_schema_name(self, tmp_path):
        from shared.catalog import write_table_catalog
        write_table_catalog(tmp_path, "silver.DimCustomer", {"columns": []})
        data = json.loads((tmp_path / "catalog" / "tables" / "silver.dimcustomer.json").read_text())
        assert data["schema"] == "silver"
        assert data["name"] == "dimcustomer"

    def test_proc_catalog_has_schema_name(self, tmp_path):
        from shared.catalog import write_object_catalog
        from shared.dmf_processing import empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_object_catalog(tmp_path, "procedures", "silver.usp_load_Dim", refs)
        data = json.loads((tmp_path / "catalog" / "procedures" / "silver.usp_load_dim.json").read_text())
        assert data["schema"] == "silver"
        assert data["name"] == "usp_load_dim"

    def test_view_catalog_has_schema_name(self, tmp_path):
        from shared.catalog import write_object_catalog
        from shared.dmf_processing import empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_object_catalog(tmp_path, "views", "dbo.vw_Sales", refs)
        data = json.loads((tmp_path / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert data["schema"] == "dbo"
        assert data["name"] == "vw_sales"


# ── Unit: diff-aware reexport ────────────────────────────────────────────────


def _make_staging(staging: Path, *, definition: str = "CREATE PROC dbo.usp_a AS INSERT INTO dbo.T1 (id) VALUES (1)") -> None:
    """Write a minimal set of staging files for write-catalog tests."""
    _write_json(staging / "table_columns.json", [
        {"schema_name": "dbo", "table_name": "T1", "column_name": "id", "column_id": 1,
         "type_name": "int", "max_length": 4, "precision": 10, "scale": 0,
         "is_nullable": False, "is_identity": False},
    ])
    _write_json(staging / "pk_unique.json", [])
    _write_json(staging / "foreign_keys.json", [])
    _write_json(staging / "identity_columns.json", [])
    _write_json(staging / "cdc.json", [])
    _write_json(staging / "object_types.json", [
        {"schema_name": "dbo", "name": "T1", "type": "U"},
        {"schema_name": "dbo", "name": "usp_a", "type": "P"},
    ])
    _write_json(staging / "definitions.json", [
        {"schema_name": "dbo", "object_name": "usp_a", "definition": definition},
    ])
    _write_json(staging / "proc_dmf.json", [
        {"referencing_schema": "dbo", "referencing_name": "usp_a",
         "referenced_schema": "dbo", "referenced_entity": "T1",
         "referenced_minor_name": "id", "referenced_class_desc": "OBJECT_OR_COLUMN",
         "is_selected": False, "is_updated": True, "is_select_all": False,
         "is_insert_all": False, "is_all_columns_found": True,
         "is_caller_dependent": False, "is_ambiguous": False},
    ])
    _write_json(staging / "view_dmf.json", [])
    _write_json(staging / "func_dmf.json", [])
    _write_json(staging / "proc_params.json", [])


class TestWriteCatalogDiffAware:
    """Verify diff-aware reexport preserves unchanged catalogs and rewrites changed ones."""

    def test_first_run_writes_all_with_hash(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _make_staging(staging)

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["new"] >= 2  # table + proc
        assert counts["unchanged"] == 0
        assert counts["changed"] == 0
        assert counts["removed"] == 0

        # Verify ddl_hash is written
        table_cat = json.loads((output / "catalog" / "tables" / "dbo.t1.json").read_text())
        assert "ddl_hash" in table_cat
        assert len(table_cat["ddl_hash"]) == 64

        proc_cat = json.loads((output / "catalog" / "procedures" / "dbo.usp_a.json").read_text())
        assert "ddl_hash" in proc_cat
        assert len(proc_cat["ddl_hash"]) == 64

    def test_rerun_same_data_preserves_catalogs(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _make_staging(staging)

        # First run
        _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        # Inject LLM-enriched section into table catalog
        table_path = output / "catalog" / "tables" / "dbo.t1.json"
        table_cat = json.loads(table_path.read_text())
        table_cat["scoping"] = {"status": "resolved", "selected_writer": "dbo.usp_a"}
        table_path.write_text(json.dumps(table_cat, indent=2) + "\n")

        # Inject statements into proc catalog
        proc_path = output / "catalog" / "procedures" / "dbo.usp_a.json"
        proc_cat = json.loads(proc_path.read_text())
        proc_cat["statements"] = [{"type": "Insert", "action": "migrate"}]
        proc_path.write_text(json.dumps(proc_cat, indent=2) + "\n")

        # Second run with identical staging data
        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["unchanged"] >= 2
        assert counts["changed"] == 0
        assert counts["new"] == 0
        assert counts["removed"] == 0

        # Verify LLM sections survived
        table_cat = json.loads(table_path.read_text())
        assert table_cat["scoping"]["status"] == "resolved"

        proc_cat = json.loads(proc_path.read_text())
        assert proc_cat["statements"] == [{"type": "Insert", "action": "migrate"}]

    def test_changed_definition_rewrites_catalog(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _make_staging(staging)

        # First run
        _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        # Save original hash
        proc_path = output / "catalog" / "procedures" / "dbo.usp_a.json"
        original_hash = json.loads(proc_path.read_text())["ddl_hash"]

        # Second run with changed definition
        _make_staging(staging, definition="CREATE PROC dbo.usp_a AS INSERT INTO dbo.T1 (id) VALUES (999)")

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["changed"] >= 1

        new_hash = json.loads(proc_path.read_text())["ddl_hash"]
        assert new_hash != original_hash

    def test_removed_object_flagged_stale(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _make_staging(staging)

        # First run
        _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        # Second run without the proc (remove from definitions and object_types)
        _write_json(staging / "definitions.json", [])
        _write_json(staging / "object_types.json", [
            {"schema_name": "dbo", "name": "T1", "type": "U"},
        ])
        _write_json(staging / "proc_dmf.json", [])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["removed"] >= 1

        # Verify stale flag
        proc_cat = json.loads((output / "catalog" / "procedures" / "dbo.usp_a.json").read_text())
        assert proc_cat.get("stale") is True

    def test_new_object_gets_fresh_catalog(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _make_staging(staging)

        # First run
        _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        # Add a new proc
        defs = json.loads((staging / "definitions.json").read_text())
        defs.append({"schema_name": "dbo", "object_name": "usp_b",
                      "definition": "CREATE PROC dbo.usp_b AS SELECT 1"})
        _write_json(staging / "definitions.json", defs)

        otypes = json.loads((staging / "object_types.json").read_text())
        otypes.append({"schema_name": "dbo", "name": "usp_b", "type": "P"})
        _write_json(staging / "object_types.json", otypes)

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["new"] >= 1

        new_proc = json.loads((output / "catalog" / "procedures" / "dbo.usp_b.json").read_text())
        assert "ddl_hash" in new_proc
        assert new_proc["schema"] == "dbo"
        assert new_proc["name"] == "usp_b"
