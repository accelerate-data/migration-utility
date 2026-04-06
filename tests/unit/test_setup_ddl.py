"""Tests for setup_ddl.py CLI.

Unit tests verify each CLI subcommand produces correct output from JSON input.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

SHARED_DIR = (
    Path(__file__).parent.parent.parent
    / "plugin"
    / "lib"
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _run_cli(args: list[str], cwd: Path = SHARED_DIR, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "shared.setup_ddl", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
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

    def test_oracle_dialect(self, tmp_path):
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--database", "TestDB",
            "--schemas", "SH",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["dialect"] == "oracle"
        assert manifest["technology"] == "oracle"

    def test_invalid_technology_rejected(self, tmp_path):
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "postgres",
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


# ── Unit: write-partial-manifest ────────────────────────────────────────────


class TestWritePartialManifest:
    def test_writes_partial_manifest(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["schema_version"] == "1.0"
        assert manifest["technology"] == "oracle"
        assert manifest["dialect"] == "oracle"
        # Partial manifest should NOT have database or schema fields
        assert "source_database" not in manifest
        assert "extracted_schemas" not in manifest
        assert "extracted_at" not in manifest

    def test_partial_manifest_sql_server(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert manifest["technology"] == "sql_server"
        assert manifest["dialect"] == "tsql"

    def test_partial_manifest_invalid_technology(self, tmp_path):
        result = _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "postgres",
        ])
        assert result.returncode != 0

    def test_full_manifest_enriches_partial(self, tmp_path):
        # Write partial first
        _run_cli([
            "write-partial-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
        ])
        # Then enrich with full manifest
        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "oracle",
            "--database", "FREEPDB1",
            "--schemas", "SH,HR",
        ])
        assert result.returncode == 0
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        # All fields present
        assert manifest["technology"] == "oracle"
        assert manifest["dialect"] == "oracle"
        assert manifest["source_database"] == "FREEPDB1"
        assert manifest["extracted_schemas"] == ["SH", "HR"]
        assert "extracted_at" in manifest


# ── Unit: list-databases guards ──────────────────────────────────────────────


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "setup_ddl" / "oracle"


class TestListDatabasesGuards:
    def test_missing_manifest_fails(self, tmp_path):
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "manifest" in result.stderr.lower() or "manifest" in result.stdout.lower()

    def test_missing_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text('{"schema_version": "1.0"}', encoding="utf-8")
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0

    def test_oracle_unsupported(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli(["list-databases", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "oracle" in result.stderr.lower()


# ── Unit: list-schemas guards ────────────────────────────────────────────────


class TestListSchemasGuards:
    def test_missing_manifest_fails(self, tmp_path):
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "manifest" in result.stderr.lower() or "manifest" in result.stdout.lower()

    def test_missing_technology_fails(self, tmp_path):
        (tmp_path / "manifest.json").write_text('{"schema_version": "1.0"}', encoding="utf-8")
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode != 0

    def test_sql_server_requires_database_arg(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "sql_server", "dialect": "tsql"}', encoding="utf-8"
        )
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode != 0
        assert "database" in result.stderr.lower()


# ── Unit: Oracle schema processing (no DB required) ──────────────────────────


class TestOracleSchemaProcessing:
    def test_groups_by_owner_from_fixture(self):
        from shared.setup_ddl import _build_oracle_schema_summary
        rows = json.loads((FIXTURE_DIR / "list_schemas.json").read_text(encoding="utf-8"))
        summary = _build_oracle_schema_summary(rows)
        owners = {entry["owner"] for entry in summary}
        assert "SH" in owners
        sh_entry = next(e for e in summary if e["owner"] == "SH")
        assert sh_entry["object_count"] > 0

    def test_empty_input_returns_empty_list(self):
        from shared.setup_ddl import _build_oracle_schema_summary
        assert _build_oracle_schema_summary([]) == []

    def test_sorted_by_owner(self):
        from shared.setup_ddl import _build_oracle_schema_summary
        rows = [
            {"OWNER": "ZZ", "OBJECT_TYPE": "TABLE", "OBJECT_NAME": "T1"},
            {"OWNER": "AA", "OBJECT_TYPE": "TABLE", "OBJECT_NAME": "T2"},
            {"OWNER": "MM", "OBJECT_TYPE": "TABLE", "OBJECT_NAME": "T3"},
        ]
        summary = _build_oracle_schema_summary(rows)
        owners = [e["owner"] for e in summary]
        assert owners == sorted(owners)

    def test_lowercase_keys_handled(self):
        from shared.setup_ddl import _build_oracle_schema_summary
        rows = [
            {"owner": "SH", "object_type": "TABLE", "object_name": "SALES"},
            {"owner": "SH", "object_type": "TABLE", "object_name": "COSTS"},
        ]
        summary = _build_oracle_schema_summary(rows)
        assert len(summary) == 1
        assert summary[0]["owner"] == "SH"
        assert summary[0]["object_count"] == 2


# ── Integration: list-databases (SQL Server Docker) ──────────────────────────


@pytest.mark.integration
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
        # System databases should be excluded
        for sysdb in ("master", "tempdb", "model", "msdb"):
            assert sysdb not in out["databases"]


# ── Integration: list-schemas SQL Server (Docker) ───────────────────────────


@pytest.mark.integration
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
            assert "object_count" in entry
            assert isinstance(entry["object_count"], int)


# ── Integration: list-schemas Oracle (Docker) ────────────────────────────────


@pytest.mark.oracle
class TestListSchemasOracleIntegration:
    def test_oracle_sh_schema_present(self, tmp_path):
        for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
            if not os.environ.get(var):
                pytest.skip(f"{var} not set")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "schemas" in out
        owners = {entry["owner"] for entry in out["schemas"]}
        assert "SH" in owners
        sh_entry = next(e for e in out["schemas"] if e["owner"] == "SH")
        assert sh_entry["object_count"] > 0


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


# ── Unit: oracle_extract helpers (fixture-based, no live DB) ─────────────────

ORACLE_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "setup_ddl" / "oracle"


class TestExtractOracleUnit:
    def test_pk_uk_fixture_has_primary_keys(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_pk_uk.json").read_text())
        pk_rows = [r for r in rows if r["is_primary_key"] == 1]
        assert len(pk_rows) > 0, "SH schema should have primary key constraints"

    def test_fk_fixture_has_foreign_keys(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_fk.json").read_text())
        assert len(rows) > 0, "SH schema should have foreign key constraints"
        constraint_names = {r["constraint_name"] for r in rows}
        assert any("FK" in name or "fk" in name.lower() for name in constraint_names)

    def test_table_columns_fixture_has_sh_tables(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_tab_columns.json").read_text())
        table_names = {r["table_name"] for r in rows}
        assert "CUSTOMERS" in table_names
        assert "SALES" in table_names

    def test_table_columns_have_required_fields(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "all_tab_columns.json").read_text())
        required = {"schema_name", "table_name", "column_name", "column_id",
                    "type_name", "max_length", "precision", "scale",
                    "is_nullable", "is_identity"}
        for row in rows[:5]:
            assert required.issubset(set(row.keys())), f"Missing fields in row: {row.keys()}"

    def test_object_types_fixture_maps_to_sql_server_codes(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "object_types.json").read_text())
        valid_types = {"U", "V", "P", "FN"}
        for row in rows:
            assert row["type"] in valid_types, f"Unexpected type code: {row['type']}"

    def test_dependencies_have_dmf_shape(self):
        rows = json.loads((ORACLE_FIXTURE_DIR / "dependencies_view.json").read_text())
        if not rows:
            return  # SH may have no view deps; skip rather than fail
        required = {
            "referencing_schema", "referencing_name", "referenced_schema",
            "referenced_entity", "referenced_minor_name", "referenced_class_desc",
            "is_selected", "is_updated", "is_insert_all",
        }
        for row in rows:
            assert required.issubset(set(row.keys()))
            assert row["is_selected"] is False
            assert row["is_updated"] is False

    def test_pk_rows_feed_apply_pk_unique(self, tmp_path):
        """Verify PK rows from Oracle fixture are consumed correctly by _apply_pk_unique_rows."""
        import sys
        sys.path.insert(0, str(Path(__file__).parents[2] / "plugin" / "lib"))
        from shared.setup_ddl import _apply_pk_unique_rows

        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_pk_uk.json").read_text())
        signals: dict = {}
        _apply_pk_unique_rows(signals, rows)
        assert len(signals) > 0
        for fqn, sig in signals.items():
            for pk in sig.get("primary_keys", []):
                assert "constraint_name" in pk
                assert "columns" in pk
                assert len(pk["columns"]) > 0

    def test_fk_rows_feed_apply_fk_rows(self, tmp_path):
        """Verify FK rows from Oracle fixture are consumed correctly by _apply_fk_rows."""
        import sys
        sys.path.insert(0, str(Path(__file__).parents[2] / "plugin" / "lib"))
        from shared.setup_ddl import _apply_fk_rows

        rows = json.loads((ORACLE_FIXTURE_DIR / "all_constraints_fk.json").read_text())
        signals: dict = {}
        _apply_fk_rows(signals, rows)
        assert len(signals) > 0
        for fqn, sig in signals.items():
            for fk in sig.get("foreign_keys", []):
                assert "constraint_name" in fk
                assert "referenced_table" in fk


# ── Integration: extract SQL Server (Docker) ─────────────────────────────────


@pytest.mark.integration
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
        assert "dbo" in manifest["extracted_schemas"]
        assert manifest["source_database"] == "MigrationTest"

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
        table_files = list(tables_dir.glob("*.json"))
        assert len(table_files) > 0

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
        """Re-running extract must not wipe scoping/profile/refactor fields."""
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

        # Inject a fake scoping field into the first table catalog
        tables_dir = tmp_path / "catalog" / "tables"
        table_files = list(tables_dir.glob("*.json"))
        if not table_files:
            pytest.skip("No table catalog files")
        first_file = table_files[0]
        data = json.loads(first_file.read_text())
        data["scoping"] = {"selected_writer": "dbo.fake_proc", "_test": True}
        first_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

        # Re-run extract
        result2 = _run_cli([
            "extract",
            "--database", "MigrationTest",
            "--schemas", "dbo",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result2.returncode == 0, result2.stderr

        # Scoping must still be present
        data2 = json.loads(first_file.read_text())
        assert "scoping" in data2, "scoping field was wiped by re-extraction"
        assert data2["scoping"].get("_test") is True


# ── Integration: extract Oracle (Docker) ─────────────────────────────────────


@pytest.mark.oracle
class TestExtractOracleIntegration:
    def _skip_if_missing(self):
        for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
            if not os.environ.get(var):
                pytest.skip(f"{var} not set")

    def test_sh_produces_ddl_and_catalog(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "ddl").is_dir()
        assert (tmp_path / "catalog").is_dir()
        tables_dir = tmp_path / "catalog" / "tables"
        assert tables_dir.is_dir()
        assert len(list(tables_dir.glob("*.json"))) > 0

    def test_sh_table_has_pk(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        tables_with_pk = []
        for f in tables_dir.glob("*.json"):
            data = json.loads(f.read_text())
            if data.get("primary_keys"):
                tables_with_pk.append(f.name)
        assert len(tables_with_pk) > 0, "No SH tables have primary keys in catalog"

    def test_sh_table_has_fk(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        tables_with_fk = []
        for f in tables_dir.glob("*.json"):
            data = json.loads(f.read_text())
            if data.get("foreign_keys"):
                tables_with_fk.append(f.name)
        assert len(tables_with_fk) > 0, "No SH tables have foreign keys in catalog"

    def test_sh_change_capture_null(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        for f in tables_dir.glob("*.json"):
            data = json.loads(f.read_text())
            assert data.get("change_capture") is None, (
                f"{f.name}: change_capture should be null for Oracle, got {data['change_capture']}"
            )
