"""Tests for setup_ddl.py CLI — both unit tests (static fixtures) and
integration tests (Docker SQL Server).

Unit tests verify each CLI subcommand produces correct output from JSON input.
Integration tests verify the full MCP-replacement pipeline against the same
MigrationTest database used by test_integration_catalog.py.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SHARED_DIR = (
    Path(__file__).parent.parent.parent.parent
    / "agent-sources" / "ad-migration" / "workbench" / "migration" / "shared"
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
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
            "--type", "procedures",
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 2

        sql = (output_folder / "ddl" / "procedures.sql").read_text()
        assert "CREATE PROC dbo.usp_a" in sql
        assert "\nGO\n" in sql

    def test_skips_null_definitions(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "object_name": "usp_a", "definition": "CREATE PROC dbo.usp_a AS SELECT 1"},
            {"schema_name": "dbo", "object_name": "usp_b", "definition": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
            "--type", "procedures",
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 1

    def test_empty_input_writes_empty_file(self, tmp_path):
        input_file = tmp_path / "input.json"
        _write_json(input_file, [])
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
            "--type", "views",
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 0
        assert (output_folder / "ddl" / "views.sql").read_text() == ""

    def test_invalid_type_rejected(self, tmp_path):
        input_file = tmp_path / "input.json"
        _write_json(input_file, [])

        result = _run_cli([
            "assemble-modules",
            "--input", str(input_file),
            "--output-folder", str(tmp_path / "out"),
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
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
        ])
        assert result.returncode == 0
        out = json.loads(result.stdout)
        assert out["count"] == 1

        sql = (output_folder / "ddl" / "tables.sql").read_text()
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
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
        ])
        assert result.returncode == 0
        sql = (output_folder / "ddl" / "tables.sql").read_text()
        assert "NVARCHAR(MAX)" in sql

    def test_decimal_type(self, tmp_path):
        rows = [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "amount", "column_id": 1,
             "type_name": "decimal", "max_length": 9, "precision": 18, "scale": 2,
             "is_nullable": False, "is_identity": False, "seed_value": None, "increment_value": None},
        ]
        input_file = tmp_path / "input.json"
        _write_json(input_file, rows)
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
        ])
        assert result.returncode == 0
        sql = (output_folder / "ddl" / "tables.sql").read_text()
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
        output_folder = tmp_path / "out"

        result = _run_cli([
            "assemble-tables",
            "--input", str(input_file),
            "--output-folder", str(output_folder),
        ])
        assert result.returncode == 0
        sql = (output_folder / "ddl" / "tables.sql").read_text()
        assert sql.count("CREATE TABLE") == 2
        assert "\nGO\n" in sql


# ── Unit: write-manifest ─────────────────────────────────────────────────────


class TestWriteManifest:
    def test_writes_manifest(self, tmp_path):
        result = _run_cli([
            "write-manifest",
            "--output-folder", str(tmp_path),
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
            "--output-folder", str(tmp_path),
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
            "--output-folder", str(tmp_path),
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
            "--output-folder", str(output),
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
            "--output-folder", str(output),
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
            "--output-folder", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr

        proc_cat = json.loads((output / "catalog" / "procedures" / "dbo.usp_dynamic.json").read_text())
        assert proc_cat.get("needs_llm") is True


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
        from shared.catalog import write_object_catalog, empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_object_catalog(tmp_path, "procedures", "silver.usp_load_Dim", refs)
        data = json.loads((tmp_path / "catalog" / "procedures" / "silver.usp_load_dim.json").read_text())
        assert data["schema"] == "silver"
        assert data["name"] == "usp_load_dim"

    def test_view_catalog_has_schema_name(self, tmp_path):
        from shared.catalog import write_object_catalog, empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_object_catalog(tmp_path, "views", "dbo.vw_Sales", refs)
        data = json.loads((tmp_path / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert data["schema"] == "dbo"
        assert data["name"] == "vw_sales"


# ── Integration: full pipeline against Docker SQL Server ─────────────────────

pytestmark_integration = pytest.mark.integration


@pytest.fixture(scope="module")
def integration_output(tmp_path_factory):
    """Run export_ddl --catalog via pyodbc to produce reference output."""
    output_dir = tmp_path_factory.mktemp("integration_ref")
    result = subprocess.run(
        [
            sys.executable, "-m", "shared.export_ddl",
            "--host", "127.0.0.1",
            "--port", "1433",
            "--database", "MigrationTest",
            "--user", "sa",
            "--password", "P@ssw0rd123",
            "--output", str(output_dir),
            "--catalog",
        ],
        cwd=str(SHARED_DIR),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(f"export_ddl failed: {result.stderr}")
    return output_dir


@pytest.fixture(scope="module")
def staging_from_export(integration_output, tmp_path_factory):
    """Build staging directory from export_ddl output to simulate MCP-saved JSON.

    This re-runs the pyodbc queries and saves results as JSON, mimicking what
    the agent would do with MCP query results.
    """
    staging = tmp_path_factory.mktemp("staging")

    try:
        import pyodbc
    except ImportError:
        pytest.skip("pyodbc not installed")

    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if not drivers:
        pytest.skip("No SQL Server ODBC driver")

    conn = pyodbc.connect(
        f"DRIVER={{{drivers[-1]}}};SERVER=127.0.0.1,1433;DATABASE=MigrationTest;"
        f"UID=sa;PWD=P@ssw0rd123;TrustServerCertificate=yes;Encrypt=no;"
    )

    def _query(sql: str) -> list[dict]:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _query_safe(sql: str) -> list[dict]:
        try:
            return _query(sql)
        except Exception:
            return []

    # Table columns
    _write_json(staging / "table_columns.json", _query("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               c.name AS column_name, c.column_id, tp.name AS type_name,
               c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity
        FROM sys.tables t
        JOIN sys.columns c ON c.object_id = t.object_id
        JOIN sys.types tp ON tp.user_type_id = c.user_type_id
        WHERE t.is_ms_shipped = 0 ORDER BY schema_name, table_name, c.column_id
    """))

    # PK + unique indexes
    _write_json(staging / "pk_unique.json", _query("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               i.name AS index_name, i.is_unique, i.is_primary_key,
               c.name AS column_name, ic.key_ordinal
        FROM sys.tables t
        JOIN sys.indexes i ON i.object_id = t.object_id
            AND (i.is_primary_key = 1 OR (i.is_unique = 1 AND i.is_primary_key = 0))
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE t.is_ms_shipped = 0
    """))

    # Foreign keys
    _write_json(staging / "foreign_keys.json", _query("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               fk.name AS constraint_name,
               COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
               SCHEMA_NAME(rt.schema_id) AS ref_schema, rt.name AS ref_table,
               COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS ref_column
        FROM sys.foreign_keys fk
        JOIN sys.tables t ON t.object_id = fk.parent_object_id
        JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
        JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
        WHERE t.is_ms_shipped = 0
    """))

    # Identity columns
    _write_json(staging / "identity_columns.json", _query("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               c.name AS column_name,
               CAST(c.seed_value AS BIGINT) AS seed_value,
               CAST(c.increment_value AS BIGINT) AS increment_value
        FROM sys.identity_columns c
        JOIN sys.tables t ON t.object_id = c.object_id
        WHERE t.is_ms_shipped = 0
    """))

    # CDC
    _write_json(staging / "cdc.json", _query("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
        FROM sys.tables t WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 1
    """))

    # Change tracking (graceful)
    _write_json(staging / "change_tracking.json", _query_safe("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name
        FROM sys.change_tracking_tables ct
        JOIN sys.tables t ON t.object_id = ct.object_id
    """))

    # Sensitivity (graceful)
    _write_json(staging / "sensitivity.json", _query_safe("""
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
               sc.label, sc.information_type, COL_NAME(sc.major_id, sc.minor_id) AS column_name
        FROM sys.sensitivity_classifications sc
        JOIN sys.tables t ON t.object_id = sc.major_id WHERE t.is_ms_shipped = 0
    """))

    # Object types
    _write_json(staging / "object_types.json", _query("""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name, o.type
        FROM sys.objects o
        WHERE o.is_ms_shipped = 0 AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF')
    """))

    # Definitions (for routing flags)
    _write_json(staging / "definitions.json", _query("""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS object_name,
               OBJECT_DEFINITION(o.object_id) AS definition
        FROM sys.objects o
        WHERE o.type IN ('P', 'V', 'FN', 'IF', 'TF') AND o.is_ms_shipped = 0
    """))

    # Proc params
    _write_json(staging / "proc_params.json", _query("""
        SELECT SCHEMA_NAME(o.schema_id) AS schema_name, o.name AS proc_name,
               p.name AS param_name, TYPE_NAME(p.user_type_id) AS type_name,
               p.max_length, p.precision, p.scale, p.is_output, p.has_default_value
        FROM sys.parameters p
        JOIN sys.objects o ON o.object_id = p.object_id
        WHERE o.type = 'P' AND o.is_ms_shipped = 0 AND p.parameter_id > 0
    """))

    # DMF refs (using export_ddl helper)
    sys.path.insert(0, str(SHARED_DIR))
    from shared.export_ddl import _extract_dmf_refs
    _write_json(staging / "proc_dmf.json", _extract_dmf_refs(conn, "P"))
    _write_json(staging / "view_dmf.json", _extract_dmf_refs(conn, "V"))
    _write_json(staging / "func_dmf.json", _extract_dmf_refs(conn, "FN,IF,TF"))

    conn.close()
    return staging


@pytest.mark.integration
class TestIntegrationWriteCatalog:
    """Compare setup-ddl write-catalog output against export_ddl --catalog output."""

    def test_catalog_counts_match(self, integration_output, staging_from_export, tmp_path):
        cli_output = tmp_path / "cli_output"

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging_from_export),
            "--output-folder", str(cli_output),
            "--database", "MigrationTest",
        ])
        assert result.returncode == 0, result.stderr

        # Count files in both outputs
        ref_tables = list((integration_output / "catalog" / "tables").glob("*.json"))
        cli_tables = list((cli_output / "catalog" / "tables").glob("*.json"))
        assert len(cli_tables) == len(ref_tables), f"Table count: {len(cli_tables)} vs {len(ref_tables)}"

        ref_procs = list((integration_output / "catalog" / "procedures").glob("*.json"))
        cli_procs = list((cli_output / "catalog" / "procedures").glob("*.json"))
        assert len(cli_procs) == len(ref_procs), f"Proc count: {len(cli_procs)} vs {len(ref_procs)}"

    def test_table_signals_match(self, integration_output, staging_from_export, tmp_path):
        cli_output = tmp_path / "cli_output"

        _run_cli([
            "write-catalog",
            "--staging-dir", str(staging_from_export),
            "--output-folder", str(cli_output),
            "--database", "MigrationTest",
        ])

        # Check a specific table's catalog signals
        for table_file in (integration_output / "catalog" / "tables").glob("test_catalog.*.json"):
            ref = json.loads(table_file.read_text())
            cli_file = cli_output / "catalog" / "tables" / table_file.name
            assert cli_file.exists(), f"Missing: {table_file.name}"
            cli = json.loads(cli_file.read_text())

            # schema/name fields should be present in CLI output
            assert "schema" in cli
            assert "name" in cli

            # Signals should match
            assert cli["primary_keys"] == ref["primary_keys"], f"{table_file.name} PKs differ"
            assert cli["foreign_keys"] == ref["foreign_keys"], f"{table_file.name} FKs differ"

    def test_proc_refs_match(self, integration_output, staging_from_export, tmp_path):
        cli_output = tmp_path / "cli_output"

        _run_cli([
            "write-catalog",
            "--staging-dir", str(staging_from_export),
            "--output-folder", str(cli_output),
            "--database", "MigrationTest",
        ])

        # Compare proc references for test_catalog procs
        for proc_file in (integration_output / "catalog" / "procedures").glob("test_catalog.*.json"):
            ref = json.loads(proc_file.read_text())
            cli_file = cli_output / "catalog" / "procedures" / proc_file.name
            assert cli_file.exists(), f"Missing: {proc_file.name}"
            cli = json.loads(cli_file.read_text())

            # schema/name fields
            assert "schema" in cli
            assert "name" in cli

            # References should match
            for obj_type in ("tables", "views", "functions", "procedures"):
                ref_in = ref["references"][obj_type]["in_scope"]
                cli_in = cli["references"][obj_type]["in_scope"]
                ref_names = {(r["schema"], r["name"]) for r in ref_in}
                cli_names = {(r["schema"], r["name"]) for r in cli_in}
                assert cli_names == ref_names, f"{proc_file.name} {obj_type} in_scope differ"
