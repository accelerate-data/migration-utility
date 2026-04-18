"""Tests for write-manifest, write-catalog, catalog schema/name fields, view enrichment, and diff-aware reexport."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
oracledb = pytest.importorskip("oracledb", reason="oracledb not installed")

from shared.loader_data import CorruptJSONError
from shared.setup_ddl_support.catalog_write import _catalog_type_technologies
from tests.helpers import run_setup_ddl_cli as _run_cli

from .conftest import _write_json


def _write_runtime_manifest(
    project_root: Path,
    *,
    source_technology: str = "sql_server",
    target_technology: str = "sql_server",
) -> None:
    project_root.mkdir(parents=True, exist_ok=True)
    source_dialect = "oracle" if source_technology == "oracle" else "tsql"
    target_dialect = "oracle" if target_technology == "oracle" else "tsql"
    (project_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "technology": source_technology,
                "dialect": source_dialect,
                "runtime": {
                    "source": {"technology": source_technology, "dialect": source_dialect},
                    "target": {"technology": target_technology, "dialect": target_dialect},
                },
            }
        ),
        encoding="utf-8",
    )


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
        assert manifest["runtime"]["source"]["connection"]["database"] == "TestDB"
        assert manifest["extraction"]["schemas"] == ["bronze", "silver"]
        assert "extracted_at" in manifest["extraction"]

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

    def test_write_manifest_scrubs_stale_unsupported_runtime_roles(self, tmp_path):
        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "runtime": {
                        "target": {
                            "technology": "duckdb",
                            "dialect": "duckdb",
                            "connection": {"path": ".runtime/target.duckdb"},
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        result = _run_cli([
            "write-manifest",
            "--project-root", str(tmp_path),
            "--technology", "sql_server",
            "--database", "TestDB",
            "--schemas", "bronze",
        ])

        assert result.returncode == 0, result.stderr
        manifest = json.loads((tmp_path / "manifest.json").read_text())
        assert "target" not in manifest["runtime"]
        assert manifest["runtime"]["source"]["technology"] == "sql_server"


# ── Unit: write-catalog ──────────────────────────────────────────────────────


class TestWriteCatalog:
    def test_table_columns_persist_source_canonical_and_target_types(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()
        (output / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "oracle",
                    "dialect": "oracle",
                    "runtime": {
                        "source": {"technology": "oracle", "dialect": "oracle"},
                        "target": {"technology": "sql_server", "dialect": "tsql"},
                    },
                }
            ),
            encoding="utf-8",
        )

        _write_json(staging / "table_columns.json", [
            {"schema_name": "SH", "table_name": "SALES", "column_name": "AMOUNT", "column_id": 1,
             "type_name": "NUMBER", "max_length": 0, "precision": 10, "scale": 2,
             "is_nullable": True, "is_identity": False},
            {"schema_name": "SH", "table_name": "SALES", "column_name": "CHANNEL", "column_id": 2,
             "type_name": "VARCHAR2", "max_length": 20, "precision": 0, "scale": 0,
             "is_nullable": False, "is_identity": False},
        ])
        _write_json(staging / "pk_unique.json", [])
        _write_json(staging / "foreign_keys.json", [])
        _write_json(staging / "identity_columns.json", [])
        _write_json(staging / "cdc.json", [])
        _write_json(staging / "object_types.json", [
            {"schema_name": "SH", "name": "SALES", "type": "U"},
        ])
        _write_json(staging / "definitions.json", [])
        _write_json(staging / "proc_dmf.json", [])
        _write_json(staging / "view_dmf.json", [])
        _write_json(staging / "func_dmf.json", [])
        _write_json(staging / "proc_params.json", [])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "",
        ])

        assert result.returncode == 0, result.stderr
        table_cat = json.loads((output / "catalog" / "tables" / "sh.sales.json").read_text())
        assert table_cat["columns"] == [
            {
                "name": "AMOUNT",
                "source_sql_type": "NUMBER(10,2)",
                "canonical_tsql_type": "DECIMAL(10,2)",
                "sql_type": "DECIMAL(10,2)",
                "is_nullable": True,
                "is_identity": False,
            },
            {
                "name": "CHANNEL",
                "source_sql_type": "VARCHAR2(20)",
                "canonical_tsql_type": "VARCHAR(20)",
                "sql_type": "VARCHAR(20)",
                "is_nullable": False,
                "is_identity": False,
            },
        ]

    def test_catalog_type_technologies_requires_manifest_and_target_role(self, tmp_path):
        with pytest.raises(ValueError, match="manifest.json not found"):
            _catalog_type_technologies(tmp_path)

        (tmp_path / "manifest.json").write_text(
            json.dumps(
                {
                    "technology": "oracle",
                    "runtime": {
                        "source": {"technology": "oracle", "dialect": "oracle"},
                    },
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="target technology"):
            _catalog_type_technologies(tmp_path)

        _write_runtime_manifest(tmp_path, source_technology="oracle", target_technology="sql_server")
        assert _catalog_type_technologies(tmp_path) == ("oracle", "sql_server")

    def test_catalog_type_technologies_rejects_malformed_manifest(self, tmp_path):
        (tmp_path / "manifest.json").write_text("{not json", encoding="utf-8")

        with pytest.raises(CorruptJSONError):
            _catalog_type_technologies(tmp_path)

    def test_write_catalog_malformed_manifest_exits_without_traceback(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()
        (output / "manifest.json").write_text("{not json", encoding="utf-8")
        _write_json(staging / "table_columns.json", [])
        _write_json(staging / "pk_unique.json", [])
        _write_json(staging / "foreign_keys.json", [])
        _write_json(staging / "identity_columns.json", [])
        _write_json(staging / "cdc.json", [])
        _write_json(staging / "object_types.json", [])
        _write_json(staging / "definitions.json", [])
        _write_json(staging / "proc_dmf.json", [])
        _write_json(staging / "view_dmf.json", [])
        _write_json(staging / "func_dmf.json", [])
        _write_json(staging / "proc_params.json", [])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        assert result.returncode == 2
        assert "Corrupt JSON in" in result.stderr
        assert "Traceback" not in result.stderr

    def test_writes_catalog_from_staging(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _write_runtime_manifest(output)

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
        _write_runtime_manifest(output)

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
        _write_runtime_manifest(output)

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
        from shared.catalog import write_proc_catalog
        from shared.dmf_processing import empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_proc_catalog(tmp_path, "silver.usp_load_Dim", refs)
        data = json.loads((tmp_path / "catalog" / "procedures" / "silver.usp_load_dim.json").read_text())
        assert data["schema"] == "silver"
        assert data["name"] == "usp_load_dim"

    def test_view_catalog_has_schema_name(self, tmp_path):
        from shared.catalog import write_view_catalog
        from shared.dmf_processing import empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_view_catalog(tmp_path, "dbo.vw_Sales", refs)
        data = json.loads((tmp_path / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert data["schema"] == "dbo"
        assert data["name"] == "vw_sales"
        # No sql or columns written when not provided
        assert "sql" not in data
        assert "columns" not in data

    def test_view_catalog_sql_and_columns_written(self, tmp_path):
        from shared.catalog import write_view_catalog
        from shared.dmf_processing import empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_view_catalog(
            tmp_path, "dbo.vw_Sales", refs,
            sql="CREATE VIEW [dbo].[vw_Sales] AS SELECT id FROM [dbo].[Sales]",
            columns=[{"name": "id", "sql_type": "INT", "is_nullable": False}],
        )
        data = json.loads((tmp_path / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert data["sql"] == "CREATE VIEW [dbo].[vw_Sales] AS SELECT id FROM [dbo].[Sales]"
        assert len(data["columns"]) == 1
        assert data["columns"][0]["name"] == "id"
        assert data["columns"][0]["sql_type"] == "INT"
        assert data["columns"][0]["is_nullable"] is False

    def test_proc_catalog_unaffected_by_view_fields(self, tmp_path):
        from shared.catalog import write_proc_catalog
        from shared.dmf_processing import empty_scoped
        refs = {"tables": empty_scoped(), "views": empty_scoped(),
                "functions": empty_scoped(), "procedures": empty_scoped()}
        write_proc_catalog(tmp_path, "dbo.usp_load", refs)
        data = json.loads((tmp_path / "catalog" / "procedures" / "dbo.usp_load.json").read_text())
        assert "sql" not in data
        assert "columns" not in data


# ── Unit: view column map helpers ─────────────────────────────────────────────


class TestViewEnrichmentHelpers:
    """Tests for _build_view_columns_map and _build_view_definitions_map."""

    def test_build_view_columns_map_basic(self):
        from shared.setup_ddl_support.staging import build_view_columns_map
        rows = [
            {"schema_name": "dbo", "view_name": "vw_Sales", "column_name": "id",
             "column_id": 1, "type_name": "int", "max_length": 4, "precision": 10,
             "scale": 0, "is_nullable": False},
            {"schema_name": "dbo", "view_name": "vw_Sales", "column_name": "name",
             "column_id": 2, "type_name": "nvarchar", "max_length": 200, "precision": 0,
             "scale": 0, "is_nullable": True},
        ]
        result = build_view_columns_map(rows)
        assert "dbo.vw_sales" in result
        cols = result["dbo.vw_sales"]
        assert len(cols) == 2
        assert cols[0]["name"] == "id"
        assert cols[0]["is_nullable"] is False
        assert cols[1]["name"] == "name"
        assert cols[1]["is_nullable"] is True
        # column_id sentinel key must not leak into output
        assert "_column_id" not in cols[0]

    def test_build_view_columns_map_ordering(self):
        from shared.setup_ddl_support.staging import build_view_columns_map
        rows = [
            {"schema_name": "dbo", "view_name": "vw_x", "column_name": "z",
             "column_id": 3, "type_name": "int", "max_length": 4, "precision": 10,
             "scale": 0, "is_nullable": False},
            {"schema_name": "dbo", "view_name": "vw_x", "column_name": "a",
             "column_id": 1, "type_name": "int", "max_length": 4, "precision": 10,
             "scale": 0, "is_nullable": False},
        ]
        result = build_view_columns_map(rows)
        cols = result["dbo.vw_x"]
        assert [c["name"] for c in cols] == ["a", "z"]

    def test_build_view_definitions_map_filters_to_views(self):
        from shared.setup_ddl_support.staging import build_view_definitions_map
        definitions = [
            {"schema_name": "dbo", "object_name": "vw_sales",
             "definition": "CREATE VIEW [dbo].[vw_sales] AS SELECT 1"},
            {"schema_name": "dbo", "object_name": "usp_load",
             "definition": "CREATE PROC [dbo].[usp_load] AS SELECT 1"},
        ]
        object_types = {"dbo.vw_sales": "views", "dbo.usp_load": "procedures"}
        result = build_view_definitions_map(definitions, object_types)
        assert "dbo.vw_sales" in result
        assert "dbo.usp_load" not in result
        assert "CREATE VIEW" in result["dbo.vw_sales"]

    def test_build_view_definitions_map_skips_null_definition(self):
        from shared.setup_ddl_support.staging import build_view_definitions_map
        definitions = [
            {"schema_name": "dbo", "object_name": "vw_empty", "definition": None},
        ]
        object_types = {"dbo.vw_empty": "views"}
        result = build_view_definitions_map(definitions, object_types)
        assert "dbo.vw_empty" not in result


# ── Unit: write-catalog with view enrichment ─────────────────────────────────


class TestWriteCatalogViewEnrichment:
    """Verify write-catalog propagates view sql and columns to catalog files."""

    def _minimal_staging(self, staging: Path, *, with_view: bool = True) -> None:
        _write_json(staging / "table_columns.json", [])
        _write_json(staging / "pk_unique.json", [])
        _write_json(staging / "foreign_keys.json", [])
        _write_json(staging / "identity_columns.json", [])
        _write_json(staging / "cdc.json", [])
        _write_json(staging / "proc_dmf.json", [])
        _write_json(staging / "func_dmf.json", [])
        _write_json(staging / "proc_params.json", [])
        view_obj_type = [{"schema_name": "dbo", "name": "vw_sales", "type": "V"}] if with_view else []
        _write_json(staging / "object_types.json", view_obj_type)
        _write_json(staging / "definitions.json", [
            {"schema_name": "dbo", "object_name": "vw_sales",
             "definition": "CREATE VIEW [dbo].[vw_sales] AS SELECT id FROM [dbo].[orders]"},
        ] if with_view else [])
        _write_json(staging / "view_dmf.json", [])

    def test_view_catalog_gets_sql_from_definitions(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _write_runtime_manifest(output)
        self._minimal_staging(staging)
        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        view_cat = json.loads((output / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert "sql" in view_cat
        assert "CREATE VIEW" in view_cat["sql"]

    def test_view_catalog_gets_columns_from_view_columns_json(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _write_runtime_manifest(output)
        self._minimal_staging(staging)
        _write_json(staging / "view_columns.json", [
            {"schema_name": "dbo", "view_name": "vw_sales", "column_name": "id",
             "column_id": 1, "type_name": "int", "max_length": 4, "precision": 10,
             "scale": 0, "is_nullable": False},
            {"schema_name": "dbo", "view_name": "vw_sales", "column_name": "amount",
             "column_id": 2, "type_name": "decimal", "max_length": 9, "precision": 18,
             "scale": 2, "is_nullable": True},
        ])
        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        view_cat = json.loads((output / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert "columns" in view_cat
        assert view_cat["columns"] == [
            {
                "name": "id",
                "source_sql_type": "INT",
                "canonical_tsql_type": "INT",
                "sql_type": "INT",
                "is_nullable": False,
            },
            {
                "name": "amount",
                "source_sql_type": "DECIMAL(18,2)",
                "canonical_tsql_type": "DECIMAL(18,2)",
                "sql_type": "DECIMAL(18,2)",
                "is_nullable": True,
            },
        ]

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        assert result.returncode == 0, result.stderr
        rerun_view_cat = json.loads((output / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert rerun_view_cat["columns"] == view_cat["columns"]

    def test_oracle_view_columns_map_to_sql_server_target_types(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        output.mkdir()
        (output / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0",
                    "technology": "oracle",
                    "dialect": "oracle",
                    "runtime": {
                        "source": {"technology": "oracle", "dialect": "oracle"},
                        "target": {"technology": "sql_server", "dialect": "tsql"},
                    },
                }
            ),
            encoding="utf-8",
        )
        self._minimal_staging(staging)
        _write_json(staging / "view_columns.json", [
            {"schema_name": "SH", "view_name": "VW_SALES", "column_name": "NAME",
             "column_id": 1, "type_name": "NVARCHAR2", "max_length": 20,
             "precision": 0, "scale": 0, "is_nullable": True},
        ])
        _write_json(staging / "object_types.json", [
            {"schema_name": "SH", "name": "VW_SALES", "type": "V"},
        ])
        _write_json(staging / "definitions.json", [
            {"schema_name": "SH", "object_name": "VW_SALES",
             "definition": "CREATE VIEW SH.VW_SALES AS SELECT NAME FROM SH.SALES"},
        ])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "",
        ])

        assert result.returncode == 0, result.stderr
        view_cat = json.loads((output / "catalog" / "views" / "sh.vw_sales.json").read_text())
        assert view_cat["columns"] == [
            {
                "name": "NAME",
                "source_sql_type": "NVARCHAR2(20)",
                "canonical_tsql_type": "NVARCHAR(20)",
                "sql_type": "NVARCHAR(20)",
                "is_nullable": True,
            }
        ]

    def test_view_columns_absent_when_no_view_columns_file(self, tmp_path):
        staging = tmp_path / "staging"
        output = tmp_path / "output"
        _write_runtime_manifest(output)
        self._minimal_staging(staging)
        # No view_columns.json written — the field should be absent from catalog
        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])
        assert result.returncode == 0, result.stderr
        view_cat = json.loads((output / "catalog" / "views" / "dbo.vw_sales.json").read_text())
        assert "columns" not in view_cat


# ── Unit: diff-aware reexport ────────────────────────────────────────────────


def _make_staging(staging: Path, *, definition: str = "CREATE PROC dbo.usp_a AS INSERT INTO dbo.T1 (id) VALUES (1)") -> None:
    """Write a minimal set of staging files for write-catalog tests."""
    _write_runtime_manifest(staging.parent / "output")
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

    def test_changed_table_rewrite_preserves_catalog_type_fields(self, tmp_path):
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

        table_path = output / "catalog" / "tables" / "dbo.t1.json"
        table_cat = json.loads(table_path.read_text(encoding="utf-8"))
        table_cat["scoping"] = {"status": "resolved", "selected_writer": "dbo.usp_a"}
        table_cat["profile"] = {
            "status": "ok",
            "classification": {"resolved_kind": "dim_non_scd", "source": "catalog"},
            "primary_key": {"columns": ["id"], "primary_key_type": "natural"},
        }
        table_path.write_text(json.dumps(table_cat, indent=2) + "\n", encoding="utf-8")

        _write_json(staging / "table_columns.json", [
            {"schema_name": "dbo", "table_name": "T1", "column_name": "id", "column_id": 1,
             "type_name": "bigint", "max_length": 8, "precision": 19, "scale": 0,
             "is_nullable": False, "is_identity": False},
        ])

        result = _run_cli([
            "write-catalog",
            "--staging-dir", str(staging),
            "--project-root", str(output),
            "--database", "TestDB",
        ])

        assert result.returncode == 0, result.stderr
        counts = json.loads(result.stdout)
        assert counts["changed"] >= 1
        table_cat = json.loads(table_path.read_text(encoding="utf-8"))
        assert table_cat["columns"] == [
            {
                "name": "id",
                "source_sql_type": "BIGINT",
                "canonical_tsql_type": "BIGINT",
                "sql_type": "BIGINT",
                "is_nullable": False,
                "is_identity": False,
            }
        ]
        assert table_cat["scoping"]["status"] == "resolved"
        assert table_cat["profile"]["status"] == "ok"

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
        assert "catalog/procedures/dbo.usp_a.json" in counts["written_paths"]

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
