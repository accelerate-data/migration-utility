"""Tests for migrate.py context assembly and write validation.

Fixture layout at fixtures/migrate/:
    manifest.json
    ddl/tables.sql, ddl/procedures.sql
    catalog/tables/<table>.json   (with profile sections)
    catalog/procedures/<proc>.json (with statements sections)
    dbt/dbt_project.yml + dbt/models/staging/
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from shared.loader import CatalogFileMissingError, CatalogLoadError, ProfileMissingError
from shared.migrate import (
    _load_refactored_sql,
    derive_materialization,
    derive_schema_tests,
    run_context,
    run_write,
)

FIXTURES = Path(__file__).parent / "fixtures" / "migrate"


@pytest.fixture()
def ddl_path(tmp_path: Path) -> Path:
    """Copy the migrate fixtures to a temp directory and return the path."""
    dest = tmp_path / "project"
    shutil.copytree(FIXTURES, dest)
    return dest


@pytest.fixture()
def dbt_project(tmp_path: Path) -> Path:
    """Create a minimal dbt project in a temp directory."""
    dbt = tmp_path / "dbt"
    dbt.mkdir()
    (dbt / "dbt_project.yml").write_text(
        "name: 'test_project'\nversion: '1.0.0'\nconfig-version: 2\n"
    )
    (dbt / "models" / "staging").mkdir(parents=True)
    return dbt


def _append_procedure(project_root: Path, ddl_sql: str) -> None:
    ddl_path = project_root / "ddl" / "procedures.sql"
    existing = ddl_path.read_text()
    ddl_path.write_text(f"{existing.rstrip()}\nGO\n{ddl_sql.strip()}\nGO\n")


def _write_catalog(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _seed_migrate_fixture(
    project_root: Path,
    table_fqn: str,
    writer_fqn: str,
    proc_sql: str,
    proc_catalog: dict[str, object],
    table_catalog: dict[str, object],
) -> None:
    _append_procedure(project_root, proc_sql)
    _write_catalog(
        project_root / "catalog" / "procedures" / f"{writer_fqn.lower()}.json",
        proc_catalog,
    )
    _write_catalog(
        project_root / "catalog" / "tables" / f"{table_fqn.lower()}.json",
        table_catalog,
    )


# ── derive_materialization ────────────────────────────────────────────────────


class TestDeriveMaterialization:
    """Materialization derivation from profile classification and watermark."""

    def test_dim_scd2_returns_snapshot(self) -> None:
        profile = {"classification": {"resolved_kind": "dim_scd2"}, "watermark": {"column": "valid_from"}}
        assert derive_materialization(profile) == "snapshot"

    def test_dim_scd2_without_watermark_returns_snapshot(self) -> None:
        profile = {"classification": {"resolved_kind": "dim_scd2"}, "watermark": None}
        assert derive_materialization(profile) == "snapshot"

    def test_fact_with_watermark_returns_incremental(self) -> None:
        profile = {"classification": {"resolved_kind": "fact_transaction"}, "watermark": {"column": "load_date"}}
        assert derive_materialization(profile) == "incremental"

    def test_no_watermark_returns_table(self) -> None:
        profile = {"classification": {"resolved_kind": "dim_non_scd"}, "watermark": None}
        assert derive_materialization(profile) == "table"

    def test_empty_watermark_column_returns_table(self) -> None:
        profile = {"classification": {"resolved_kind": "fact_periodic_snapshot"}, "watermark": {"column": ""}}
        assert derive_materialization(profile) == "table"

    def test_missing_watermark_key_returns_table(self) -> None:
        profile = {"classification": {"resolved_kind": "fact_periodic_snapshot"}}
        assert derive_materialization(profile) == "table"


# ── derive_schema_tests ──────────────────────────────────────────────────────


class TestDeriveSchemaTests:
    """Schema test derivation from profile answers."""

    def test_pk_produces_unique_and_not_null(self) -> None:
        profile = {
            "primary_key": {"columns": ["sale_id"], "primary_key_type": "surrogate"},
            "foreign_keys": [],
        }
        tests = derive_schema_tests(profile)
        assert "entity_integrity" in tests
        assert tests["entity_integrity"] == [
            {"column": "sale_id", "tests": ["unique", "not_null"]}
        ]

    def test_multi_column_pk(self) -> None:
        profile = {
            "primary_key": {"columns": ["order_id", "line_id"], "primary_key_type": "natural"},
        }
        tests = derive_schema_tests(profile)
        assert len(tests["entity_integrity"]) == 2
        assert tests["entity_integrity"][0]["column"] == "order_id"
        assert tests["entity_integrity"][1]["column"] == "line_id"

    def test_fk_produces_relationships(self) -> None:
        profile = {
            "foreign_keys": [
                {
                    "column": "customer_sk",
                    "references_source_relation": "silver.dimcustomer",
                    "references_column": "customer_sk",
                }
            ],
        }
        tests = derive_schema_tests(profile)
        assert "referential_integrity" in tests
        ri = tests["referential_integrity"][0]
        assert ri["column"] == "customer_sk"
        assert "ref(" in ri["to"]
        assert ri["field"] == "customer_sk"

    def test_watermark_produces_recency(self) -> None:
        profile = {"watermark": {"column": "load_date"}}
        tests = derive_schema_tests(profile)
        assert tests["recency"] == {"column": "load_date"}

    def test_no_watermark_no_recency(self) -> None:
        profile = {"watermark": None}
        tests = derive_schema_tests(profile)
        assert "recency" not in tests

    def test_pii_produces_meta_tags(self) -> None:
        profile = {
            "pii_actions": [
                {"column": "email", "suggested_action": "mask"},
                {"column": "ssn", "suggested_action": "redact"},
            ]
        }
        tests = derive_schema_tests(profile)
        assert len(tests["pii"]) == 2
        assert tests["pii"][0] == {"column": "email", "suggested_action": "mask"}

    def test_empty_profile_returns_empty_tests(self) -> None:
        tests = derive_schema_tests({})
        assert tests == {}


# ── run_context ───────────────────────────────────────────────────────────────


class TestRunContext:
    """Context assembly from catalog + DDL."""

    def test_full_context_all_fields_present(self, ddl_path: Path, assert_valid_schema) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        assert_valid_schema(result, "migrate_context_output.json")

        assert result["table"] == "silver.factsales"
        assert result["writer"] == "dbo.usp_load_fact_sales"
        assert result["needs_llm"] is False
        assert result["profile"]["classification"]["resolved_kind"] == "fact_transaction"
        assert result["materialization"] == "incremental"
        assert len(result["statements"]) >= 1
        assert result["statements"][0]["action"] == "migrate"
        assert "proc_body" in result
        assert len(result["proc_body"]) > 0
        assert isinstance(result["columns"], list)
        assert isinstance(result["source_tables"], list)
        assert "bronze.sales" in result["source_tables"]
        assert "schema_tests" in result
        assert "refactored_sql" in result
        assert len(result["refactored_sql"]) > 0

    def test_dim_scd2_materialization_snapshot(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.DimCustomer", "dbo.usp_load_dim_customer")
        assert result["materialization"] == "snapshot"

    def test_no_watermark_materialization_table(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.DimProduct", "dbo.usp_load_dim_product")
        assert result["materialization"] == "table"

    def test_schema_tests_pk_unique_not_null(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        tests = result["schema_tests"]
        assert "entity_integrity" in tests
        ei = tests["entity_integrity"]
        assert any(t["column"] == "sale_id" for t in ei)
        sale_id_test = next(t for t in ei if t["column"] == "sale_id")
        assert "unique" in sale_id_test["tests"]
        assert "not_null" in sale_id_test["tests"]

    def test_schema_tests_fk_relationships(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        tests = result["schema_tests"]
        assert "referential_integrity" in tests
        ri = tests["referential_integrity"]
        assert any(t["column"] == "customer_sk" for t in ri)

    def test_schema_tests_recency_when_incremental(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        tests = result["schema_tests"]
        assert "recency" in tests
        assert tests["recency"]["column"] == "load_date"

    def test_schema_tests_pii_meta(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.DimCustomer", "dbo.usp_load_dim_customer")
        tests = result["schema_tests"]
        assert "pii" in tests
        assert any(p["column"] == "email" for p in tests["pii"])

    def test_missing_profile_raises(self, ddl_path: Path) -> None:
        """Table without profile section raises ProfileMissingError."""
        cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
        data = json.loads(cat_path.read_text())
        del data["profile"]
        cat_path.write_text(json.dumps(data))

        with pytest.raises(ProfileMissingError):
            run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

    def test_missing_statements_raises(self, ddl_path: Path) -> None:
        """Procedure without statements section raises ValueError."""
        cat_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
        data = json.loads(cat_path.read_text())
        del data["statements"]
        cat_path.write_text(json.dumps(data))

        with pytest.raises(ValueError):
            run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

    def test_truncate_insert_context_includes_skip_and_migrate_statements(self, ddl_path: Path) -> None:
        _seed_migrate_fixture(
            ddl_path,
            "silver.dimcurrency",
            "dbo.usp_load_dim_currency",
            """
CREATE PROCEDURE [dbo].[usp_load_dim_currency]
AS
BEGIN
    TRUNCATE TABLE [silver].[DimCurrency];
    INSERT INTO [silver].[DimCurrency] (currency_key)
    SELECT CurrencyCode FROM [bronze].[Currency];
END
            """,
            {
                "schema": "dbo",
                "name": "usp_load_dim_currency",
                "mode": "deterministic",
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "silver", "name": "DimCurrency", "is_selected": False, "is_updated": True},
                            {"schema": "bronze", "name": "Currency", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "statements": [
                    {"type": "TruncateTable", "action": "skip", "source": "ast", "sql": "TRUNCATE TABLE [silver].[DimCurrency]"},
                    {
                        "type": "Insert",
                        "action": "migrate",
                        "source": "ast",
                        "sql": "INSERT INTO [silver].[DimCurrency] (currency_key) SELECT CurrencyCode FROM [bronze].[Currency]",
                    },
                ],
            },
            {
                "schema": "silver",
                "name": "dimcurrency",
                "columns": [{"name": "currency_key", "data_type": "NVARCHAR(10)", "is_nullable": False}],
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {
                    "procedures": {
                        "in_scope": [
                            {"schema": "dbo", "name": "usp_load_dim_currency", "is_selected": False, "is_updated": True}
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
                "refactor": {
                    "status": "ok",
                    "extracted_sql": "SELECT CurrencyCode FROM [bronze].[Currency]",
                    "refactored_sql": "with source_currency as (\n    select * from [bronze].[Currency]\n)\n\nselect * from source_currency",
                },
                "profile": {
                    "status": "ok",
                    "writer": "dbo.usp_load_dim_currency",
                    "classification": {"resolved_kind": "dim_non_scd", "source": "catalog"},
                    "primary_key": {"columns": ["currency_key"], "primary_key_type": "natural"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(ddl_path, "silver.dimcurrency", "dbo.usp_load_dim_currency")

        assert result["needs_llm"] is False
        assert result["materialization"] == "table"
        assert [stmt["action"] for stmt in result["statements"]] == ["skip", "migrate"]

    def test_exec_only_orchestrator_context_allows_empty_statements(self, ddl_path: Path) -> None:
        _seed_migrate_fixture(
            ddl_path,
            "silver.factorders",
            "dbo.usp_load_fact_orders",
            """
CREATE PROCEDURE [dbo].[usp_load_fact_orders]
AS
BEGIN
    EXEC [dbo].[usp_stage_fact_orders];
END
            """,
            {
                "schema": "dbo",
                "name": "usp_load_fact_orders",
                "mode": "call_graph_enrich",
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {
                        "in_scope": [{"schema": "dbo", "name": "usp_stage_fact_orders"}],
                        "out_of_scope": [],
                    },
                },
                "statements": [],
            },
            {
                "schema": "silver",
                "name": "factorders",
                "columns": [{"name": "order_id", "data_type": "BIGINT", "is_nullable": False}],
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {
                    "procedures": {
                        "in_scope": [
                            {"schema": "dbo", "name": "usp_load_fact_orders", "is_selected": False, "is_updated": True}
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
                "refactor": {
                    "status": "ok",
                    "extracted_sql": "SELECT order_id FROM [bronze].[Orders]",
                    "refactored_sql": "with source_orders as (\n    select * from [bronze].[Orders]\n)\n\nselect * from source_orders",
                },
                "profile": {
                    "status": "ok",
                    "writer": "dbo.usp_load_fact_orders",
                    "classification": {"resolved_kind": "fact_transaction", "source": "catalog"},
                    "primary_key": {"columns": ["order_id"], "primary_key_type": "natural"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(ddl_path, "silver.factorders", "dbo.usp_load_fact_orders")

        assert result["needs_llm"] is False
        assert result["statements"] == []
        assert result["source_tables"] == []

    def test_dynamic_sp_executesql_context_needs_llm(self, ddl_path: Path) -> None:
        _seed_migrate_fixture(
            ddl_path,
            "silver.dimgeography",
            "dbo.usp_load_dim_geography",
            """
CREATE PROCEDURE [dbo].[usp_load_dim_geography]
AS
BEGIN
    DECLARE @sql NVARCHAR(MAX);
    SET @sql = N'INSERT INTO [silver].[DimGeography] (geo_key) SELECT GeographyKey FROM [bronze].[Geography]';
    EXEC sp_executesql @sql;
END
            """,
            {
                "schema": "dbo",
                "name": "usp_load_dim_geography",
                "mode": "llm_required",
                "routing_reasons": ["dynamic_sql_variable"],
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "statements": [],
            },
            {
                "schema": "silver",
                "name": "dimgeography",
                "columns": [{"name": "geo_key", "data_type": "INT", "is_nullable": False}],
                "primary_keys": [],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {
                    "procedures": {
                        "in_scope": [
                            {"schema": "dbo", "name": "usp_load_dim_geography", "is_selected": False, "is_updated": True}
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                    "functions": {"in_scope": [], "out_of_scope": []},
                },
                "refactor": {
                    "status": "ok",
                    "extracted_sql": "SELECT GeographyKey FROM [bronze].[Geography]",
                    "refactored_sql": "with source_geography as (\n    select * from [bronze].[Geography]\n)\n\nselect * from source_geography",
                },
                "profile": {
                    "status": "ok",
                    "writer": "dbo.usp_load_dim_geography",
                    "classification": {"resolved_kind": "dim_non_scd", "source": "catalog"},
                    "primary_key": {"columns": ["geo_key"], "primary_key_type": "natural"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(ddl_path, "silver.dimgeography", "dbo.usp_load_dim_geography")

        assert result["needs_llm"] is True
        assert result["statements"] == []

    def test_source_columns_populated_from_view_catalog(self, ddl_path: Path) -> None:
        """When a source is a view, source_columns falls back to catalog/views/<fqn>.json."""
        view_fqn = "silver.vw_currency"
        view_columns = [
            {"name": "CurrencyCode", "data_type": "NCHAR(3)"},
            {"name": "CurrencyName", "data_type": "NVARCHAR(50)"},
        ]

        # Write view catalog (no table catalog for this FQN)
        _write_catalog(
            ddl_path / "catalog" / "views" / f"{view_fqn}.json",
            {"schema": "silver", "name": "vw_currency", "columns": view_columns},
        )

        _seed_migrate_fixture(
            ddl_path,
            "silver.factcurrencyrate",
            "dbo.usp_load_fact_currency_rate",
            """
CREATE PROCEDURE [dbo].[usp_load_fact_currency_rate]
AS
BEGIN
    INSERT INTO [silver].[FactCurrencyRate] (CurrencyCode)
    SELECT CurrencyCode FROM [silver].[vw_currency]
END
""",
            {
                "mode": "direct",
                "statements": [{"type": "insert", "action": "migrate"}],
                "references": {
                    "tables": {
                        "in_scope": [
                            {
                                "schema": "silver",
                                "name": "vw_currency",
                                "is_selected": True,
                                "is_updated": False,
                            }
                        ]
                    }
                },
            },
            {
                "schema": "silver",
                "name": "factcurrencyrate",
                "columns": [{"name": "CurrencyCode", "data_type": "NCHAR(3)"}],
                "profile": {
                    "status": "ok",
                    "writer": "dbo.usp_load_fact_currency_rate",
                    "classification": {"resolved_kind": "fact_transaction", "source": "catalog"},
                    "primary_key": None,
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(
            ddl_path, "silver.factcurrencyrate", "dbo.usp_load_fact_currency_rate"
        )

        assert "source_columns" in result
        assert "silver.vw_currency" in result["source_columns"]
        assert result["source_columns"]["silver.vw_currency"] == view_columns


class TestRunContextViewSources:
    """Context assembly includes view sources and pre-resolved columns."""

    def test_view_source_in_source_tables(self, ddl_path: Path) -> None:
        """A proc that reads from a view includes the view FQN in source_tables."""
        _seed_migrate_fixture(
            ddl_path,
            table_fqn="silver.dimcustomer",
            writer_fqn="dbo.usp_load_from_view",
            proc_sql=(
                "CREATE PROCEDURE dbo.usp_load_from_view AS "
                "INSERT INTO [silver].[DimCustomer] SELECT * FROM [silver].[vw_customer_360]"
            ),
            proc_catalog={
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "silver", "name": "DimCustomer", "is_selected": False, "is_updated": True, "is_insert_all": True},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {
                        "in_scope": [
                            {"schema": "silver", "name": "vw_customer_360", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                    "functions": {"in_scope": [], "out_of_scope": []},
                    "procedures": {"in_scope": [], "out_of_scope": []},
                },
                "statements": [
                    {"type": "Insert", "action": "migrate", "source": "ast", "sql": "INSERT INTO [silver].[DimCustomer] SELECT * FROM [silver].[vw_customer_360]"},
                ],
            },
            table_catalog={
                "schema": "silver",
                "name": "dimcustomer",
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_nullable": False},
                    {"name": "customer_name", "data_type": "NVARCHAR(100)", "is_nullable": True},
                ],
                "primary_keys": [{"constraint_name": "PK_DimCustomer", "columns": ["customer_id"], "type": "PRIMARY KEY"}],
                "unique_indexes": [],
                "foreign_keys": [],
                "auto_increment_columns": [],
                "change_capture": None,
                "sensitivity_classifications": [],
                "referenced_by": {"procedures": {"in_scope": [], "out_of_scope": []}, "views": {"in_scope": [], "out_of_scope": []}, "functions": {"in_scope": [], "out_of_scope": []}},
                "profile": {
                    "status": "ok",
                    "writer": "dbo.usp_load_from_view",
                    "classification": {"resolved_kind": "dim_non_scd", "source": "catalog"},
                    "primary_key": {"columns": ["customer_id"], "primary_key_type": "surrogate"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )
        # Also create the view catalog so source_columns can resolve
        _write_catalog(
            ddl_path / "catalog" / "views" / "silver.vw_customer_360.json",
            {
                "schema": "silver",
                "name": "vw_customer_360",
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_nullable": False},
                    {"name": "customer_name", "data_type": "NVARCHAR(100)", "is_nullable": True},
                ],
            },
        )

        result = run_context(ddl_path, "silver.DimCustomer", "dbo.usp_load_from_view")

        assert "silver.vw_customer_360" in result["source_tables"]
        assert "source_columns" in result
        assert "silver.vw_customer_360" in result["source_columns"]
        view_cols = result["source_columns"]["silver.vw_customer_360"]
        assert len(view_cols) == 2
        assert view_cols[0]["name"] == "customer_id"

    def test_source_columns_has_entry_per_source(self, ddl_path: Path) -> None:
        """source_columns has an entry for every source, even without catalog."""
        result = run_context(ddl_path, "silver.DimProduct", "dbo.usp_load_dim_product")
        assert "source_columns" in result
        assert "bronze.product" in result["source_tables"]
        # Every source_table has an entry in source_columns (may be empty)
        for fqn in result["source_tables"]:
            assert fqn in result["source_columns"]


# ── run_write ─────────────────────────────────────────────────────────────────


class TestRunWrite:
    """Artifact writing to dbt project."""

    def test_write_valid_sql_and_yml(self, dbt_project: Path, assert_valid_schema) -> None:
        model_sql = "select 1 as id"
        schema_yml = "version: 2\nmodels:\n  - name: stg_factsales\n"

        result = run_write(
            "silver.FactSales",
            Path("/tmp"),  # ddl_path not used by write
            dbt_project,
            model_sql,
            schema_yml,
        )
        assert_valid_schema(result, "migrate_write_output.json")

        assert result["status"] == "ok"
        assert len(result["written"]) == 2
        assert (dbt_project / "models" / "staging" / "stg_factsales.sql").exists()
        assert (dbt_project / "models" / "staging" / "_stg_factsales.yml").exists()

    def test_write_sql_only_no_yml(self, dbt_project: Path) -> None:
        result = run_write(
            "silver.FactSales",
            Path("/tmp"),
            dbt_project,
            "select 1 as id",
            "",
        )

        assert result["status"] == "ok"
        assert len(result["written"]) == 1
        assert (dbt_project / "models" / "staging" / "stg_factsales.sql").exists()

    def test_write_empty_sql_raises(self, dbt_project: Path) -> None:
        with pytest.raises(ValueError, match="model SQL is empty"):
            run_write("silver.FactSales", Path("/tmp"), dbt_project, "", "")

    def test_write_whitespace_sql_raises(self, dbt_project: Path) -> None:
        with pytest.raises(ValueError, match="model SQL is empty"):
            run_write("silver.FactSales", Path("/tmp"), dbt_project, "   \n  ", "")

    def test_write_nonexistent_project_raises(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "no_such_dir"
        with pytest.raises(FileNotFoundError):
            run_write("silver.FactSales", Path("/tmp"), nonexistent, "select 1", "")

    def test_write_missing_dbt_project_yml_raises(self, tmp_path: Path) -> None:
        """Directory exists but no dbt_project.yml."""
        empty_dir = tmp_path / "empty_dbt"
        empty_dir.mkdir()
        with pytest.raises(FileNotFoundError):
            run_write("silver.FactSales", Path("/tmp"), empty_dir, "select 1", "")

    def test_write_idempotent(self, dbt_project: Path) -> None:
        """Running write twice produces the same files."""
        model_sql = "select 1 as id"
        schema_yml = "version: 2\n"

        run_write("silver.FactSales", Path("/tmp"), dbt_project, model_sql, schema_yml)
        run_write("silver.FactSales", Path("/tmp"), dbt_project, model_sql, schema_yml)

        sql_file = dbt_project / "models" / "staging" / "stg_factsales.sql"
        assert sql_file.read_text() == model_sql

    def test_write_creates_staging_dir_if_missing(self, tmp_path: Path) -> None:
        """Write creates models/staging/ if it doesn't exist."""
        dbt = tmp_path / "dbt_no_staging"
        dbt.mkdir()
        (dbt / "dbt_project.yml").write_text("name: test\n")

        result = run_write("silver.FactSales", Path("/tmp"), dbt, "select 1", "")
        assert result["status"] == "ok"
        assert (dbt / "models" / "staging" / "stg_factsales.sql").exists()

    def test_write_nonexistent_project_exits_2(self, tmp_path: Path) -> None:
        """CLI write to nonexistent dbt project exits with code 2."""
        from typer.testing import CliRunner

        from shared.migrate import app

        runner = CliRunner()
        nonexistent = tmp_path / "no_such_dir"
        result = runner.invoke(app, [
            "write",
            "--table", "silver.FactSales",
            "--dbt-project-path", str(nonexistent),
            "--model-sql", "select 1",
        ])
        assert result.exit_code == 2


# ── Corrupt catalog JSON tests ──────────────────────────────────────────


def test_context_corrupt_table_catalog_raises(ddl_path: Path) -> None:
    """context with corrupt table catalog raises CatalogLoadError."""
    cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
    cat_path.write_text("{truncated", encoding="utf-8")
    with pytest.raises(CatalogLoadError):
        run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")


def test_context_corrupt_proc_catalog_raises(ddl_path: Path) -> None:
    """context with corrupt procedure catalog raises CatalogLoadError."""
    cat_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
    cat_path.write_text("{truncated", encoding="utf-8")
    with pytest.raises(CatalogLoadError):
        run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")


def test_context_missing_profile_section_raises(ddl_path: Path) -> None:
    """context with table catalog missing profile section raises ProfileMissingError."""
    cat_path = ddl_path / "catalog" / "tables" / "silver.factsales.json"
    cat = json.loads(cat_path.read_text())
    cat.pop("profile", None)
    cat_path.write_text(json.dumps(cat, indent=2) + "\n")
    with pytest.raises(ProfileMissingError):
        run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")


def test_write_does_not_read_catalog(tmp_path: Path) -> None:
    """write only writes dbt artifacts — corrupt catalog does not affect it."""
    dbt = tmp_path / "dbt"
    dbt.mkdir()
    (dbt / "dbt_project.yml").write_text("name: test\nversion: '1.0.0'\nconfig-version: 2\n")
    (dbt / "models" / "staging").mkdir(parents=True)
    result = run_write("silver.FactSales", Path("/tmp"), dbt, "select 1", "")
    assert result["status"] == "ok"


# ── _load_refactored_sql ──────────────────────────────────────────────────────


def _seed_refactor_fixture(
    tmp_path: Path,
    writer_fqn: str,
    proc_refactor: dict | None,
) -> None:
    """Write a minimal table catalog (with selected_writer) and procedure catalog."""
    table_dir = tmp_path / "catalog" / "tables"
    table_dir.mkdir(parents=True)
    proc_dir = tmp_path / "catalog" / "procedures"
    proc_dir.mkdir(parents=True)
    (tmp_path / "manifest.json").write_text("{}")
    (table_dir / "silver.mytable.json").write_text(json.dumps({
        "schema": "silver",
        "name": "mytable",
        "scoping": {"status": "resolved", "selected_writer": writer_fqn},
    }))
    proc_catalog: dict = {"schema": "dbo", "name": "usp_writer"}
    if proc_refactor is not None:
        proc_catalog["refactor"] = proc_refactor
    (proc_dir / f"{writer_fqn.lower()}.json").write_text(json.dumps(proc_catalog))


def test_load_refactored_sql_returns_sql_when_status_ok(tmp_path: Path) -> None:
    """Procedure catalog with refactor.status=ok returns refactored_sql."""
    _seed_refactor_fixture(tmp_path, "dbo.usp_writer", {"status": "ok", "refactored_sql": "SELECT 1"})
    assert _load_refactored_sql(tmp_path, "silver.mytable") == "SELECT 1"


def test_load_refactored_sql_returns_none_when_refactor_section_absent(tmp_path: Path) -> None:
    """Procedure catalog without refactor section returns None."""
    _seed_refactor_fixture(tmp_path, "dbo.usp_writer", None)
    assert _load_refactored_sql(tmp_path, "silver.mytable") is None


def test_load_refactored_sql_returns_none_when_refactored_sql_field_absent(tmp_path: Path) -> None:
    """Refactor section present but refactored_sql field missing returns None."""
    _seed_refactor_fixture(tmp_path, "dbo.usp_writer", {"status": "partial"})
    assert _load_refactored_sql(tmp_path, "silver.mytable") is None


def test_load_refactored_sql_returns_none_when_no_selected_writer(tmp_path: Path) -> None:
    """Table catalog with no selected_writer returns None."""
    table_dir = tmp_path / "catalog" / "tables"
    table_dir.mkdir(parents=True)
    (tmp_path / "manifest.json").write_text("{}")
    (table_dir / "silver.mytable.json").write_text(json.dumps({"schema": "silver", "name": "mytable"}))
    assert _load_refactored_sql(tmp_path, "silver.mytable") is None
