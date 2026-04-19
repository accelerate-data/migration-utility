from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.catalog_models import TableProfileSection
from shared.loader import CatalogLoadError, ProfileMissingError
from shared.migrate import (
    derive_materialization,
    derive_schema_tests,
    run_context,
)
from tests.unit.migrate.helpers import (
    _seed_migrate_fixture,
    _write_catalog,
)


class TestDeriveMaterialization:
    """Materialization derivation from profile classification and watermark."""

    def test_dim_scd2_returns_snapshot(self) -> None:
        profile = {"classification": {"resolved_kind": "dim_scd2"}, "watermark": {"column": "valid_from"}}
        assert derive_materialization(profile) == "snapshot"

    def test_dim_scd2_typed_profile_returns_snapshot(self) -> None:
        profile = TableProfileSection.model_validate({
            "classification": {"resolved_kind": "dim_scd2"},
            "watermark": {"column": "valid_from"},
        })
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

    def test_view_profile_stg_returns_view(self) -> None:
        profile = {"classification": "stg"}
        assert derive_materialization(profile) == "view"

    def test_view_profile_mart_returns_view(self) -> None:
        profile = {"classification": "mart"}
        assert derive_materialization(profile) == "view"

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

    def test_typed_profile_legacy_pk_column_produces_entity_integrity(self) -> None:
        profile = TableProfileSection.model_validate({
            "primary_key": {"column": "sale_id", "primary_key_type": "surrogate"},
        })
        tests = derive_schema_tests(profile)
        assert tests["entity_integrity"] == [
            {"column": "sale_id", "tests": ["unique", "not_null"]}
        ]

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
        assert ri["to"] == "ref('dimcustomer')"
        assert ri["field"] == "customer_sk"

    def test_typed_profile_legacy_fk_columns_produces_relationships(self) -> None:
        profile = TableProfileSection.model_validate({
            "foreign_keys": [
                {
                    "columns": ["customer_sk"],
                    "references_table": "silver.dimcustomer",
                    "references_column": "customer_sk",
                }
            ],
        })
        tests = derive_schema_tests(profile)
        assert tests["referential_integrity"] == [
            {"column": "customer_sk", "to": "ref('dimcustomer')", "field": "customer_sk"}
        ]

    def test_watermark_produces_recency(self) -> None:
        profile = {"watermark": {"column": "load_date"}}
        tests = derive_schema_tests(profile)
        assert tests["recency"] == {"column": "load_date"}

    def test_typed_profile_legacy_watermark_columns_produces_recency(self) -> None:
        profile = TableProfileSection.model_validate({
            "watermark": {"columns": ["load_date"]},
        })
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

    def test_typed_profile_legacy_pii_action_produces_suggested_action(self) -> None:
        profile = TableProfileSection.model_validate({
            "pii_actions": [{"column": "email", "action": "drop"}],
        })
        tests = derive_schema_tests(profile)
        assert tests["pii"] == [{"column": "email", "suggested_action": "drop"}]

    def test_empty_profile_returns_empty_tests(self) -> None:
        tests = derive_schema_tests({})
        assert tests == {}

class TestRunContext:
    """Context assembly from catalog + DDL."""

    def test_full_context_all_fields_present(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

        assert result.table == "silver.factsales"
        assert result.writer == "dbo.usp_load_fact_sales"
        assert result.needs_llm is False
        assert result.profile["classification"]["resolved_kind"] == "fact_transaction"
        assert result.materialization == "incremental"
        assert len(result.statements) >= 1
        assert result.statements[0]["action"] == "migrate"
        assert result.proc_body is not None
        assert len(result.proc_body) > 0
        assert isinstance(result.columns, list)
        assert isinstance(result.source_tables, list)
        assert "bronze.sales" in result.source_tables
        assert result.schema_tests is not None
        assert result.refactored_sql is not None
        assert len(result.refactored_sql) > 0
        assert result.selected_writer_ddl_slice is None
        assert not hasattr(result, "writer_ddl_slice")

    def test_context_uses_selected_writer_slice_without_full_proc_body(self, ddl_path: Path) -> None:
        """Sliced writers expose only the selected table slice to LLM-facing context."""
        proc_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
        proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
        proc_cat["table_slices"] = {
            "silver.factsales": "insert into silver.FactSales select sale_id from bronze.Sales"
        }
        proc_cat["references"]["tables"]["in_scope"].append(
            {"schema": "bronze", "name": "Unrelated", "is_selected": True, "is_updated": False}
        )
        proc_path.write_text(json.dumps(proc_cat, indent=2) + "\n", encoding="utf-8")

        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

        assert result.selected_writer_ddl_slice == "insert into silver.FactSales select sale_id from bronze.Sales"
        assert result.proc_body == ""
        assert result.statements == []
        assert result.source_tables == ["bronze.sales"]
        assert set(result.source_columns) == {"bronze.sales"}
        assert not hasattr(result, "writer_ddl_slice")

    def test_context_missing_selected_writer_slice_raises(self, ddl_path: Path) -> None:
        """A sliced writer without a target-table slice is not safe LLM context."""
        proc_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
        proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
        proc_cat["table_slices"] = {
            "silver.other": "insert into silver.Other select sale_id from bronze.Sales"
        }
        proc_path.write_text(json.dumps(proc_cat, indent=2) + "\n", encoding="utf-8")

        with pytest.raises(ValueError, match="no slice exists for target silver\\.factsales"):
            run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

    def test_context_selected_writer_slice_uses_manifest_dialect(self, ddl_path: Path) -> None:
        """Selected-slice source extraction uses the project dialect, not a T-SQL default."""
        manifest_path = ddl_path / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["technology"] = "oracle"
        manifest["dialect"] = "oracle"
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

        proc_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
        proc_cat = json.loads(proc_path.read_text(encoding="utf-8"))
        proc_cat["table_slices"] = {
            "silver.factsales": """
                INSERT INTO silver.FactSales (sale_id)
                SELECT sale_id FROM bronze.Sales
                MINUS
                SELECT sale_id FROM bronze.ReturnedSales
            """
        }
        proc_path.write_text(json.dumps(proc_cat, indent=2) + "\n", encoding="utf-8")

        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

        assert result.source_tables == ["bronze.returnedsales", "bronze.sales"]

    def test_dim_scd2_materialization_snapshot(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.DimCustomer", "dbo.usp_load_dim_customer")
        assert result.materialization == "snapshot"

    def test_no_watermark_materialization_table(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.DimProduct", "dbo.usp_load_dim_product")
        assert result.materialization == "table"

    def test_schema_tests_pk_unique_not_null(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        tests = result.schema_tests
        assert "entity_integrity" in tests
        ei = tests["entity_integrity"]
        assert any(t["column"] == "sale_id" for t in ei)
        sale_id_test = next(t for t in ei if t["column"] == "sale_id")
        assert "unique" in sale_id_test["tests"]
        assert "not_null" in sale_id_test["tests"]

    def test_schema_tests_fk_relationships(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        tests = result.schema_tests
        assert "referential_integrity" in tests
        ri = tests["referential_integrity"]
        assert any(t["column"] == "customer_sk" for t in ri)

    def test_schema_tests_recency_when_incremental(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        tests = result.schema_tests
        assert "recency" in tests
        assert tests["recency"]["column"] == "load_date"

    def test_schema_tests_pii_meta(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.DimCustomer", "dbo.usp_load_dim_customer")
        tests = result.schema_tests
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

    def test_missing_statements_returns_empty(self, ddl_path: Path) -> None:
        """Procedure without statements section returns empty statements list."""
        cat_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
        data = json.loads(cat_path.read_text())
        del data["statements"]
        cat_path.write_text(json.dumps(data))

        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")
        assert result.statements == []

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
                    "classification": {"resolved_kind": "dim_non_scd", "source": "catalog"},
                    "primary_key": {"columns": ["currency_key"], "primary_key_type": "natural"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(ddl_path, "silver.dimcurrency", "dbo.usp_load_dim_currency")

        assert result.needs_llm is False
        assert result.materialization == "table"
        assert [stmt["action"] for stmt in result.statements] == ["skip", "migrate"]

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
                    "classification": {"resolved_kind": "fact_transaction", "source": "catalog"},
                    "primary_key": {"columns": ["order_id"], "primary_key_type": "natural"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(ddl_path, "silver.factorders", "dbo.usp_load_fact_orders")

        assert result.needs_llm is False
        assert result.statements == []
        assert result.source_tables == []

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
                    "classification": {"resolved_kind": "dim_non_scd", "source": "catalog"},
                    "primary_key": {"columns": ["geo_key"], "primary_key_type": "natural"},
                    "watermark": None,
                    "foreign_keys": [],
                    "pii_actions": [],
                },
            },
        )

        result = run_context(ddl_path, "silver.dimgeography", "dbo.usp_load_dim_geography")

        assert result.needs_llm is True
        assert result.statements == []

    def test_source_columns_populated_from_view_catalog(self, ddl_path: Path) -> None:
        """When a source is a view, source_columns falls back to catalog/views/<fqn>.json."""
        view_fqn = "silver.vw_currency"
        view_columns = [
            {
                "name": "CurrencyCode",
                "source_sql_type": "NCHAR(6)",
                "canonical_tsql_type": "NCHAR(3)",
                "sql_type": "NCHAR(3)",
                "data_type": "NCHAR(6)",
            },
            {
                "name": "CurrencyName",
                "source_sql_type": "NVARCHAR(100)",
                "canonical_tsql_type": "NVARCHAR(50)",
                "sql_type": "NVARCHAR(50)",
                "type": "NVARCHAR(100)",
            },
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
                "columns": [
                    {
                        "name": "CurrencyCode",
                        "source_sql_type": "NCHAR(6)",
                        "canonical_tsql_type": "NCHAR(3)",
                        "sql_type": "NCHAR(3)",
                        "data_type": "NCHAR(6)",
                    }
                ],
                "profile": {
                    "status": "ok",
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

        assert result.source_columns is not None
        assert "silver.vw_currency" in result.source_columns
        assert result.source_columns["silver.vw_currency"] == [
            {"name": "CurrencyCode", "sql_type": "NCHAR(3)"},
            {"name": "CurrencyName", "sql_type": "NVARCHAR(50)"},
        ]
        assert result.columns == [{"name": "CurrencyCode", "sql_type": "NCHAR(3)"}]

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
