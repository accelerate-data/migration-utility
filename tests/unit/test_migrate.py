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

from shared.loader import CatalogFileMissingError, ProfileMissingError
from shared.migrate import (
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

    def test_full_context_all_fields_present(self, ddl_path: Path) -> None:
        result = run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")

        assert result["table"] == "silver.factsales"
        assert result["writer"] == "dbo.usp_load_fact_sales"
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
        """Procedure without statements section raises CatalogFileMissingError."""
        cat_path = ddl_path / "catalog" / "procedures" / "dbo.usp_load_fact_sales.json"
        data = json.loads(cat_path.read_text())
        del data["statements"]
        cat_path.write_text(json.dumps(data))

        with pytest.raises(CatalogFileMissingError):
            run_context(ddl_path, "silver.FactSales", "dbo.usp_load_fact_sales")


# ── run_write ─────────────────────────────────────────────────────────────────


class TestRunWrite:
    """Artifact writing to dbt project."""

    def test_write_valid_sql_and_yml(self, dbt_project: Path) -> None:
        model_sql = "select 1 as id"
        schema_yml = "version: 2\nmodels:\n  - name: stg_factsales\n"

        result = run_write(
            "silver.FactSales",
            Path("/tmp"),  # ddl_path not used by write
            dbt_project,
            model_sql,
            schema_yml,
        )

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
