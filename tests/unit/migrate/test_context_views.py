from __future__ import annotations

from pathlib import Path


from shared.migrate import (
    run_context,
)
from tests.unit.migrate.helpers import (
    _seed_migrate_fixture,
    _write_catalog,
)


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

        assert "silver.vw_customer_360" in result.source_tables
        assert result.source_columns is not None
        assert "silver.vw_customer_360" in result.source_columns
        view_cols = result.source_columns["silver.vw_customer_360"]
        assert len(view_cols) == 2
        assert view_cols[0]["name"] == "customer_id"

    def test_source_columns_has_entry_per_source(self, ddl_path: Path) -> None:
        """source_columns has an entry for every source, even without catalog."""
        result = run_context(ddl_path, "silver.DimProduct", "dbo.usp_load_dim_product")
        assert result.source_columns is not None
        assert "bronze.product" in result.source_tables
        # Every source_table has an entry in source_columns (may be empty)
        for fqn in result.source_tables:
            assert fqn in result.source_columns
