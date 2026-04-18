"""Tests for context_helpers.py — collect_source_tables and load_object_columns.

Validates that collect_source_tables includes both table and view references,
and that load_object_columns transparently falls back from tables to views.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.context_helpers import collect_source_tables, collect_source_tables_from_sql, load_object_columns


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    """Create a minimal project with catalog dirs and a manifest."""
    (tmp_path / "manifest.json").write_text(json.dumps({"technology": "sqlserver"}))
    (tmp_path / "catalog" / "procedures").mkdir(parents=True)
    (tmp_path / "catalog" / "tables").mkdir(parents=True)
    (tmp_path / "catalog" / "views").mkdir(parents=True)
    return tmp_path


def _write_catalog(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


# ── collect_source_tables ────────────────────────────────────────────────────


class TestCollectSourceTables:
    """collect_source_tables returns both table and view source FQNs."""

    def test_tables_only(self, project: Path) -> None:
        _write_catalog(
            project / "catalog" / "procedures" / "dbo.usp_load.json",
            {
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "bronze", "name": "Product", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {"in_scope": [], "out_of_scope": []},
                },
            },
        )
        result = collect_source_tables(project, "dbo.usp_load")
        assert result == ["bronze.product"]

    def test_views_included(self, project: Path) -> None:
        _write_catalog(
            project / "catalog" / "procedures" / "dbo.usp_load.json",
            {
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {
                        "in_scope": [
                            {"schema": "silver", "name": "vw_customer_360", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                },
            },
        )
        result = collect_source_tables(project, "dbo.usp_load")
        assert result == ["silver.vw_customer_360"]

    def test_mixed_tables_and_views(self, project: Path) -> None:
        _write_catalog(
            project / "catalog" / "procedures" / "dbo.usp_load.json",
            {
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "bronze", "name": "Product", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                    "views": {
                        "in_scope": [
                            {"schema": "silver", "name": "vw_enriched", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                },
            },
        )
        result = collect_source_tables(project, "dbo.usp_load")
        assert result == ["bronze.product", "silver.vw_enriched"]

    def test_updated_views_excluded(self, project: Path) -> None:
        """Views with is_updated=True are write targets, not sources."""
        _write_catalog(
            project / "catalog" / "procedures" / "dbo.usp_load.json",
            {
                "references": {
                    "tables": {"in_scope": [], "out_of_scope": []},
                    "views": {
                        "in_scope": [
                            {"schema": "silver", "name": "vw_target", "is_selected": False, "is_updated": True},
                            {"schema": "silver", "name": "vw_source", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                },
            },
        )
        result = collect_source_tables(project, "dbo.usp_load")
        assert result == ["silver.vw_source"]

    def test_missing_proc_returns_empty(self, project: Path) -> None:
        result = collect_source_tables(project, "dbo.nonexistent")
        assert result == []

    def test_no_views_section_returns_tables_only(self, project: Path) -> None:
        """Proc catalog without a views section still returns tables."""
        _write_catalog(
            project / "catalog" / "procedures" / "dbo.usp_legacy.json",
            {
                "references": {
                    "tables": {
                        "in_scope": [
                            {"schema": "bronze", "name": "Orders", "is_selected": True, "is_updated": False},
                        ],
                        "out_of_scope": [],
                    },
                },
            },
        )
        result = collect_source_tables(project, "dbo.usp_legacy")
        assert result == ["bronze.orders"]


# ── collect_source_tables_from_sql ───────────────────────────────────────────


class TestCollectSourceTablesFromSql:
    """Selected SQL source extraction honors the project SQL dialect."""

    def test_tsql_insert_select_sources(self) -> None:
        sql = "INSERT INTO silver.Target SELECT id FROM bronze.Product"
        result = collect_source_tables_from_sql(sql, dialect="tsql")
        assert result == ["bronze.product"]

    def test_oracle_minus_sources(self) -> None:
        sql = """
            INSERT INTO silver.Target (id)
            SELECT id FROM bronze.A
            MINUS
            SELECT id FROM bronze.B
        """
        result = collect_source_tables_from_sql(sql, dialect="oracle")
        assert result == ["bronze.a", "bronze.b"]


# ── load_object_columns ──────────────────────────────────────────────────────


class TestLoadObjectColumns:
    """load_object_columns tries tables first, then views."""

    def test_table_catalog(self, project: Path) -> None:
        _write_catalog(
            project / "catalog" / "tables" / "bronze.product.json",
            {"columns": [{"name": "id", "data_type": "INT"}]},
        )
        result = load_object_columns(project, "bronze.product")
        assert result == [{"name": "id"}]

    def test_view_catalog(self, project: Path) -> None:
        _write_catalog(
            project / "catalog" / "views" / "silver.vw_customer.json",
            {"columns": [{"name": "customer_id", "data_type": "INT"}]},
        )
        result = load_object_columns(project, "silver.vw_customer")
        assert result == [{"name": "customer_id"}]

    def test_table_takes_precedence_over_view(self, project: Path) -> None:
        """If same FQN exists in both tables/ and views/, table wins."""
        _write_catalog(
            project / "catalog" / "tables" / "silver.overlap.json",
            {"columns": [{"name": "from_table", "data_type": "INT"}]},
        )
        _write_catalog(
            project / "catalog" / "views" / "silver.overlap.json",
            {"columns": [{"name": "from_view", "data_type": "INT"}]},
        )
        result = load_object_columns(project, "silver.overlap")
        assert result[0]["name"] == "from_table"

    def test_missing_returns_empty(self, project: Path) -> None:
        result = load_object_columns(project, "silver.nonexistent")
        assert result == []
        assert result == []
