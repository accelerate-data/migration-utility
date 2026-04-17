from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    ObjectNotFoundError,
)
from tests.unit.discover.discover_test_helpers import (
    _CATALOG_FIXTURES,
    _FLAT_FIXTURES,
    _LISTING_OBJECTS_EVAL_FIXTURES,
    _SOURCE_TABLE_GUARD_FIXTURES,
    _UNPARSEABLE_FIXTURES,
    _make_proc_cat,
    _make_project_with_corrupt_catalog,
    _make_project_with_proc_view_refs,
    _make_project_with_view_catalog,
    _make_table_cat,
)

def test_refs_catalog_finds_writers() -> None:
    """refs uses catalog data when catalog/tables/*.json exists."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.FactSales")
    assert result.source == "catalog"
    writer_names = [w.procedure for w in result.writers]
    assert "dbo.usp_load_fact_sales" in writer_names
    # Writer has is_updated flag
    writer = next(w for w in result.writers if w.procedure == "dbo.usp_load_fact_sales")
    assert writer.is_updated is True

def test_refs_catalog_finds_readers() -> None:
    """refs catalog path correctly identifies readers (is_selected only)."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.FactSales")
    assert result.source == "catalog"
    assert "dbo.usp_read_fact_sales" in result.readers
    assert "dbo.vw_sales_summary" in result.readers

def test_refs_catalog_no_confidence() -> None:
    """Catalog-path refs output has no confidence or status fields."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.FactSales")
    assert result.source == "catalog"
    for w in result.writers:
        w_dict = w.model_dump()
        assert "confidence" not in w_dict
        assert "status" not in w_dict

def test_refs_procedure_returns_payload_error() -> None:
    """refs on a procedure returns an error payload instead of raising."""
    result = discover.run_refs(_LISTING_OBJECTS_EVAL_FIXTURES, "silver.usp_load_dimproduct")
    assert result.error is not None
    assert "refs only works for tables, views, and functions" in result.error
    assert result.readers == []
    assert result.writers == []

def test_refs_missing_target_returns_payload_error() -> None:
    """refs on a missing target returns a payload error instead of raising."""
    result = discover.run_refs(_CATALOG_FIXTURES.parent, "silver.DoesNotExist")
    assert result.error == "no catalog file for silver.doesnotexist — it may not exist in the extracted schemas"
    assert result.readers == []
    assert result.writers == []

def test_refs_errors_without_catalog() -> None:
    """refs raises CatalogNotFoundError when no catalog/ directory exists."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        ddl_dir = p / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE dbo.T (Id INT)\nGO\n", encoding="utf-8",
        )
        with pytest.raises(CatalogNotFoundError):
            discover.run_refs(p, "dbo.T")

def test_refs_corrupt_table_catalog_raises() -> None:
    """refs with corrupt table catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_refs(root, "dbo.T")

def test_refs_view_catalog_returns_view_type() -> None:
    """run_refs on a view FQN returns type='view' and the referencing proc as reader."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_view_catalog(Path(tmp))
        result = discover.run_refs(root, "dbo.vw_customer_dim")

    assert result.source == "catalog"
    assert result.type == "view"
    assert "dbo.usp_load_fact_sales" in result.readers
