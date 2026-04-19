"""Tests for top-level catalog model support module exports."""

from __future__ import annotations


def test_catalog_support_exports_top_level_catalog_models() -> None:
    from shared.catalog_model_support.catalogs import (
        FunctionCatalog,
        ProcedureCatalog,
        TableCatalog,
        ViewCatalog,
    )

    table = TableCatalog.model_validate({"schema": "silver", "name": "DimCustomer"})
    proc = ProcedureCatalog.model_validate({"schema": "dbo", "name": "usp_load"})
    view = ViewCatalog.model_validate({"schema": "silver", "name": "vw_sales"})
    func = FunctionCatalog.model_validate({"schema": "dbo", "name": "fn_clean"})

    assert table.object_schema == "silver"
    assert table.model_dump(by_alias=True)["schema"] == "silver"
    assert proc.references is None
    assert view.is_materialized_view is False
    assert func.subtype is None
