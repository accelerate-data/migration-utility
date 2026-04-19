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


def test_procedure_catalog_default_serialized_shape_is_stable() -> None:
    from shared.catalog_model_support.catalogs import ProcedureCatalog

    proc = ProcedureCatalog.model_validate({"schema": "dbo", "name": "usp_load"})

    assert proc.model_dump(by_alias=True) == {
        "schema": "dbo",
        "name": "usp_load",
        "references": None,
        "referenced_by": None,
        "params": [],
        "needs_llm": False,
        "needs_enrich": False,
        "mode": None,
        "routing_reasons": [],
        "statements": [],
        "table_slices": {},
        "refactor": None,
        "ddl_hash": None,
        "stale": False,
        "dmf_errors": None,
        "segmenter_error": None,
        "warnings": [],
        "errors": [],
    }


def test_view_catalog_default_serialized_shape_is_stable() -> None:
    from shared.catalog_model_support.catalogs import ViewCatalog

    view = ViewCatalog.model_validate({"schema": "silver", "name": "vw_sales"})

    assert view.model_dump(by_alias=True) == {
        "schema": "silver",
        "name": "vw_sales",
        "references": None,
        "referenced_by": None,
        "is_materialized_view": False,
        "sql": None,
        "columns": [],
        "primary_keys": [],
        "unique_indexes": [],
        "scoping": None,
        "profile": None,
        "refactor": None,
        "test_gen": None,
        "generate": None,
        "excluded": False,
        "ddl_hash": None,
        "stale": False,
        "dmf_errors": None,
        "segmenter_error": None,
        "long_truncation": False,
        "parse_error": None,
        "warnings": [],
        "errors": [],
    }


def test_function_catalog_default_serialized_shape_is_stable() -> None:
    from shared.catalog_model_support.catalogs import FunctionCatalog

    func = FunctionCatalog.model_validate({"schema": "dbo", "name": "fn_clean"})

    assert func.model_dump(by_alias=True) == {
        "schema": "dbo",
        "name": "fn_clean",
        "references": None,
        "referenced_by": None,
        "ddl_hash": None,
        "stale": False,
        "dmf_errors": None,
        "segmenter_error": None,
        "subtype": None,
        "parse_error": None,
        "warnings": [],
        "errors": [],
    }
