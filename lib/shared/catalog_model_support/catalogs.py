from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from shared.catalog_model_support.base import _CATALOG_CONFIG
from shared.catalog_model_support.enrichment import GenerateSection, RefactorSection, TestGenSection
from shared.catalog_model_support.profile import TableProfileSection, ViewProfileSection
from shared.catalog_model_support.references import ReferencedByBucket, ReferencesBucket
from shared.catalog_model_support.scoping import TableScopingSection, ViewScopingSection

__all__ = ["FunctionCatalog", "ProcedureCatalog", "TableCatalog", "ViewCatalog"]


class TableCatalog(BaseModel):
    """Typed representation of a table catalog JSON file."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    columns: list[Any] = []
    primary_keys: list[Any] = []
    unique_indexes: list[Any] = []
    foreign_keys: list[Any] = []
    auto_increment_columns: list[Any] = []
    change_capture: Any | None = None
    sensitivity_classifications: list[Any] = []
    statements: list[Any] = []
    referenced_by: ReferencedByBucket | None = None
    scoping: TableScopingSection | None = None
    profile: TableProfileSection | None = None
    refactor: RefactorSection | None = None
    test_gen: TestGenSection | None = None
    generate: GenerateSection | None = None
    excluded: bool = False
    is_source: bool = False
    is_seed: bool = False
    ddl_hash: str | None = None
    stale: bool = False
    warnings: list[Any] = []
    errors: list[Any] = []


class ProcedureCatalog(BaseModel):
    """Typed representation of a procedure catalog JSON file."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    references: ReferencesBucket | None = None
    referenced_by: ReferencedByBucket | None = None
    params: list[Any] = []
    needs_llm: bool = False
    needs_enrich: bool = False
    mode: str | None = None
    routing_reasons: list[str] = []
    statements: list[Any] = []
    table_slices: dict[str, Any] = {}
    refactor: RefactorSection | None = None
    ddl_hash: str | None = None
    stale: bool = False
    dmf_errors: list[str] | None = None
    segmenter_error: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class ViewCatalog(BaseModel):
    """Typed representation of a view catalog JSON file."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    references: ReferencesBucket | None = None
    referenced_by: ReferencedByBucket | None = None
    is_materialized_view: bool = False
    sql: str | None = None
    columns: list[Any] = []
    primary_keys: list[Any] = []
    unique_indexes: list[Any] = []
    scoping: ViewScopingSection | None = None
    profile: ViewProfileSection | None = None
    refactor: RefactorSection | None = None
    test_gen: TestGenSection | None = None
    generate: GenerateSection | None = None
    excluded: bool = False
    ddl_hash: str | None = None
    stale: bool = False
    dmf_errors: list[str] | None = None
    segmenter_error: str | None = None
    long_truncation: bool = False
    parse_error: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class FunctionCatalog(BaseModel):
    """Typed representation of a function catalog JSON file."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    references: ReferencesBucket | None = None
    referenced_by: ReferencedByBucket | None = None
    ddl_hash: str | None = None
    stale: bool = False
    dmf_errors: list[str] | None = None
    segmenter_error: str | None = None
    subtype: str | None = None
    parse_error: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []
