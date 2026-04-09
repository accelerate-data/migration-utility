"""Pydantic v2 models for catalog objects.

These models type the JSON files produced by ``setup-ddl`` and enriched by
skills (scoping, profiling, refactoring, test generation, model generation).
All models use ``extra="allow"`` so unknown fields pass through without
validation errors — this provides forward compatibility with future schema
additions.

The JSON schemas in ``schemas/`` remain authoritative for external validation
(AJV in evals).  These Pydantic models are the internal Python API.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ── Shared config ───────────────────────────────────────────────────────────

_CATALOG_CONFIG = ConfigDict(extra="allow", populate_by_name=True)


# ── Shared reference models ─────────────────────────────────────────────────


class RefEntry(BaseModel):
    """A single reference entry inside in_scope / out_of_scope lists."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    is_selected: bool = False
    is_updated: bool = False
    is_insert_all: bool = False
    detection: str | None = None
    columns: list[Any] = []


class ScopedRefList(BaseModel):
    """Scoped reference list: in_scope + out_of_scope."""

    model_config = ConfigDict(extra="allow")

    in_scope: list[RefEntry] = []
    out_of_scope: list[RefEntry] = []


class ReferencesBucket(BaseModel):
    """Outbound references from a proc/view/function to other objects."""

    model_config = ConfigDict(extra="allow")

    tables: ScopedRefList = ScopedRefList()
    views: ScopedRefList = ScopedRefList()
    functions: ScopedRefList = ScopedRefList()
    procedures: ScopedRefList = ScopedRefList()


class ReferencedByBucket(BaseModel):
    """Inbound references to a table/view from other objects."""

    model_config = ConfigDict(extra="allow")

    procedures: ScopedRefList = ScopedRefList()
    views: ScopedRefList = ScopedRefList()
    functions: ScopedRefList = ScopedRefList()


# ── Statement entry ─────────────────────────────────────────────────────────


class StatementEntry(BaseModel):
    """A single resolved statement in a procedure catalog."""

    model_config = ConfigDict(extra="allow")

    action: str
    source: str
    sql: str
    type: str | None = None
    rationale: str | None = None


# ── Per-type scoping sections ───────────────────────────────────────────────


class TableScopingSection(BaseModel):
    """Writer-selection results from the analyzing-table skill."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    selected_writer: str | None = None
    selected_writer_rationale: str | None = None
    candidates: list[Any] | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class ViewScopingSection(BaseModel):
    """SQL analysis results from the analyzing-table/view skill."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    sql_elements: list[Any] | None = None
    call_tree: dict[str, Any] | None = None
    logic_summary: str | None = None
    rationale: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


# ── Per-type profile sections ───────────────────────────────────────────────


class TableProfileSection(BaseModel):
    """Profiling results for a table (classification, keys, PII, etc.)."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    writer: str = ""
    classification: Any | None = None
    primary_key: Any | None = None
    natural_key: Any | None = None
    watermark: Any | None = None
    foreign_keys: list[Any] = []
    pii_actions: list[Any] = []
    warnings: list[Any] = []
    errors: list[Any] = []


class ViewProfileSection(BaseModel):
    """Profiling results for a view (stg/mart classification)."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    classification: str = ""
    rationale: str = ""
    source: str = ""
    warnings: list[Any] = []
    errors: list[Any] = []


# ── Shared enriched-section models ──────────────────────────────────────────


class RefactorSection(BaseModel):
    """CTE restructuring results from the refactoring-sql skill."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    extracted_sql: str | None = None
    refactored_sql: str | None = None
    semantic_review: Any | None = None
    compare_sql: Any | None = None
    shared_sources: list[str] | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class TestGenSection(BaseModel):
    """Test generation summary from the test-harness write command."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    test_spec_path: str | None = None
    branches: int | None = None
    unit_tests: int | None = None
    coverage: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class GenerateSection(BaseModel):
    """dbt model generation summary from the migrate write-catalog command."""

    model_config = ConfigDict(extra="allow")

    status: str = ""
    model_path: str | None = None
    schema_yml: bool | None = None
    compiled: bool | None = None
    tests_passed: bool | None = None
    test_count: int | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


# ── Top-level catalog models ────────────────────────────────────────────────


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
    referenced_by: ReferencedByBucket | None = None
    scoping: TableScopingSection | None = None
    profile: TableProfileSection | None = None
    refactor: RefactorSection | None = None
    test_gen: TestGenSection | None = None
    generate: GenerateSection | None = None
    excluded: bool = False
    is_source: bool = False


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


class ViewCatalog(BaseModel):
    """Typed representation of a view catalog JSON file."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    references: ReferencesBucket | None = None
    referenced_by: ReferencedByBucket | None = None
    is_materialized_view: bool = False
    columns: list[Any] = []
    primary_keys: list[Any] = []
    unique_indexes: list[Any] = []
    scoping: ViewScopingSection | None = None
    profile: ViewProfileSection | None = None
    refactor: RefactorSection | None = None
    test_gen: TestGenSection | None = None
    generate: GenerateSection | None = None
    excluded: bool = False


class FunctionCatalog(BaseModel):
    """Typed representation of a function catalog JSON file."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    references: ReferencesBucket | None = None
    referenced_by: ReferencedByBucket | None = None
