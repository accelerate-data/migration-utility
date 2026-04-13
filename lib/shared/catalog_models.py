"""Pydantic v2 models for catalog objects.

These models type the JSON files produced by ``setup-ddl`` and enriched by
skills (scoping, profiling, refactoring, test generation, model generation).
All models use ``extra="forbid"`` so unexpected fields raise immediately —
the contract and code must stay in sync.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


# ── Shared config ───────────────────────────────────────────────────────────

_CATALOG_CONFIG = ConfigDict(extra="forbid", populate_by_name=True)
_STRICT_CONFIG = ConfigDict(extra="forbid")


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
    is_schema_bound: bool = False
    is_caller_dependent: bool = False
    is_ambiguous: bool = False
    columns: list[Any] = []


class ScopedRefList(BaseModel):
    """Scoped reference list: in_scope + out_of_scope."""

    model_config = _STRICT_CONFIG

    in_scope: list[RefEntry] = []
    out_of_scope: list[RefEntry] = []


class ReferencesBucket(BaseModel):
    """Outbound references from a proc/view/function to other objects."""

    model_config = _STRICT_CONFIG

    tables: ScopedRefList = ScopedRefList()
    views: ScopedRefList = ScopedRefList()
    functions: ScopedRefList = ScopedRefList()
    procedures: ScopedRefList = ScopedRefList()


class ReferencedByBucket(BaseModel):
    """Inbound references to a table/view from other objects."""

    model_config = _STRICT_CONFIG

    procedures: ScopedRefList = ScopedRefList()
    views: ScopedRefList = ScopedRefList()
    functions: ScopedRefList = ScopedRefList()


# ── Statement entry ─────────────────────────────────────────────────────────


class StatementEntry(BaseModel):
    """A single resolved statement in a procedure catalog."""

    model_config = _STRICT_CONFIG

    action: str
    source: str
    sql: str
    type: str | None = None
    rationale: str | None = None
    index: int | None = None


# ── Diagnostics entry ──────────────────────────────────────────────────────


class DiagnosticsEntry(BaseModel):
    """A single warning or error entry (mirrors common.json#/$defs/diagnostics_entry)."""

    model_config = _STRICT_CONFIG

    code: str
    message: str
    severity: Literal["error", "warning"]
    item_id: str | None = None
    field: str | None = None
    details: dict[str, Any] | None = None


# ── Per-type scoping sections ───────────────────────────────────────────────


class CandidateWriter(BaseModel):
    """A single candidate writer procedure discovered during table scoping."""

    model_config = _STRICT_CONFIG

    procedure_name: str
    rationale: str
    dependencies: dict[str, Any] | None = None


class TableScopingSection(BaseModel):
    """Writer-selection results from the analyzing-table skill."""

    model_config = _STRICT_CONFIG

    status: str = ""
    selected_writer: str | None = None
    selected_writer_rationale: str | None = None
    candidates: list[CandidateWriter] | None = None
    warnings: list[DiagnosticsEntry] = []
    errors: list[DiagnosticsEntry] = []


class SqlElement(BaseModel):
    """A single SQL structural element discovered during view scoping."""

    model_config = _STRICT_CONFIG

    type: str
    detail: str


class ViewScopingSection(BaseModel):
    """SQL analysis results from the analyzing-table/view skill."""

    model_config = _STRICT_CONFIG

    status: str = ""
    sql_elements: list[SqlElement] | None = None
    call_tree: dict[str, Any] | None = None
    logic_summary: str | None = None
    rationale: str | None = None
    warnings: list[DiagnosticsEntry] = []
    errors: list[DiagnosticsEntry] = []


# ── Scoping summary (batch rollup) ────────────────────────────────────────


class ScopingResultItem(BaseModel):
    """Per-item status in a scoping batch run."""

    model_config = _STRICT_CONFIG

    item_id: str
    status: Literal[
        "resolved", "ambiguous_multi_writer", "no_writer_found", "analyzed", "error",
    ]


class ScopingSummaryCounts(BaseModel):
    """Aggregate counts for a scoping batch run."""

    model_config = _STRICT_CONFIG

    total: int
    resolved: int
    ambiguous_multi_writer: int
    no_writer_found: int
    analyzed: int
    error: int


class ScopingSummary(BaseModel):
    """Batch rollup written to ``.migration-runs/summary.<epoch>.json`` by the scope command."""

    model_config = _STRICT_CONFIG

    schema_version: Literal["1.0"]
    run_id: str
    results: list[ScopingResultItem]
    summary: ScopingSummaryCounts


# ── Per-type profile sections ───────────────────────────────────────────────


class TableProfileSection(BaseModel):
    """Profiling results for a table (classification, keys, PII, etc.)."""

    model_config = _STRICT_CONFIG

    status: str = ""
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

    model_config = _STRICT_CONFIG

    status: str = ""
    classification: str = ""
    rationale: str = ""
    source: str = ""
    warnings: list[Any] = []
    errors: list[Any] = []


# ── Shared enriched-section models ──────────────────────────────────────────


class SemanticCheck(BaseModel):
    """A single semantic equivalence check (e.g. source_tables, joins)."""

    model_config = _STRICT_CONFIG

    passed: bool
    summary: str


class SemanticChecks(BaseModel):
    """The five semantic equivalence checks performed during refactoring review."""

    model_config = _STRICT_CONFIG

    source_tables: SemanticCheck
    output_columns: SemanticCheck
    joins: SemanticCheck
    filters: SemanticCheck
    aggregation_grain: SemanticCheck


class SemanticReview(BaseModel):
    """Structured semantic review from the refactoring-sql skill's review sub-agent."""

    model_config = _STRICT_CONFIG

    passed: bool
    checks: SemanticChecks
    issues: list[Any] = []


class CompareSqlSummary(BaseModel):
    """Persisted proof summary from executable compare-sql equivalence testing."""

    model_config = _STRICT_CONFIG

    required: bool
    executed: bool
    passed: bool
    scenarios_total: int
    scenarios_passed: int
    failed_scenarios: list[Any] = []


class RefactorSection(BaseModel):
    """CTE restructuring results from the refactoring-sql skill."""

    model_config = _STRICT_CONFIG

    status: str = ""
    extracted_sql: str | None = None
    refactored_sql: str | None = None
    semantic_review: SemanticReview | None = None
    compare_sql: CompareSqlSummary | None = None
    warnings: list[DiagnosticsEntry] = []
    errors: list[DiagnosticsEntry] = []


class TestGenSection(BaseModel):
    """Test generation summary from the test-harness write command."""

    model_config = _STRICT_CONFIG

    status: str = ""
    test_spec_path: str | None = None
    branches: int | None = None
    unit_tests: int | None = None
    coverage: str | None = None
    warnings: list[Any] = []
    errors: list[Any] = []


class GenerateSection(BaseModel):
    """dbt model generation summary from the migrate write-catalog command."""

    model_config = _STRICT_CONFIG

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
    statements: list[Any] = []
    referenced_by: ReferencedByBucket | None = None
    scoping: TableScopingSection | None = None
    profile: TableProfileSection | None = None
    refactor: RefactorSection | None = None
    test_gen: TestGenSection | None = None
    generate: GenerateSection | None = None
    excluded: bool = False
    is_source: bool = False
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
