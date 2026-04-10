"""Pydantic v2 models for CLI command output contracts.

These models enforce the shape of JSON emitted by ``run_*`` functions in
``discover.py``, ``dry_run.py``, ``batch_plan.py``, and ``profile.py``.
All models use ``extra="forbid"`` so unexpected fields raise immediately —
the contract and code must stay in sync.

Replaces the JSON schemas that previously lived in ``schemas/``.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from shared.catalog_models import ReferencesBucket


# ── Shared config ───────────────────────────────────────────────────────────

_OUTPUT_CONFIG = ConfigDict(extra="forbid")


# ═══════════════════════════════════════════════════════════════════════════
# discover list
# ═══════════════════════════════════════════════════════════════════════════


class DiscoverListOutput(BaseModel):
    """Output of ``discover list``."""

    model_config = _OUTPUT_CONFIG

    objects: list[str]


# ═══════════════════════════════════════════════════════════════════════════
# discover show
# ═══════════════════════════════════════════════════════════════════════════


class ColumnDef(BaseModel):
    model_config = _OUTPUT_CONFIG

    name: str
    sql_type: str
    is_nullable: bool | None = None
    is_identity: bool | None = None


class ParamDef(BaseModel):
    model_config = _OUTPUT_CONFIG

    name: str
    sql_type: str
    is_output: bool | None = None
    has_default: bool | None = None


class ProcRefs(BaseModel):
    model_config = _OUTPUT_CONFIG

    reads_from: list[str]
    writes_to: list[str]
    write_operations: dict[str, list[str]]
    uses_functions: list[str]


class BasicRefs(BaseModel):
    model_config = _OUTPUT_CONFIG

    reads_from: list[str]
    writes_to: list[str]


class StatementEntry(BaseModel):
    model_config = _OUTPUT_CONFIG

    type: str
    action: Literal["migrate", "skip", "needs_llm"]
    sql: str


class SqlElement(BaseModel):
    model_config = _OUTPUT_CONFIG

    type: str
    detail: str | None = None


class AnalysisError(BaseModel):
    model_config = _OUTPUT_CONFIG

    code: str
    severity: Literal["error", "warning"]
    message: str


class DiscoverShowOutput(BaseModel):
    """Output of ``discover show``."""

    model_config = _OUTPUT_CONFIG

    name: str
    type: Literal["table", "procedure", "view", "function"]
    raw_ddl: str
    columns: list[ColumnDef] = Field(default_factory=list)
    params: list[ParamDef] = Field(default_factory=list)
    refs: ProcRefs | BasicRefs | None = None
    routing_reasons: list[str] = Field(default_factory=list)
    statements: list[StatementEntry] | None = None
    needs_llm: bool | None = None
    parse_error: str | None = None
    sql_elements: list[SqlElement] | None = None
    errors: list[AnalysisError] | None = None


# ═══════════════════════════════════════════════════════════════════════════
# discover refs
# ═══════════════════════════════════════════════════════════════════════════


class WriterEntry(BaseModel):
    model_config = _OUTPUT_CONFIG

    procedure: str
    write_type: Literal["direct"] = "direct"
    is_updated: Literal[True] = True
    is_selected: bool | None = None
    is_insert_all: bool | None = None


class DiscoverRefsOutput(BaseModel):
    """Output of ``discover refs``.

    Two shapes: full result (name + readers + writers) or error-only
    (procedure target or missing catalog).
    """

    model_config = _OUTPUT_CONFIG

    name: str | None = None
    type: Literal["table", "view", "mv", "function", "object"] | None = None
    source: Literal["catalog"] | None = None
    error: str | None = None
    readers: list[str] = Field(default_factory=list)
    writers: list[WriterEntry] = Field(default_factory=list)


# ═══════════════════════════════════════════════════════════════════════════
# dry-run / ready
# ═══════════════════════════════════════════════════════════════════════════


class GuardResult(BaseModel):
    model_config = _OUTPUT_CONFIG

    check: str
    passed: bool
    code: str | None = None
    message: str | None = None


class DryRunOutput(BaseModel):
    """Output of ``migrate-util ready`` for a single (table, stage) pair."""

    model_config = _OUTPUT_CONFIG

    table: str | None = None
    stage: Literal["scope", "profile", "test-gen", "refactor", "migrate", "generate"] | None = None
    object_type: Literal["table", "view", "mv"] | None = None
    guards_passed: bool | None = None
    not_applicable: bool | None = None
    guard_results: list[GuardResult] | None = None
    ready: bool | None = None
    reason: str | None = None
    code: str | None = None


# ═══════════════════════════════════════════════════════════════════════════
# status
# ═══════════════════════════════════════════════════════════════════════════


class StageStatuses(BaseModel):
    model_config = _OUTPUT_CONFIG

    scope: str | None = None
    profile: str | None = None
    test_gen: str | None = None
    refactor: str | None = None
    generate: str | None = None


class ObjectStatus(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    stages: StageStatuses


class StatusSummary(BaseModel):
    model_config = _OUTPUT_CONFIG

    total: int
    by_stage: dict[str, dict[str, int]]


class StatusOutput(BaseModel):
    """Output of ``migrate-util status``.

    Single-object mode: ``fqn``, ``type``, ``stages`` populated.
    Batch mode: ``objects`` + ``summary`` populated.
    """

    model_config = _OUTPUT_CONFIG

    fqn: str | None = None
    type: Literal["table", "view", "mv"] | None = None
    stages: StageStatuses | None = None
    objects: list[ObjectStatus] = Field(default_factory=list)
    summary: StatusSummary | None = None


# ═══════════════════════════════════════════════════════════════════════════
# batch-plan
# ═══════════════════════════════════════════════════════════════════════════


class DiagnosticEntry(BaseModel):
    model_config = _OUTPUT_CONFIG

    code: str | None = None
    message: str | None = None
    severity: Literal["warning", "error"] | None = None


class ObjectNode(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    pipeline_status: Literal[
        "scope_needed",
        "profile_needed",
        "test_gen_needed",
        "refactor_needed",
        "migrate_needed",
        "complete",
        "n_a",
    ]
    has_dbt_model: bool
    direct_deps: list[str]
    blocking_deps: list[str]
    diagnostics: list[DiagnosticEntry]
    diagnostic_stage_flags: dict[str, Literal["error", "warning"]]


class MigrateBatch(BaseModel):
    model_config = _OUTPUT_CONFIG

    batch: int
    objects: list[ObjectNode]


class NaObject(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    reason: str


class ExcludedObject(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    note: str


class SourceTable(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    reason: str


class SourcePending(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]


class CircularRef(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    note: str


class CatalogDiagnosticEntry(BaseModel):
    model_config = _OUTPUT_CONFIG

    fqn: str
    object_type: Literal["table", "view", "mv"]
    code: str | None = None
    message: str | None = None
    severity: Literal["warning", "error"]
    item_id: str | None = None
    field: str | None = None
    details: dict[str, Any] | None = None


class CatalogDiagnostics(BaseModel):
    model_config = _OUTPUT_CONFIG

    total_errors: int
    total_warnings: int
    errors: list[CatalogDiagnosticEntry]
    warnings: list[CatalogDiagnosticEntry]


class BatchSummary(BaseModel):
    model_config = _OUTPUT_CONFIG

    total_objects: int
    tables: int
    views: int
    mvs: int
    writerless_tables: int
    excluded_count: int
    source_tables: int
    source_pending: int


class BatchPlanOutput(BaseModel):
    """Output of ``migrate-util batch-plan``."""

    model_config = _OUTPUT_CONFIG

    summary: BatchSummary
    scope_phase: list[ObjectNode]
    profile_phase: list[ObjectNode]
    migrate_batches: list[MigrateBatch]
    completed_objects: list[ObjectNode]
    n_a_objects: list[NaObject]
    excluded_objects: list[ExcludedObject]
    source_tables: list[SourceTable]
    source_pending: list[SourcePending]
    circular_refs: list[CircularRef]
    catalog_diagnostics: CatalogDiagnostics


# ═══════════════════════════════════════════════════════════════════════════
# exclude
# ═══════════════════════════════════════════════════════════════════════════


class ExcludeOutput(BaseModel):
    """Output of ``migrate-util exclude``."""

    model_config = _OUTPUT_CONFIG

    marked: list[str]
    not_found: list[str]


# ═══════════════════════════════════════════════════════════════════════════
# profile context
# ═══════════════════════════════════════════════════════════════════════════


class PrimaryKeyConstraint(BaseModel):
    model_config = _OUTPUT_CONFIG

    constraint_name: str | None = None
    columns: list[str] = Field(default_factory=list)
    type: Literal["PRIMARY KEY", "UNIQUE"] | None = None


class ForeignKeySignal(BaseModel):
    model_config = _OUTPUT_CONFIG

    constraint_name: str | None = None
    columns: list[str] = Field(default_factory=list)
    referenced_schema: str | None = None
    referenced_table: str | None = None
    referenced_columns: list[str] = Field(default_factory=list)


class AutoIncrementSignal(BaseModel):
    model_config = _OUTPUT_CONFIG

    column: str
    mechanism: Literal[
        "identity", "autoincrement", "sequence", "generated_always",
    ] | None = None
    seed: int | None = None
    increment: int | None = None


class UniqueIndexSignal(BaseModel):
    model_config = _OUTPUT_CONFIG

    index_name: str | None = None
    columns: list[str] = Field(default_factory=list)


class ChangeCaptureSignal(BaseModel):
    model_config = _OUTPUT_CONFIG

    enabled: bool
    mechanism: Literal[
        "cdc", "change_tracking", "stream", "change_data_feed",
    ] | None = None


class SensitivitySignal(BaseModel):
    model_config = _OUTPUT_CONFIG

    column: str
    label: str | None = None
    information_type: str | None = None


class CatalogSignals(BaseModel):
    model_config = _OUTPUT_CONFIG

    primary_keys: list[PrimaryKeyConstraint] = Field(default_factory=list)
    foreign_keys: list[ForeignKeySignal] = Field(default_factory=list)
    auto_increment_columns: list[AutoIncrementSignal] = Field(default_factory=list)
    unique_indexes: list[UniqueIndexSignal] = Field(default_factory=list)
    change_capture: ChangeCaptureSignal | None = None
    sensitivity_classifications: list[SensitivitySignal] = Field(default_factory=list)


class ProfileColumnDef(BaseModel):
    model_config = _OUTPUT_CONFIG

    name: str
    sql_type: str
    is_nullable: bool | None = None
    is_identity: bool | None = None
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None


class RelatedProcedure(BaseModel):
    model_config = _OUTPUT_CONFIG

    procedure: str
    proc_body: str
    references: dict[str, Any] | None = None


class ProfileContext(BaseModel):
    """Output of ``profile context``.

    Assembles all deterministic context needed for LLM profiling reasoning.
    """

    model_config = _OUTPUT_CONFIG

    table: str
    writer: str
    catalog_signals: CatalogSignals
    writer_references: ReferencesBucket
    proc_body: str
    columns: list[ProfileColumnDef]
    related_procedures: list[RelatedProcedure]
    writer_ddl_slice: str | None = None


# ═══════════════════════════════════════════════════════════════════════════
# view profile context
# ═══════════════════════════════════════════════════════════════════════════


class ViewColumnDef(BaseModel):
    model_config = _OUTPUT_CONFIG

    name: str
    sql_type: str
    is_nullable: bool | None = None
    is_identity: bool | None = None


class EnrichedInScopeRef(BaseModel):
    """In-scope reference with object_type added for view context."""

    model_config = _OUTPUT_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    object_type: Literal["table", "view", "function", "procedure"]
    is_selected: bool | None = None
    is_updated: bool | None = None
    is_insert_all: bool | None = None
    detection: str | None = None
    columns: list[Any] | None = None


class OutOfScopeRef(BaseModel):
    """Out-of-scope reference entry."""

    model_config = _OUTPUT_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""


class EnrichedScopedRefList(BaseModel):
    model_config = _OUTPUT_CONFIG

    in_scope: list[EnrichedInScopeRef] = Field(default_factory=list)
    out_of_scope: list[OutOfScopeRef] = Field(default_factory=list)


class ViewReferences(BaseModel):
    model_config = _OUTPUT_CONFIG

    tables: EnrichedScopedRefList = EnrichedScopedRefList()
    views: EnrichedScopedRefList = EnrichedScopedRefList()
    functions: EnrichedScopedRefList = EnrichedScopedRefList()


class ViewReferencedBy(BaseModel):
    model_config = _OUTPUT_CONFIG

    procedures: EnrichedScopedRefList = EnrichedScopedRefList()
    views: EnrichedScopedRefList = EnrichedScopedRefList()
    functions: EnrichedScopedRefList = EnrichedScopedRefList()


class ViewProfileContext(BaseModel):
    """Output of ``profile view-context``.

    View catalog data with object_type added to each in_scope entry.
    LLM uses sql_elements + logic_summary for classification.
    """

    model_config = _OUTPUT_CONFIG

    view: str
    is_materialized_view: bool
    sql_elements: list[SqlElement] | None = None
    logic_summary: str | None = None
    columns: list[ViewColumnDef] = Field(default_factory=list)
    references: ViewReferences
    referenced_by: ViewReferencedBy
    warnings: list[DiagnosticEntry] = Field(default_factory=list)
    errors: list[DiagnosticEntry] = Field(default_factory=list)
