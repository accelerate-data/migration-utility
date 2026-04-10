"""migrate-util and batch-plan output contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel
from pydantic import Field

from shared.output_models.shared import OUTPUT_CONFIG


class GuardResult(BaseModel):
    model_config = OUTPUT_CONFIG

    check: str
    passed: bool
    code: str | None = None
    message: str | None = None


class DryRunOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    table: str | None = None
    stage: Literal["scope", "profile", "test-gen", "refactor", "migrate", "generate"] | None = None
    object_type: Literal["table", "view", "mv"] | None = None
    guards_passed: bool | None = None
    not_applicable: bool | None = None
    guard_results: list[GuardResult] | None = None
    ready: bool | None = None
    reason: str | None = None
    code: str | None = None


class StageStatuses(BaseModel):
    model_config = OUTPUT_CONFIG

    scope: str | None = None
    profile: str | None = None
    test_gen: str | None = None
    refactor: str | None = None
    generate: str | None = None


class ObjectStatus(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    type: Literal["table", "view", "mv"]
    stages: StageStatuses


class StatusSummary(BaseModel):
    model_config = OUTPUT_CONFIG

    total: int
    by_stage: dict[str, dict[str, int]]


class StatusOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str | None = None
    type: Literal["table", "view", "mv"] | None = None
    stages: StageStatuses | None = None
    objects: list[ObjectStatus] = Field(default_factory=list)
    summary: StatusSummary | None = None


class DiagnosticEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    code: str | None = None
    message: str | None = None
    severity: Literal["warning", "error"] | None = None


class ObjectNode(BaseModel):
    model_config = OUTPUT_CONFIG

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
    model_config = OUTPUT_CONFIG

    batch: int
    objects: list[str]


class NaObject(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    reason: str


class ExcludedObject(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    reason: str


class SourceTable(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str


class SourcePending(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    reason: str


class CircularRef(BaseModel):
    model_config = OUTPUT_CONFIG

    object: str
    cycle: list[str]


class CatalogDiagnosticEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    code: str
    severity: Literal["warning", "error"]
    message: str


class CatalogDiagnostics(BaseModel):
    model_config = OUTPUT_CONFIG

    warnings: list[CatalogDiagnosticEntry]
    errors: list[CatalogDiagnosticEntry]


class BatchSummary(BaseModel):
    model_config = OUTPUT_CONFIG

    total_objects: int
    batched_objects: int
    source_tables: int
    source_pending: int
    n_a_objects: int
    excluded_objects: int
    circular_refs: int


class BatchPlanOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    objects: list[ObjectNode]
    migrate_batches: list[MigrateBatch]
    source_tables: list[SourceTable]
    source_pending: list[SourcePending]
    n_a_objects: list[NaObject]
    excluded_objects: list[ExcludedObject]
    circular_refs: list[CircularRef]
    catalog_diagnostics: CatalogDiagnostics
    summary: BatchSummary


class ExcludeOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    marked: list[str]
    not_found: list[str]


class SyncExcludedWarningsOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    updated: list[str]
    cleared: list[str]
