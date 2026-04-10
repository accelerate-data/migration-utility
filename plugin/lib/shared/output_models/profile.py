"""Profile and view-profile output contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from shared.catalog_models import ReferencesBucket
from shared.output_models.discover import ColumnDef, SqlElement
from shared.output_models.dry_run import DiagnosticEntry
from shared.output_models.shared import OUTPUT_CONFIG


class PrimaryKeyConstraint(BaseModel):
    model_config = OUTPUT_CONFIG

    constraint_name: str
    columns: list[str]


class ForeignKeySignal(BaseModel):
    model_config = OUTPUT_CONFIG

    constraint_name: str
    columns: list[str]
    referenced_schema: str
    referenced_table: str
    referenced_columns: list[str]


class AutoIncrementSignal(BaseModel):
    model_config = OUTPUT_CONFIG

    column: str
    mechanism: str
    seed: int | None = None
    increment: int | None = None


class UniqueIndexSignal(BaseModel):
    model_config = OUTPUT_CONFIG

    index_name: str
    columns: list[str]


class ChangeCaptureSignal(BaseModel):
    model_config = OUTPUT_CONFIG

    enabled: bool
    mechanism: str


class SensitivitySignal(BaseModel):
    model_config = OUTPUT_CONFIG

    column: str
    label: str | None = None
    information_type: str | None = None


class CatalogSignals(BaseModel):
    model_config = OUTPUT_CONFIG

    primary_keys: list[PrimaryKeyConstraint] = Field(default_factory=list)
    foreign_keys: list[ForeignKeySignal] = Field(default_factory=list)
    auto_increment_columns: list[AutoIncrementSignal] = Field(default_factory=list)
    unique_indexes: list[UniqueIndexSignal] = Field(default_factory=list)
    change_capture: ChangeCaptureSignal | None = None
    sensitivity_classifications: list[SensitivitySignal] = Field(default_factory=list)


class ProfileColumnDef(BaseModel):
    model_config = OUTPUT_CONFIG

    name: str
    sql_type: str
    is_nullable: bool | None = None
    is_identity: bool | None = None
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None


class RelatedProcedure(BaseModel):
    model_config = OUTPUT_CONFIG

    procedure: str
    proc_body: str
    references: dict[str, Any] | None = None


class ProfileContext(BaseModel):
    model_config = OUTPUT_CONFIG

    table: str
    writer: str
    catalog_signals: CatalogSignals
    writer_references: ReferencesBucket
    proc_body: str
    columns: list[ProfileColumnDef]
    related_procedures: list[RelatedProcedure]
    writer_ddl_slice: str | None = None


ViewColumnDef = ColumnDef


class EnrichedInScopeRef(BaseModel):
    model_config = OUTPUT_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    object_type: Literal["table", "view", "function", "procedure"]
    is_selected: bool | None = None
    is_updated: bool | None = None
    is_insert_all: bool | None = None
    is_schema_bound: bool = False
    is_caller_dependent: bool = False
    is_ambiguous: bool = False
    detection: str | None = None
    columns: list[Any] | None = None


class OutOfScopeRef(BaseModel):
    model_config = OUTPUT_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""


class EnrichedScopedRefList(BaseModel):
    model_config = OUTPUT_CONFIG

    in_scope: list[EnrichedInScopeRef] = Field(default_factory=list)
    out_of_scope: list[OutOfScopeRef] = Field(default_factory=list)


class ViewReferences(BaseModel):
    model_config = OUTPUT_CONFIG

    tables: EnrichedScopedRefList = Field(default_factory=EnrichedScopedRefList)
    views: EnrichedScopedRefList = Field(default_factory=EnrichedScopedRefList)
    functions: EnrichedScopedRefList = Field(default_factory=EnrichedScopedRefList)


class ViewReferencedBy(BaseModel):
    model_config = OUTPUT_CONFIG

    procedures: EnrichedScopedRefList = Field(default_factory=EnrichedScopedRefList)
    views: EnrichedScopedRefList = Field(default_factory=EnrichedScopedRefList)
    functions: EnrichedScopedRefList = Field(default_factory=EnrichedScopedRefList)


class ViewProfileContext(BaseModel):
    model_config = OUTPUT_CONFIG

    view: str
    is_materialized_view: bool
    sql_elements: list[SqlElement] | None = None
    logic_summary: str | None = None
    columns: list[ViewColumnDef] = Field(default_factory=list)
    references: ViewReferences
    referenced_by: ViewReferencedBy
    warnings: list[DiagnosticEntry] = Field(default_factory=list)
    errors: list[DiagnosticEntry] = Field(default_factory=list)
