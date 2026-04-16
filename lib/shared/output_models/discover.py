"""Discover command output contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from shared.output_models.shared import OUTPUT_CONFIG


class DiscoverListOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    objects: list[str]


class ColumnDef(BaseModel):
    model_config = OUTPUT_CONFIG

    name: str
    sql_type: str
    is_nullable: bool | None = None
    is_identity: bool | None = None


class ParamDef(BaseModel):
    model_config = OUTPUT_CONFIG

    name: str
    sql_type: str
    is_output: bool | None = None
    has_default: bool | None = None


class ProcRefs(BaseModel):
    model_config = OUTPUT_CONFIG

    reads_from: list[str]
    writes_to: list[str]
    write_operations: dict[str, list[str]]
    uses_functions: list[str]


class BasicRefs(BaseModel):
    model_config = OUTPUT_CONFIG

    reads_from: list[str]
    writes_to: list[str]


class StatementEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    type: str
    action: Literal["migrate", "skip", "needs_llm"]
    sql: str


class SqlElement(BaseModel):
    model_config = OUTPUT_CONFIG

    type: str
    detail: str | None = None


class AnalysisError(BaseModel):
    model_config = OUTPUT_CONFIG

    code: str
    severity: Literal["error", "warning"]
    message: str


class DiscoverShowOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    name: str
    type: Literal["table", "procedure", "view", "function"]
    raw_ddl: str
    is_source: bool | None = None
    is_seed: bool | None = None
    columns: list[ColumnDef] = Field(default_factory=list)
    params: list[ParamDef] = Field(default_factory=list)
    refs: ProcRefs | BasicRefs | None = None
    routing_reasons: list[str] = Field(default_factory=list)
    statements: list[StatementEntry] | None = None
    needs_llm: bool | None = None
    parse_error: str | None = None
    sql_elements: list[SqlElement] | None = None
    errors: list[AnalysisError] | None = None


class WriterEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    procedure: str
    write_type: Literal["direct"] = "direct"
    is_updated: Literal[True] = True
    is_selected: bool | None = None
    is_insert_all: bool | None = None


class DiscoverRefsOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    name: str | None = None
    type: Literal["table", "view", "mv", "function", "object"] | None = None
    source: Literal["catalog"] | None = None
    error: str | None = None
    readers: list[str] = Field(default_factory=list)
    writers: list[WriterEntry] = Field(default_factory=list)
