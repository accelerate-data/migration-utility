"""Migration command output contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from shared.catalog_models import DiagnosticsEntry
from shared.output_models.shared import OUTPUT_CONFIG


class MigrateContextOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    table: str
    writer: str
    needs_llm: bool
    profile: dict[str, Any]
    materialization: Literal["table", "incremental", "snapshot", "view"]
    statements: list[dict[str, Any]]
    proc_body: str
    columns: list[Any]
    source_tables: list[str]
    source_columns: dict[str, list[Any]]
    schema_tests: dict[str, Any]
    refactored_sql: str | None = None
    selected_writer_ddl_slice: str | None = None


class MigrateWriteOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    written: list[str]
    status: Literal["ok"]


class MigrateWriteGenerateOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    ok: bool
    table: str
    catalog_path: str
    status: Literal["ok", "partial", "error"]


class RenderUnitTestsOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    tests_rendered: int = Field(ge=0)
    model_name: str
    warnings: list[DiagnosticsEntry] = Field(default_factory=list)
    errors: list[DiagnosticsEntry] = Field(default_factory=list)
