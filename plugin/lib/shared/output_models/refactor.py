"""Refactor command output contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from shared.output_models.shared import OUTPUT_CONFIG


class RefactorContextOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    table: str
    object_type: Literal["table", "view", "mv"] | None = None
    writer: str | None = None
    proc_body: str | None = None
    view_sql: str | None = None
    profile: dict[str, Any]
    statements: list[dict[str, Any]] | None = None
    columns: list[Any]
    source_tables: list[str]
    source_columns: dict[str, list[Any]] | None = None
    test_spec: dict[str, Any] | None = None
    sandbox: dict[str, Any] | None = None
    writer_ddl_slice: str | None = None


class RefactorWriteOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    ok: bool
    table: str
    status: str | None = None
    writer: str | None = None
    catalog_path: str | None = None
    object_type: str | None = None
    error: str | None = None
