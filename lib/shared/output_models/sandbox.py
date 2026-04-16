"""Sandbox and test-harness output contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from shared.output_models.shared import OUTPUT_CONFIG


class ErrorEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    code: str
    message: str


class CompareSqlScenario(BaseModel):
    model_config = OUTPUT_CONFIG

    scenario_name: str
    status: Literal["ok", "error"] | None = None
    equivalent: bool | None = None
    a_count: int | None = None
    b_count: int | None = None
    a_minus_b: list[dict[str, Any]] | None = None
    b_minus_a: list[dict[str, Any]] | None = None
    errors: list[ErrorEntry] | None = None


class CompareSqlOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    schema_version: Literal["1.0"]
    sandbox_database: str
    total: int
    passed: int
    failed: int
    results: list[CompareSqlScenario]


class SandboxUpOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    sandbox_database: str
    status: Literal["ok", "partial", "error"]
    tables_cloned: list[str]
    views_cloned: list[str]
    procedures_cloned: list[str]
    errors: list[ErrorEntry]


class SandboxDownOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    sandbox_database: str
    status: Literal["ok", "error"]
    errors: list[ErrorEntry] = Field(default_factory=list)


class SandboxStatusOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    sandbox_database: str
    status: Literal["ok", "not_found", "error"]
    exists: bool
    has_content: bool | None = None
    tables_count: int | None = Field(default=None, ge=0)
    views_count: int | None = Field(default=None, ge=0)
    procedures_count: int | None = Field(default=None, ge=0)
    errors: list[ErrorEntry] = Field(default_factory=list)


class TestHarnessExecuteOutput(BaseModel):
    __test__ = False
    model_config = OUTPUT_CONFIG

    schema_version: Literal["1.0"] = "1.0"
    scenario_name: str
    status: Literal["ok", "error"]
    ground_truth_rows: list[dict[str, Any]]
    row_count: int = Field(ge=0)
    errors: list[ErrorEntry]


class ExecuteSpecResult(BaseModel):
    model_config = OUTPUT_CONFIG

    scenario_name: str
    status: Literal["ok", "error"]
    row_count: int = Field(ge=0)
    errors: list[ErrorEntry]


class ExecuteSpecOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    schema_version: Literal["1.0"] = "1.0"
    sandbox_database: str
    spec_path: str
    total: int = Field(ge=0)
    ok: int = Field(ge=0)
    failed: int = Field(ge=0)
    results: list[ExecuteSpecResult]
