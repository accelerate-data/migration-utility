"""Model generation and review output contracts."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from shared.catalog_models import DiagnosticsEntry
from shared.output_models.shared import OUTPUT_CONFIG


class GeneratorItem(BaseModel):
    model_config = OUTPUT_CONFIG

    item_id: str
    selected_writer: str | None = None


class ModelGeneratorInput(BaseModel):
    model_config = OUTPUT_CONFIG

    schema_version: Literal["2.0"]
    run_id: str
    items: list[GeneratorItem] = Field(min_length=1)


class ArtifactPaths(BaseModel):
    model_config = OUTPUT_CONFIG

    model_sql: str
    model_yaml: str


class FeedbackItem(BaseModel):
    model_config = OUTPUT_CONFIG

    code: str
    message: str
    severity: Literal["error", "warning", "info"]
    ack_required: bool


class ModelGenerationHandoff(BaseModel):
    model_config = OUTPUT_CONFIG

    artifact_paths: ArtifactPaths | None = None
    revision_feedback: list[FeedbackItem] | None = None


class GeneratedModelInfo(BaseModel):
    model_config = OUTPUT_CONFIG

    materialized: str
    uses_watermark: bool


class GeneratedYamlInfo(BaseModel):
    model_config = OUTPUT_CONFIG

    has_model_description: bool
    schema_tests_rendered: list[str]
    has_unit_tests: bool


class GeneratedInfo(BaseModel):
    model_config = OUTPUT_CONFIG

    model_sql: GeneratedModelInfo
    model_yaml: GeneratedYamlInfo


class ExecutionInfo(BaseModel):
    model_config = OUTPUT_CONFIG

    dbt_compile_passed: bool
    dbt_test_passed: bool
    self_correction_iterations: int = Field(ge=0)
    dbt_errors: list[str]


class ReviewInfo(BaseModel):
    model_config = OUTPUT_CONFIG

    iterations: int = Field(ge=0)
    verdict: Literal["approved", "approved_with_warnings"]


class ModelGenerationItemOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    table_ref: str
    model_name: str
    artifact_paths: ArtifactPaths
    generated: GeneratedInfo
    execution: ExecutionInfo
    review: ReviewInfo
    warnings: list[DiagnosticsEntry] = Field(default_factory=list)
    errors: list[DiagnosticsEntry] = Field(default_factory=list)


class ModelGenerationOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    item_id: str
    status: Literal["ok", "partial", "error"]
    output: ModelGenerationItemOutput


class CheckResult(BaseModel):
    model_config = OUTPUT_CONFIG

    passed: bool
    issues: list[DiagnosticsEntry] = Field(default_factory=list)


class ReviewChecks(BaseModel):
    model_config = OUTPUT_CONFIG

    standards: CheckResult
    correctness: CheckResult


class ModelReviewOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    item_id: str
    status: Literal["approved", "revision_requested", "approved_with_warnings", "error"]
    checks: ReviewChecks
    feedback_for_model_generator: list[FeedbackItem] = Field(default_factory=list)
    acknowledgements: dict[str, str] | None = None
    warnings: list[DiagnosticsEntry] = Field(default_factory=list)
    errors: list[DiagnosticsEntry] = Field(default_factory=list)
