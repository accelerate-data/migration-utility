"""Test-spec and review output contracts."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from shared.catalog_models import DiagnosticsEntry
from shared.output_models.shared import OUTPUT_CONFIG


class BranchEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    id: str
    statement_index: int = Field(ge=0)
    description: str
    scenarios: list[str]


class GivenEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    table: str
    rows: list[dict[str, Any]]


class ExpectEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    rows: list[dict[str, Any]]


class UnitTestEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    name: str
    target_table: str | None = None
    procedure: str | None = None
    sql: str | None = None
    model: str | None = None
    given: list[GivenEntry] = Field(min_length=1)
    expect: ExpectEntry | None = None


class ValidationSection(BaseModel):
    model_config = OUTPUT_CONFIG

    passed: bool
    issues: list[DiagnosticsEntry] = Field(default_factory=list)


class TestSpec(BaseModel):
    __test__ = False
    model_config = OUTPUT_CONFIG

    item_id: str
    object_type: Literal["table", "view", "mv"] = "table"
    status: Literal["ok", "partial", "error"]
    coverage: Literal["complete", "partial"]
    branch_manifest: list[BranchEntry]
    unit_tests: list[UnitTestEntry]
    uncovered_branches: list[str]
    warnings: list[DiagnosticsEntry] = Field(default_factory=list)
    validation: ValidationSection
    errors: list[DiagnosticsEntry] = Field(default_factory=list)


class CommandSummary(BaseModel):
    model_config = OUTPUT_CONFIG

    total: int = Field(ge=0)
    ok: int = Field(ge=0)
    partial: int = Field(ge=0)
    error: int = Field(ge=0)


class TestSpecOutput(BaseModel):
    __test__ = False
    model_config = OUTPUT_CONFIG

    schema_version: Literal["1.0"]
    results: list[TestSpec]
    summary: CommandSummary


class ReviewerBranchEntry(BaseModel):
    model_config = OUTPUT_CONFIG

    id: str
    description: str
    covered: bool
    covering_scenarios: list[str]


class UncoveredBranch(BaseModel):
    model_config = OUTPUT_CONFIG

    id: str
    description: str


class UntestableBranch(BaseModel):
    model_config = OUTPUT_CONFIG

    id: str
    description: str
    rationale: str


class CoverageSection(BaseModel):
    model_config = OUTPUT_CONFIG

    total_branches: int = Field(ge=0)
    covered_branches: int = Field(ge=0)
    untestable_branches: int = Field(default=0, ge=0)
    score: Literal["complete", "partial"]
    uncovered: list[UncoveredBranch] = Field(default_factory=list)
    untestable: list[UntestableBranch] = Field(default_factory=list)


class QualityIssue(BaseModel):
    model_config = OUTPUT_CONFIG

    scenario: str
    issue: str
    severity: Literal["warning", "error"]


class GeneratorFeedback(BaseModel):
    model_config = OUTPUT_CONFIG

    uncovered_branches: list[str]
    quality_fixes: list[str]


class TestReviewOutput(BaseModel):
    __test__ = False
    model_config = OUTPUT_CONFIG

    item_id: str
    status: Literal["approved", "approved_with_warnings", "revision_requested", "error"]
    reviewer_branch_manifest: list[ReviewerBranchEntry]
    coverage: CoverageSection
    quality_issues: list[QualityIssue]
    feedback_for_generator: GeneratorFeedback
    warnings: list[DiagnosticsEntry] = Field(default_factory=list)
    errors: list[DiagnosticsEntry] = Field(default_factory=list)
