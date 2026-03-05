from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, field_validator


class ValidationResult(BaseModel):
    passed: bool = True
    issues: list[str] = []


class CandidateWriter(BaseModel):
    procedure_name: str
    write_type: Literal["direct", "indirect", "read_only"]
    call_path: list[str]
    rationale: str
    confidence: float

    @field_validator("confidence")
    @classmethod
    def confidence_in_range(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"confidence must be in [0, 1], got {v}")
        return v


class ScopingResult(BaseModel):
    item_id: str
    status: Literal[
        "resolved",
        "ambiguous_multi_writer",
        "partial",
        "no_writer_found",
        "error",
    ]
    selected_writer: Optional[str] = None
    candidate_writers: list[CandidateWriter] = []
    warnings: list[str] = []
    validation: ValidationResult = ValidationResult()
    errors: list[str] = []


class Summary(BaseModel):
    total: int = 0
    resolved: int = 0
    ambiguous_multi_writer: int = 0
    no_writer_found: int = 0
    partial: int = 0
    error: int = 0


class CandidateWritersOutput(BaseModel):
    schema_version: str = "1.0"
    batch_id: str
    results: list[ScopingResult]
    summary: Summary
