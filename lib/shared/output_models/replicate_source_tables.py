"""replicate-source-tables output contracts."""

from __future__ import annotations

from pydantic import BaseModel, Field

from shared.output_models.shared import OUTPUT_CONFIG


class ReplicateTableResult(BaseModel):
    model_config = OUTPUT_CONFIG

    fqn: str
    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    columns: list[str] = Field(default_factory=list)
    predicate: str | None = None
    status: str
    rows_copied: int = 0
    error: str | None = None


class ReplicateSourceTablesOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    status: str
    dry_run: bool
    limit: int
    tables: list[ReplicateTableResult]
