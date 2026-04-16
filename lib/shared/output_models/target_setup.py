"""target-setup output contracts."""

from __future__ import annotations

from pydantic import BaseModel, Field

from shared.output_models.shared import OUTPUT_CONFIG


class SetupTargetOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    files: list[str]
    sources_path: str | None = None
    target_source_schema: str
    created_tables: list[str]
    existing_tables: list[str]
    desired_tables: list[str]
    seed_files: list[str] = Field(default_factory=list)
    seed_row_counts: dict[str, int] = Field(default_factory=dict)
    dbt_seed_ran: bool = False
    dbt_seed_command: list[str] = Field(default_factory=list)
