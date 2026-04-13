"""target-setup output contracts."""

from __future__ import annotations

from pydantic import BaseModel

from shared.output_models.shared import OUTPUT_CONFIG


class SetupTargetOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    files: list[str]
    sources_path: str | None = None
    target_source_schema: str
    created_tables: list[str]
    existing_tables: list[str]
    desired_tables: list[str]
