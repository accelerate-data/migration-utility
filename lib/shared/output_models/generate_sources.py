"""generate-sources output contracts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic import Field

from shared.output_models.shared import OUTPUT_CONFIG


class GenerateSourcesOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    sources: dict[str, Any] | None = None
    included: list[str]
    excluded: list[str]
    unconfirmed: list[str]
    incomplete: list[str]
    generated_model_names: list[str] = Field(default_factory=list)
    path: str | None = None
    written_paths: list[str] = Field(default_factory=list)
    error: str | None = None
    message: str | None = None
