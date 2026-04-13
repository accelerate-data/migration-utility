"""generate-sources output contracts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from shared.output_models.shared import OUTPUT_CONFIG


class GenerateSourcesOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    sources: dict[str, Any] | None = None
    included: list[str]
    excluded: list[str]
    unconfirmed: list[str]
    incomplete: list[str]
    path: str | None = None
    error: str | None = None
    message: str | None = None
