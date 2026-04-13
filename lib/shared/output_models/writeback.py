"""Catalog write-back output contracts."""

from typing import Literal

from pydantic import BaseModel

from shared.output_models.shared import OUTPUT_CONFIG


class WriteSourceOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    written: str
    is_source: bool
    status: Literal["ok"]


class WriteSliceOutput(BaseModel):
    model_config = OUTPUT_CONFIG

    written: str
    status: Literal["ok"]
