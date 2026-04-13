"""cli_utils.py — Shared CLI output helpers."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel


def emit(data: Any) -> None:
    """Write data as JSON to stdout."""
    if isinstance(data, BaseModel):
        data = data.model_dump(mode="json", exclude_none=True)
    print(json.dumps(data, ensure_ascii=False))
