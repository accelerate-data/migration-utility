"""cli_utils.py — Shared CLI output helpers."""

from __future__ import annotations

import json
from typing import Any


def emit(data: Any) -> None:
    """Write data as JSON to stdout."""
    print(json.dumps(data, ensure_ascii=False))
