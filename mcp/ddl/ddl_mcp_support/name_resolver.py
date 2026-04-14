"""Table name normalization utilities."""
from __future__ import annotations

import re

_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
_DOUBLEQUOTE_RE = re.compile(r'"([^"]+)"')


def _strip_quotes(name: str) -> str:
    name = _BRACKET_RE.sub(r"\1", name)
    return _DOUBLEQUOTE_RE.sub(r"\1", name)


def normalize(name: str, default_schema: str = "dbo") -> str:
    cleaned = _strip_quotes(name).lower().strip()
    parts = [p.strip() for p in cleaned.split(".")]
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return f"{default_schema}.{parts[-1]}"
