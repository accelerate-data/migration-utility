"""Table name normalization utilities.

Normalizes fully-qualified names from T-SQL DDL into a canonical form:
bracket-free, lowercase, schema-defaulted.
"""

from __future__ import annotations

import re

_BRACKET_RE = re.compile(r"\[([^\]]+)\]")


def _strip_brackets(name: str) -> str:
    """Replace [identifier] tokens with bare identifier."""
    return _BRACKET_RE.sub(r"\1", name)


def normalize(name: str, default_schema: str = "dbo") -> str:
    """Return a canonical schema.table string.

    Steps:
    1. Strip square brackets.
    2. Lowercase.
    3. If no schema qualifier present, prepend default_schema.

    Examples:
        normalize("[silver].[DimProduct]")      -> "silver.dimproduct"
        normalize("DimProduct")                 -> "dbo.dimproduct"
        normalize("dbo.usp_Load", "dbo")        -> "dbo.usp_load"
    """
    cleaned = _strip_brackets(name).lower().strip()
    parts = [p.strip() for p in cleaned.split(".")]
    # Keep only the last two parts (schema + name) — discard db/server prefix
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return f"{default_schema}.{parts[-1]}"
