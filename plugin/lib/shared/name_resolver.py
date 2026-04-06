"""Table name normalization utilities.

Normalizes fully-qualified names from T-SQL DDL into a canonical form:
bracket-free, quote-free, lowercase, schema-defaulted.
"""

from __future__ import annotations

import re

_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
_DOUBLEQUOTE_RE = re.compile(r'"([^"]+)"')


def _strip_quotes(name: str) -> str:
    """Replace [identifier] and "identifier" tokens with bare identifier."""
    name = _BRACKET_RE.sub(r"\1", name)
    return _DOUBLEQUOTE_RE.sub(r"\1", name)


def normalize(name: str, default_schema: str = "dbo") -> str:
    """Return a canonical schema.table string.

    Steps:
    1. Strip square brackets and double-quotes.
    2. Lowercase.
    3. If no schema qualifier present, prepend default_schema.

    Examples:
        normalize("[silver].[DimProduct]")      -> "silver.dimproduct"
        normalize('"SH"."GET_PRODUCT_COUNT"')   -> "sh.get_product_count"
        normalize("DimProduct")                 -> "dbo.dimproduct"
        normalize("dbo.usp_Load", "dbo")        -> "dbo.usp_load"
    """
    cleaned = _strip_quotes(name).lower().strip()
    parts = [p.strip() for p in cleaned.split(".")]
    # Keep only the last two parts (schema + name) — discard db/server prefix
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return f"{default_schema}.{parts[-1]}"


def fqn_parts(fqn: str) -> tuple[str, str]:
    """Split a normalized FQN into (schema, name).

    >>> fqn_parts("silver.dimcustomer")
    ('silver', 'dimcustomer')
    >>> fqn_parts("dimcustomer")
    ('dbo', 'dimcustomer')
    """
    parts = fqn.split(".")
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return "dbo", parts[-1]
