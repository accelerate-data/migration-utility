from __future__ import annotations

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG


class StatementEntry(BaseModel):
    """A single resolved statement in a procedure catalog."""

    model_config = _STRICT_CONFIG

    action: str
    source: str
    sql: str
    type: str | None = None
    rationale: str | None = None
    index: int | None = None


__all__ = ["StatementEntry"]
