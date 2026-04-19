from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from shared.catalog_model_support.base import _STRICT_CONFIG


class DiagnosticsEntry(BaseModel):
    """A single warning or error entry (mirrors common.json#/$defs/diagnostics_entry)."""

    model_config = _STRICT_CONFIG

    code: str
    message: str
    severity: Literal["error", "warning"]
    item_id: str | None = None
    field: str | None = None
    details: dict[str, Any] | None = None


class ProfileDiagnosticsEntry(BaseModel):
    """A profile warning or error entry persisted in catalog profile sections."""

    model_config = _STRICT_CONFIG

    code: str
    message: str
    severity: Literal["error", "warning", "medium"]
    item_id: str | None = None
    field: str | None = None
    details: dict[str, Any] | None = None


__all__ = ["DiagnosticsEntry", "ProfileDiagnosticsEntry"]
