"""Shared Pydantic config for catalog contract models."""

from __future__ import annotations

from pydantic import ConfigDict

_CATALOG_CONFIG = ConfigDict(extra="forbid", populate_by_name=True)
_STRICT_CONFIG = ConfigDict(extra="forbid")

__all__ = ["_CATALOG_CONFIG", "_STRICT_CONFIG"]
