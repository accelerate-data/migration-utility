from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from shared.catalog_model_support.base import _CATALOG_CONFIG, _STRICT_CONFIG


class RefEntry(BaseModel):
    """A single reference entry inside in_scope / out_of_scope lists."""

    model_config = _CATALOG_CONFIG

    object_schema: str = Field(default="", alias="schema")
    name: str = ""
    is_selected: bool = False
    is_updated: bool = False
    is_insert_all: bool = False
    detection: str | None = None
    is_schema_bound: bool = False
    is_caller_dependent: bool = False
    is_ambiguous: bool = False
    columns: list[Any] = []


class ScopedRefList(BaseModel):
    """Scoped reference list: in_scope + out_of_scope."""

    model_config = _STRICT_CONFIG

    in_scope: list[RefEntry] = []
    out_of_scope: list[RefEntry] = []


class ReferencesBucket(BaseModel):
    """Outbound references from a proc/view/function to other objects."""

    model_config = _STRICT_CONFIG

    tables: ScopedRefList = ScopedRefList()
    views: ScopedRefList = ScopedRefList()
    functions: ScopedRefList = ScopedRefList()
    procedures: ScopedRefList = ScopedRefList()


class ReferencedByBucket(BaseModel):
    """Inbound references to a table/view from other objects."""

    model_config = _STRICT_CONFIG

    procedures: ScopedRefList = ScopedRefList()
    views: ScopedRefList = ScopedRefList()
    functions: ScopedRefList = ScopedRefList()


__all__ = ["RefEntry", "ReferencedByBucket", "ReferencesBucket", "ScopedRefList"]
