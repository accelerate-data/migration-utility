"""Canonical Intermediate Representation (IR) types for migration skills.

All types are Pydantic models — JSON-serializable via model_dump_json().
Inter-skill data flows as JSON; these types are never shared as live objects
across process boundaries.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class TableRef(BaseModel):
    """A schema-qualified table reference."""

    schema_name: str
    table_name: str

    @property
    def fqn(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class ColumnRef(BaseModel):
    """A reference to a column, optionally qualified by table."""

    column_name: str
    table_ref: TableRef | None = None


class ProcParam(BaseModel):
    """A stored procedure parameter."""

    name: str
    sql_type: str
    default_value: str | None = None
    is_output: bool = False


class CteNode(BaseModel):
    """A single CTE definition within a SELECT model."""

    name: str
    select_sql: str


class SelectModel(BaseModel):
    """The logical SELECT structure extracted from a procedure body."""

    ctes: list[CteNode] = Field(default_factory=list)
    final_select: str
    columns: list[ColumnRef] = Field(default_factory=list)


class Procedure(BaseModel):
    """Full representation of a parsed stored procedure."""

    schema_name: str
    procedure_name: str
    params: list[ProcParam] = Field(default_factory=list)
    body_sql: str
    source_file: str | None = None
    ast: Any | None = Field(default=None, exclude=True)

    @property
    def fqn(self) -> str:
        return f"{self.schema_name}.{self.procedure_name}"

    model_config = {"arbitrary_types_allowed": True}
