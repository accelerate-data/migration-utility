"""Typed manifest runtime models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RuntimeConnection(BaseModel):
    """Connection details for one runtime role."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    host: str | None = None
    port: str | None = None
    database: str | None = None
    service: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")
    user: str | None = None
    password_env: str | None = None
    driver: str | None = None
    dsn: str | None = None
    path: str | None = None
    tenant_id: str | None = None


class RuntimeSchemas(BaseModel):
    """Named schema roles for a runtime target."""

    model_config = ConfigDict(extra="forbid")

    source: str | None = None
    marts: str | None = None


class RuntimeRole(BaseModel):
    """One independent runtime role."""

    model_config = ConfigDict(extra="forbid")

    technology: str
    dialect: str
    connection: RuntimeConnection = Field(default_factory=RuntimeConnection)
    schemas: RuntimeSchemas | None = None


class RuntimeSection(BaseModel):
    """Runtime roles stored in manifest.json."""

    model_config = ConfigDict(extra="forbid")

    source: RuntimeRole | None = None
    target: RuntimeRole | None = None
    sandbox: RuntimeRole | None = None


class ExtractionSection(BaseModel):
    """Derived extraction state."""

    model_config = ConfigDict(extra="forbid")

    schemas: list[str] = Field(default_factory=list)
    extracted_at: str | None = None


class ManifestModel(BaseModel):
    """Top-level manifest contract."""

    model_config = ConfigDict(extra="allow")

    schema_version: str | None = None
    technology: str | None = None
    dialect: str | None = None
    runtime: RuntimeSection | None = None
    extraction: ExtractionSection | None = None
    init_handoff: dict[str, Any] | None = None
