"""Shared manifest runtime helpers."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from shared.runtime_config_models import (
    ExtractionSection,
    ManifestModel,
    RuntimeRole,
    RuntimeSection,
)

TECH_DIALECT = {
    "sql_server": "tsql",
    "snowflake": "snowflake",
    "oracle": "oracle",
    "duckdb": "duckdb",
}

KNOWN_TECHNOLOGIES = frozenset(TECH_DIALECT)


def dialect_for_technology(technology: str) -> str:
    """Return the canonical dialect for a supported technology."""
    if technology not in TECH_DIALECT:
        raise ValueError(
            f"Unknown technology: {technology}. Must be one of {sorted(KNOWN_TECHNOLOGIES)}."
        )
    return TECH_DIALECT[technology]


def get_manifest_model(manifest: dict[str, Any]) -> ManifestModel:
    """Validate and return the full typed manifest."""
    return ManifestModel.model_validate(manifest)


def get_runtime_section(manifest: dict[str, Any]) -> RuntimeSection:
    """Return the runtime section, defaulting to an empty one."""
    model = get_manifest_model(manifest)
    return model.runtime or RuntimeSection()


def get_runtime_role(manifest: dict[str, Any], role: str) -> RuntimeRole | None:
    """Return one runtime role from manifest.json."""
    return getattr(get_runtime_section(manifest), role, None)


def get_extraction_section(manifest: dict[str, Any]) -> ExtractionSection:
    """Return extraction state, defaulting to an empty section."""
    model = get_manifest_model(manifest)
    return model.extraction or ExtractionSection()


def get_extracted_schemas(manifest: dict[str, Any]) -> list[str]:
    """Return extracted schemas from manifest state."""
    return get_extraction_section(manifest).schemas


def get_primary_technology(manifest: dict[str, Any]) -> str | None:
    """Return the first configured runtime technology, preferring source."""
    for role_name in ("source", "target", "sandbox"):
        role = get_runtime_role(manifest, role_name)
        if role is not None:
            return role.technology
    model = get_manifest_model(manifest)
    technology = model.technology
    return technology if technology in KNOWN_TECHNOLOGIES else None


def get_primary_dialect(manifest: dict[str, Any]) -> str:
    """Return the source dialect or a top-level fallback."""
    source = get_runtime_role(manifest, "source")
    if source is not None:
        return source.dialect
    model = get_manifest_model(manifest)
    if model.dialect:
        return model.dialect
    technology = get_primary_technology(manifest)
    if technology is not None:
        return dialect_for_technology(technology)
    return "tsql"


def get_sandbox_name(manifest: dict[str, Any]) -> str | None:
    """Return the active sandbox environment name."""
    sandbox = get_runtime_role(manifest, "sandbox")
    if sandbox is None:
        return None
    return (
        sandbox.connection.database
        or sandbox.connection.schema_name
        or sandbox.connection.path
        or sandbox.connection.service
    )


def _role_dict(role: RuntimeRole | None) -> dict[str, Any] | None:
    if role is None:
        return None
    return role.model_dump(mode="json", by_alias=True, exclude_none=True)


def set_runtime_role(
    manifest: dict[str, Any],
    role_name: str,
    role: RuntimeRole | None,
) -> dict[str, Any]:
    """Set or clear a runtime role and return a validated manifest dict."""
    updated = deepcopy(manifest)
    runtime = dict(updated.get("runtime") or {})
    if role is None:
        runtime.pop(role_name, None)
    else:
        runtime[role_name] = _role_dict(role)

    if runtime:
        updated["runtime"] = runtime
    else:
        updated.pop("runtime", None)

    source = get_runtime_role(updated, "source")
    if source is not None:
        updated["technology"] = source.technology
        updated["dialect"] = source.dialect

    return get_manifest_model(updated).model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )


def set_extraction(
    manifest: dict[str, Any],
    schemas: list[str],
    extracted_at: str | None = None,
) -> dict[str, Any]:
    """Set extraction state and return a validated manifest dict."""
    updated = deepcopy(manifest)
    extraction = ExtractionSection(
        schemas=schemas,
        extracted_at=extracted_at or datetime.now(timezone.utc).isoformat(),
    )
    updated["extraction"] = extraction.model_dump(mode="json", exclude_none=True)
    return get_manifest_model(updated).model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
