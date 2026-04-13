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
    "oracle": "oracle",
}

KNOWN_TECHNOLOGIES = frozenset(TECH_DIALECT)
SUPPORTED_DIALECTS = frozenset(TECH_DIALECT.values())


def configured_technologies(manifest: dict[str, Any]) -> list[str]:
    """Return all explicitly configured manifest technologies."""
    configured: list[str] = []
    top_level = manifest.get("technology")
    if isinstance(top_level, str) and top_level:
        configured.append(top_level)
    for role_name in ("source", "target", "sandbox"):
        role = get_runtime_role(manifest, role_name)
        if role is not None and role.technology:
            configured.append(role.technology)
    return configured


def validate_supported_technologies(manifest: dict[str, Any]) -> None:
    """Raise when manifest config includes unsupported technologies."""
    unsupported = sorted({value for value in configured_technologies(manifest) if value not in KNOWN_TECHNOLOGIES})
    if unsupported:
        raise ValueError(
            "manifest.json does not define a supported runtime technology. "
            f"Unsupported: {unsupported}. Supported: {sorted(KNOWN_TECHNOLOGIES)}."
        )


def configured_dialects(manifest: dict[str, Any]) -> list[str]:
    """Return all explicitly configured manifest dialects."""
    configured: list[str] = []
    top_level = manifest.get("dialect")
    if isinstance(top_level, str) and top_level:
        configured.append(top_level)
    for role_name in ("source", "target", "sandbox"):
        role = get_runtime_role(manifest, role_name)
        if role is not None and role.dialect:
            configured.append(role.dialect)
    return configured


def validate_supported_dialects(manifest: dict[str, Any]) -> None:
    """Raise when manifest config includes unsupported or mismatched dialects."""
    unsupported = sorted({value for value in configured_dialects(manifest) if value not in SUPPORTED_DIALECTS})
    if unsupported:
        raise ValueError(
            "manifest.json does not define a supported runtime dialect. "
            f"Unsupported: {unsupported}. Supported: {sorted(SUPPORTED_DIALECTS)}."
        )

    model = get_manifest_model(manifest)
    if model.technology and model.dialect and model.technology in KNOWN_TECHNOLOGIES:
        expected = dialect_for_technology(model.technology)
        if model.dialect != expected:
            raise ValueError(
                "manifest.json has mismatched top-level technology and dialect. "
                f"Expected {expected!r} for technology {model.technology!r}, got {model.dialect!r}."
            )

    runtime = model.runtime or RuntimeSection()
    for role_name in ("source", "target", "sandbox"):
        role = getattr(runtime, role_name, None)
        if role is None or role.technology not in KNOWN_TECHNOLOGIES:
            continue
        expected = dialect_for_technology(role.technology)
        if role.dialect != expected:
            raise ValueError(
                f"manifest.json has mismatched runtime.{role_name} technology and dialect. "
                f"Expected {expected!r} for technology {role.technology!r}, got {role.dialect!r}."
            )


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
            technology = role.technology
            return technology if technology in KNOWN_TECHNOLOGIES else None
    model = get_manifest_model(manifest)
    technology = model.technology
    return technology if technology in KNOWN_TECHNOLOGIES else None


def get_primary_dialect(manifest: dict[str, Any]) -> str:
    """Return the source dialect or a top-level fallback."""
    validate_supported_technologies(manifest)
    validate_supported_dialects(manifest)
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
    updated = sanitize_manifest(manifest)
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


def sanitize_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Drop unsupported runtime metadata and canonicalize supported dialects."""
    updated = deepcopy(manifest)

    runtime = updated.get("runtime")
    if isinstance(runtime, dict):
        cleaned_runtime: dict[str, Any] = {}
        for role_name in ("source", "target", "sandbox"):
            role = runtime.get(role_name)
            if not isinstance(role, dict):
                continue
            technology = role.get("technology")
            if technology not in KNOWN_TECHNOLOGIES:
                continue
            cleaned_role = deepcopy(role)
            cleaned_role["dialect"] = dialect_for_technology(technology)
            cleaned_runtime[role_name] = cleaned_role
        if cleaned_runtime:
            updated["runtime"] = cleaned_runtime
        else:
            updated.pop("runtime", None)

    technology = updated.get("technology")
    if technology in KNOWN_TECHNOLOGIES:
        updated["dialect"] = dialect_for_technology(technology)
    else:
        updated.pop("technology", None)
        if updated.get("dialect") not in SUPPORTED_DIALECTS:
            updated.pop("dialect", None)

    return updated


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
