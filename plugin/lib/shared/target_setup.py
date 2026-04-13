"""Shared target-setup orchestration helpers."""

from __future__ import annotations

import json
from pathlib import Path

from shared.generate_sources import generate_sources, write_sources_yml
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.runtime_config import get_runtime_role


def read_manifest(project_root: Path) -> dict:
    manifest_path = project_root / "manifest.json"
    if not manifest_path.exists():
        raise ValueError("manifest.json not found. Run /setup-ddl first.")
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"manifest.json is not valid JSON: {exc}") from exc


def get_target_source_schema(project_root: Path, default: str = "bronze") -> str:
    """Return the configured target source schema, defaulting to bronze."""
    manifest = read_manifest(project_root)
    target_role = get_runtime_role(manifest, "target")
    if target_role is None:
        return default
    if target_role.schemas is None or not target_role.schemas.source:
        return default
    return target_role.schemas.source


def generate_target_sources(project_root: Path) -> GenerateSourcesOutput:
    """Build logical dbt sources remapped to the configured target source schema."""
    return generate_sources(
        project_root,
        source_schema_override=get_target_source_schema(project_root),
    )


def write_target_sources_yml(project_root: Path) -> GenerateSourcesOutput:
    """Write sources.yml using the configured target source schema mapping."""
    return write_sources_yml(
        project_root,
        source_schema_override=get_target_source_schema(project_root),
    )
