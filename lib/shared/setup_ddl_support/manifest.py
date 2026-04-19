"""Compatibility facade for setup-ddl manifest and source-identity helpers."""

from __future__ import annotations

from shared.runtime_config import (
    TECH_DIALECT,
    dialect_for_technology,
    get_primary_technology,
    get_runtime_role,
    sanitize_manifest,
    set_extraction,
    set_runtime_role,
    validate_supported_dialects,
    validate_supported_technologies,
)
from shared.runtime_config_models import (
    ExtractionSection,
    ManifestModel,
    RuntimeConnection,
    RuntimeRole,
    RuntimeSection,
)
from shared.setup_ddl_support.manifest_io import (
    UnsupportedOperationError,
    read_manifest_or_empty,
    read_manifest_strict,
    require_technology,
    run_read_handoff,
    run_write_manifest,
    run_write_partial_manifest,
)
from shared.setup_ddl_support.oracle_schema_summary import build_oracle_schema_summary
from shared.setup_ddl_support.runtime_identity import (
    build_runtime_role,
    get_connection_identity,
    identity_changed,
)

__all__ = [
    "TECH_DIALECT",
    "UnsupportedOperationError",
    "build_oracle_schema_summary",
    "build_runtime_role",
    "dialect_for_technology",
    "ExtractionSection",
    "get_connection_identity",
    "get_primary_technology",
    "get_runtime_role",
    "identity_changed",
    "ManifestModel",
    "read_manifest_or_empty",
    "read_manifest_strict",
    "require_technology",
    "RuntimeConnection",
    "RuntimeRole",
    "RuntimeSection",
    "run_read_handoff",
    "run_write_manifest",
    "run_write_partial_manifest",
    "sanitize_manifest",
    "set_extraction",
    "set_runtime_role",
    "validate_supported_dialects",
    "validate_supported_technologies",
]
