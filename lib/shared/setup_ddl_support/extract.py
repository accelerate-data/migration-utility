"""Compatibility facade for setup-ddl extraction support entrypoints."""

from __future__ import annotations

from shared.setup_ddl_support.assembly import (
    assemble_ddl_from_staging,
    run_assemble_modules,
    run_assemble_tables,
)
from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale, run_write_catalog
from shared.setup_ddl_support.db_extraction import run_db_extraction
from shared.setup_ddl_support.discovery import run_list_databases, run_list_schemas
from shared.setup_ddl_support.extract_orchestration import run_extract
from shared.setup_ddl_support.manifest import (
    TECH_DIALECT,
    UnsupportedOperationError,
    build_oracle_schema_summary,
    get_connection_identity,
    identity_changed,
    read_manifest_strict,
    require_technology,
    run_write_manifest,
)

__all__ = [
    "TECH_DIALECT",
    "UnsupportedOperationError",
    "assemble_ddl_from_staging",
    "build_oracle_schema_summary",
    "get_connection_identity",
    "identity_changed",
    "mark_all_catalog_stale",
    "read_manifest_strict",
    "require_technology",
    "run_assemble_modules",
    "run_assemble_tables",
    "run_db_extraction",
    "run_extract",
    "run_list_databases",
    "run_list_schemas",
    "run_write_catalog",
    "run_write_manifest",
]
