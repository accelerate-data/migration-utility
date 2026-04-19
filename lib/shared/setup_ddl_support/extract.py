"""Compatibility facade for setup-ddl extraction support entrypoints."""

from __future__ import annotations

from shared.setup_ddl_support.assembly import (
    assemble_ddl_from_staging,
    run_assemble_modules,
    run_assemble_tables,
)
from shared.setup_ddl_support.db_extraction import run_db_extraction
from shared.setup_ddl_support.discovery import run_list_databases, run_list_schemas
from shared.setup_ddl_support.extract_orchestration import run_extract

__all__ = [
    "assemble_ddl_from_staging",
    "run_assemble_modules",
    "run_assemble_tables",
    "run_db_extraction",
    "run_extract",
    "run_list_databases",
    "run_list_schemas",
]
