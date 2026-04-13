"""Compatibility barrel for setup-ddl internals.

Public entrypoints live here for callers that still import ``shared.setup_ddl_core``.
Implementation is split across ``shared.setup_ddl_support`` modules.
"""

from shared.setup_ddl_support.catalog_write import mark_all_catalog_stale, run_write_catalog
from shared.setup_ddl_support.extract import (
    run_assemble_modules,
    run_assemble_tables,
    run_extract,
    run_list_databases,
    run_list_schemas,
)
from shared.setup_ddl_support.manifest import (
    UnsupportedOperationError,
    run_read_handoff,
    run_write_manifest,
    run_write_partial_manifest,
)

__all__ = [
    "UnsupportedOperationError",
    "mark_all_catalog_stale",
    "run_assemble_modules",
    "run_assemble_tables",
    "run_extract",
    "run_list_databases",
    "run_list_schemas",
    "run_read_handoff",
    "run_write_catalog",
    "run_write_manifest",
    "run_write_partial_manifest",
]
