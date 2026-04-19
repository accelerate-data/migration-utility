"""Import-boundary tests for split setup-ddl extraction support modules."""

from __future__ import annotations


def test_extract_facade_reexports_existing_public_entrypoints() -> None:
    from shared.setup_ddl_support import extract

    assert isinstance(extract.TECH_DIALECT, dict)
    assert issubclass(extract.UnsupportedOperationError, Exception)
    assert callable(extract.run_assemble_modules)
    assert callable(extract.run_assemble_tables)
    assert callable(extract.assemble_ddl_from_staging)
    assert callable(extract.run_list_databases)
    assert callable(extract.run_list_schemas)
    assert callable(extract.run_db_extraction)
    assert callable(extract.run_extract)
    assert callable(extract.build_oracle_schema_summary)
    assert callable(extract.get_connection_identity)
    assert callable(extract.identity_changed)
    assert callable(extract.mark_all_catalog_stale)
    assert callable(extract.read_manifest_strict)
    assert callable(extract.require_technology)
    assert callable(extract.run_write_catalog)
    assert callable(extract.run_write_manifest)


def test_extraction_support_modules_own_split_entrypoints() -> None:
    from shared.setup_ddl_support.assembly import (
        assemble_ddl_from_staging,
        run_assemble_modules,
        run_assemble_tables,
    )
    from shared.setup_ddl_support.db_extraction import run_db_extraction
    from shared.setup_ddl_support.discovery import run_list_databases, run_list_schemas
    from shared.setup_ddl_support.extract_orchestration import run_extract

    assert callable(run_assemble_modules)
    assert callable(run_assemble_tables)
    assert callable(assemble_ddl_from_staging)
    assert callable(run_list_databases)
    assert callable(run_list_schemas)
    assert callable(run_db_extraction)
    assert callable(run_extract)
