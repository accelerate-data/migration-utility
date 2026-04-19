"""Import-boundary tests for split setup-ddl extraction support modules."""

from __future__ import annotations


def test_extract_facade_reexports_existing_public_entrypoints() -> None:
    from shared.setup_ddl_support import extract

    assert callable(extract.run_assemble_modules)
    assert callable(extract.run_assemble_tables)
    assert callable(extract.assemble_ddl_from_staging)
    assert callable(extract.run_list_databases)
    assert callable(extract.run_list_schemas)
    assert callable(extract.run_db_extraction)
    assert callable(extract.run_extract)


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
