"""Import-boundary tests for split Oracle extraction modules."""

from __future__ import annotations


def test_oracle_extract_facade_exports_legacy_helpers() -> None:
    from shared import oracle_extract

    assert callable(oracle_extract.run_oracle_extraction)
    assert callable(oracle_extract._extract_definitions)
    assert callable(oracle_extract._extract_view_ddl)
    assert callable(oracle_extract._extract_table_columns)
    assert callable(oracle_extract._oracle_column_length)
    assert callable(oracle_extract._extract_pk_unique)
    assert callable(oracle_extract._extract_foreign_keys)
    assert callable(oracle_extract._extract_identity_columns)
    assert callable(oracle_extract._extract_object_types)
    assert callable(oracle_extract._extract_view_columns)
    assert callable(oracle_extract._extract_dmf)
    assert callable(oracle_extract._extract_proc_params)
    assert callable(oracle_extract._extract_packages)


def test_split_modules_export_owned_entrypoints() -> None:
    from shared import oracle_extract_ddl, oracle_extract_queries, oracle_extract_services

    assert callable(oracle_extract_queries.table_columns_sql)
    assert callable(oracle_extract_queries.view_columns_sql)
    assert callable(oracle_extract_ddl.extract_view_ddl_rows)
    assert callable(oracle_extract_services.extract_table_columns)
    assert callable(oracle_extract_services.extract_view_columns)
