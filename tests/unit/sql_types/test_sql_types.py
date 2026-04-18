from __future__ import annotations

import shared.sql_types as sql_types


def test_sql_server_column_maps_to_target_sql_server_contract() -> None:
    mapped = sql_types.map_catalog_column_type(
        source_technology="sql_server",
        target_technology="sql_server",
        type_name="nvarchar",
        max_length=100,
        precision=0,
        scale=0,
    )

    assert mapped == {
        "source_sql_type": "NVARCHAR(50)",
        "canonical_tsql_type": "NVARCHAR(50)",
        "sql_type": "NVARCHAR(50)",
    }


def test_oracle_number_maps_through_canonical_tsql_to_sql_server() -> None:
    mapped = sql_types.map_catalog_column_type(
        source_technology="oracle",
        target_technology="sql_server",
        type_name="NUMBER",
        max_length=0,
        precision=10,
        scale=2,
    )

    assert mapped == {
        "source_sql_type": "NUMBER(10,2)",
        "canonical_tsql_type": "DECIMAL(10,2)",
        "sql_type": "DECIMAL(10,2)",
    }


def test_sql_server_datetime_maps_to_target_oracle() -> None:
    mapped = sql_types.map_catalog_column_type(
        source_technology="sql_server",
        target_technology="oracle",
        type_name="datetime2",
        max_length=8,
        precision=0,
        scale=7,
    )

    assert mapped == {
        "source_sql_type": "DATETIME2",
        "canonical_tsql_type": "DATETIME2",
        "sql_type": "TIMESTAMP",
    }
