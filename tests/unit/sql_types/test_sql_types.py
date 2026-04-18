from __future__ import annotations

import pytest

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


@pytest.mark.parametrize(
    ("source_technology", "target_technology", "type_name", "max_length", "precision", "scale", "expected"),
    [
        (
            "sql_server",
            "sql_server",
            "nchar",
            10,
            0,
            0,
            {
                "source_sql_type": "NCHAR(5)",
                "canonical_tsql_type": "NCHAR(5)",
                "sql_type": "NCHAR(5)",
            },
        ),
        (
            "sql_server",
            "oracle",
            "money",
            8,
            19,
            4,
            {
                "source_sql_type": "MONEY",
                "canonical_tsql_type": "DECIMAL(19,4)",
                "sql_type": "NUMBER(19,4)",
            },
        ),
        (
            "sql_server",
            "oracle",
            "bit",
            1,
            1,
            0,
            {
                "source_sql_type": "BIT",
                "canonical_tsql_type": "BIT",
                "sql_type": "NUMBER(1,0)",
            },
        ),
        (
            "sql_server",
            "oracle",
            "uniqueidentifier",
            16,
            0,
            0,
            {
                "source_sql_type": "UNIQUEIDENTIFIER",
                "canonical_tsql_type": "UNIQUEIDENTIFIER",
                "sql_type": "RAW(16)",
            },
        ),
        (
            "oracle",
            "sql_server",
            "NUMBER",
            0,
            9,
            0,
            {
                "source_sql_type": "NUMBER(9,0)",
                "canonical_tsql_type": "INT",
                "sql_type": "INT",
            },
        ),
        (
            "oracle",
            "sql_server",
            "NUMBER",
            0,
            18,
            0,
            {
                "source_sql_type": "NUMBER(18,0)",
                "canonical_tsql_type": "BIGINT",
                "sql_type": "BIGINT",
            },
        ),
        (
            "oracle",
            "sql_server",
            "RAW",
            16,
            0,
            0,
            {
                "source_sql_type": "RAW(16)",
                "canonical_tsql_type": "VARBINARY(16)",
                "sql_type": "VARBINARY(16)",
            },
        ),
        (
            "oracle",
            "sql_server",
            "NVARCHAR2",
            20,
            0,
            0,
            {
                "source_sql_type": "NVARCHAR2(20)",
                "canonical_tsql_type": "NVARCHAR(20)",
                "sql_type": "NVARCHAR(20)",
            },
        ),
    ],
)
def test_catalog_column_type_mapping_preserves_source_and_renders_target(
    source_technology: str,
    target_technology: str,
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
    expected: dict[str, str],
) -> None:
    assert sql_types.map_catalog_column_type(
        source_technology=source_technology,
        target_technology=target_technology,
        type_name=type_name,
        max_length=max_length,
        precision=precision,
        scale=scale,
    ) == expected


def test_unsupported_source_type_raises_mapping_error() -> None:
    with pytest.raises(sql_types.TypeMappingError, match="Unsupported source type"):
        sql_types.map_catalog_column_type(
            source_technology="oracle",
            target_technology="sql_server",
            type_name="XMLTYPE",
            max_length=0,
            precision=0,
            scale=0,
        )
