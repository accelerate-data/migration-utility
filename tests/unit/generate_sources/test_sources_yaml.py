"""Tests for dbt sources YAML construction."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from shared.generate_sources import generate_sources, write_sources_yml
from shared.generate_sources_support.sources_yaml import build_sources_yaml


def test_sources_yaml_support_module_exports_builder() -> None:
    assert callable(build_sources_yaml)


def test_build_sources_yaml_adds_column_tests_freshness_and_relationships() -> None:
    sources = build_sources_yaml(
        [
            {
                "schema": "bronze",
                "name": "Customer",
                "columns": [
                    {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                    {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
                ],
                "primary_keys": [{"columns": ["customer_id"]}],
                "profile": {"watermark": {"column": "loaded_at"}},
            },
            {
                "schema": "bronze",
                "name": "Order",
                "columns": [
                    {"name": "order_id", "sql_type": "INT", "is_nullable": False},
                    {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                ],
                "foreign_keys": [
                    {
                        "columns": ["customer_id"],
                        "referenced_schema": "bronze",
                        "referenced_table": "Customer",
                        "referenced_columns": ["customer_id"],
                    }
                ],
            },
        ],
        physical_source_schema="BRONZE",
    )

    source = sources["sources"][0]
    assert source["name"] == "bronze"
    assert source["schema"] == "BRONZE"
    customer = source["tables"][0]
    order = source["tables"][1]
    assert customer["columns"][0]["tests"] == ["not_null", "unique"]
    assert customer["loaded_at_field"] == "loaded_at"
    assert customer["freshness"]["warn_after"] == {"count": 24, "period": "hour"}
    assert order["columns"][1]["tests"] == [
        "not_null",
        {"relationships": {"to": "source('bronze', 'Customer')", "field": "customer_id"}},
    ]


def _make_project(tables: list[dict]) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Create a temp project with the given table catalog entries."""
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    tables_dir = root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (root / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
    )
    for table in tables:
        schema = table.get("schema", "silver").lower()
        name = table.get("name", "unknown")
        fqn = f"{schema}.{name.lower()}"
        path = tables_dir / f"{fqn}.json"
        path.write_text(json.dumps(table), encoding="utf-8")
    return tmp, root


def test_sources_yml_uses_single_bronze_source_namespace() -> None:
    """All confirmed source tables emit under the canonical bronze source."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "TableA",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "silver", "name": "TableB",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "bronze", "name": "TableC",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        schemas = {s["name"] for s in result.sources["sources"]}
        assert schemas == {"bronze"}
        tables = result.sources["sources"][0]["tables"]
        assert [table["name"] for table in tables] == ["TableA", "TableB", "TableC"]
    finally:
        tmp.cleanup()


def test_sources_yml_includes_catalog_columns_types_and_not_null_tests() -> None:
    """Confirmed source tables emit catalog columns with type metadata and not_null tests."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "email", "data_type": "NVARCHAR(255)", "is_nullable": True},
                {"name": "status", "type": "VARCHAR(20)", "is_nullable": False},
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert table["columns"] == [
            {"name": "customer_id", "data_type": "INT", "tests": ["not_null"]},
            {"name": "email", "data_type": "NVARCHAR(255)"},
            {"name": "status", "data_type": "VARCHAR(20)", "tests": ["not_null"]},
        ]
    finally:
        tmp.cleanup()


def test_sources_yml_uses_profile_watermark_for_freshness_when_column_exists() -> None:
    """profile.watermark.column becomes loaded_at_field only when it is an emitted source column."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
            ],
            "profile": {"watermark": {"column": "loaded_at", "source": "llm"}},
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert table["loaded_at_field"] == "loaded_at"
        assert table["freshness"] == {
            "warn_after": {"count": 24, "period": "hour"},
            "error_after": {"count": 48, "period": "hour"},
        }
    finally:
        tmp.cleanup()


def test_sources_yml_uses_legacy_profile_watermark_columns_for_freshness() -> None:
    """profile.watermark.columns keeps freshness compatible with legacy profile payloads."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
            ],
            "profile": {"watermark": {"columns": ["loaded_at"], "source": "llm"}},
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert table["loaded_at_field"] == "loaded_at"
    finally:
        tmp.cleanup()


def test_sources_yml_skips_freshness_without_usable_profile_watermark() -> None:
    """Change-capture flags and missing watermark columns do not emit source freshness."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "profile": {"watermark": {"column": "missing_loaded_at", "source": "llm"}},
            "change_capture": {"enabled": True, "mechanism": "change_tracking"},
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert "loaded_at_field" not in table
        assert "freshness" not in table
    finally:
        tmp.cleanup()


def test_sources_yml_skips_change_capture_without_profile_watermark() -> None:
    """Change-capture metadata alone does not emit source freshness."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "change_capture": {"enabled": True, "mechanism": "cdc"},
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        table = result.sources["sources"][0]["tables"][0]
        assert "loaded_at_field" not in table
        assert "freshness" not in table
    finally:
        tmp.cleanup()


def test_sources_yml_adds_unique_for_single_column_pk_and_unique_index() -> None:
    """Single-column primary keys and unique indexes emit unique tests."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "email", "sql_type": "NVARCHAR(255)", "is_nullable": True},
            ],
            "primary_keys": [{"constraint_name": "PK_Customer", "columns": ["customer_id"]}],
            "unique_indexes": [{"index_name": "UQ_Customer_Email", "columns": ["email"]}],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        columns = result.sources["sources"][0]["tables"][0]["columns"]
        assert columns[0]["tests"] == ["not_null", "unique"]
        assert columns[1]["tests"] == ["unique"]
    finally:
        tmp.cleanup()


def test_sources_yml_does_not_mark_composite_keys_individually_unique() -> None:
    """Composite primary keys and unique indexes do not add per-column unique tests."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "OrderLine",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "order_id", "sql_type": "INT", "is_nullable": False},
                {"name": "line_id", "sql_type": "INT", "is_nullable": False},
                {"name": "sku", "sql_type": "VARCHAR(30)", "is_nullable": True},
            ],
            "primary_keys": [{"constraint_name": "PK_OrderLine", "columns": ["order_id", "line_id"]}],
            "unique_indexes": [{"index_name": "UQ_OrderLine", "columns": ["order_id", "sku"]}],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        columns = result.sources["sources"][0]["tables"][0]["columns"]
        assert columns[0]["tests"] == ["not_null"]
        assert columns[1]["tests"] == ["not_null"]
        assert "tests" not in columns[2]
    finally:
        tmp.cleanup()


def test_sources_yml_adds_relationship_for_confirmed_source_reference() -> None:
    """Single-column FKs to another confirmed source emit dbt relationships tests."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
        },
        {
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "foreign_keys": [
                {
                    "constraint_name": "FK_Order_Customer",
                    "columns": ["customer_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                }
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        order_table = next(
            table
            for table in result.sources["sources"][0]["tables"]
            if table["name"] == "Order"
        )
        customer_id = order_table["columns"][0]
        assert customer_id["tests"] == [
            "not_null",
            {"relationships": {"to": "source('bronze', 'Customer')", "field": "customer_id"}},
        ]
    finally:
        tmp.cleanup()


def test_sources_yml_normalizes_relationship_target_casing_to_emitted_source_names() -> None:
    """Relationship targets use the same canonical schema/table names written to sources.yml."""
    tmp, root = _make_project([
        {
            "schema": "BrOnZe",
            "name": "CuStOmEr",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
        },
        {
            "schema": "BrOnZe",
            "name": "OrDeR",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "foreign_keys": [
                {
                    "constraint_name": "FK_OrDeR_CuStOmEr",
                    "columns": ["customer_id"],
                    "referenced_schema": "BRONZE",
                    "referenced_table": "customer",
                    "referenced_columns": ["customer_id"],
                }
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        order_table = next(
            table
            for table in result.sources["sources"][0]["tables"]
            if table["name"] == "OrDeR"
        )
        customer_id = order_table["columns"][0]
        assert customer_id["tests"] == [
            "not_null",
            {"relationships": {"to": "source('bronze', 'CuStOmEr')", "field": "customer_id"}},
        ]
    finally:
        tmp.cleanup()


def test_sources_yml_skips_unresolved_and_composite_relationships() -> None:
    """FK tests are skipped when the reference is not source-local and single-column."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "OrderLine",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "order_id", "sql_type": "INT", "is_nullable": False},
                {"name": "line_id", "sql_type": "INT", "is_nullable": False},
                {"name": "customer_id", "sql_type": "INT", "is_nullable": True},
            ],
            "foreign_keys": [
                {
                    "constraint_name": "FK_OrderLine_Order",
                    "columns": ["order_id", "line_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": "Order",
                    "referenced_columns": ["order_id", "line_id"],
                },
                {
                    "constraint_name": "FK_OrderLine_Customer",
                    "columns": ["customer_id"],
                    "referenced_schema": "silver",
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                },
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        columns = result.sources["sources"][0]["tables"][0]["columns"]
        assert columns[0]["tests"] == ["not_null"]
        assert columns[1]["tests"] == ["not_null"]
        assert "tests" not in columns[2]
    finally:
        tmp.cleanup()


def test_sources_yml_skips_relationship_when_referenced_table_is_not_emitted_source() -> None:
    """FK targets present in catalog but absent from sources.yml do not emit relationships."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
        },
        {
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "foreign_keys": [
                {
                    "constraint_name": "FK_Order_Customer",
                    "columns": ["customer_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                }
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        order_table = result.sources["sources"][0]["tables"][0]
        assert order_table["name"] == "Order"
        assert order_table["columns"][0]["tests"] == ["not_null"]
    finally:
        tmp.cleanup()


def test_sources_yml_skips_malformed_none_relationship_values() -> None:
    """Malformed FK values with None schema/table/columns do not emit relationships tests."""
    tmp, root = _make_project([
        {
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
        },
        {
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [{"name": "customer_id", "sql_type": "INT", "is_nullable": False}],
            "foreign_keys": [
                {
                    "constraint_name": "FK_Order_Customer_Schema_None",
                    "columns": ["customer_id"],
                    "referenced_schema": None,
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                },
                {
                    "constraint_name": "FK_Order_Customer_Table_None",
                    "columns": ["customer_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": None,
                    "referenced_columns": ["customer_id"],
                },
                {
                    "constraint_name": "FK_Order_Customer_Column_None",
                    "columns": ["customer_id"],
                    "referenced_schema": "bronze",
                    "referenced_table": "Customer",
                    "referenced_columns": [None],
                },
                {
                    "constraint_name": "FK_Order_Customer_Local_None",
                    "columns": [None],
                    "referenced_schema": "bronze",
                    "referenced_table": "Customer",
                    "referenced_columns": ["customer_id"],
                },
            ],
        },
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        order_table = next(
            table
            for table in result.sources["sources"][0]["tables"]
            if table["name"] == "Order"
        )
        customer_id = order_table["columns"][0]
        assert customer_id["tests"] == ["not_null"]
    finally:
        tmp.cleanup()


def test_sources_yml_uses_target_sql_type_and_hides_source_debug_types() -> None:
    """Generated source YAML uses sql_type without leaking source/debug fields."""
    tmp, root = _make_project([
        {
            "schema": "silver",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {
                    "name": "customer_id",
                    "source_sql_type": "NUMBER(10,0)",
                    "canonical_tsql_type": "INT",
                    "sql_type": "INT",
                    "is_nullable": False,
                },
            ],
        },
    ])
    try:
        result = write_sources_yml(root)
        assert result.path is not None
        sources_content = Path(result.path).read_text(encoding="utf-8")
        assert "data_type: INT" in sources_content
        assert "NUMBER" not in sources_content
        assert "canonical_tsql_type" not in sources_content
    finally:
        tmp.cleanup()


def test_write_sources_yml_writes_source_yaml_idempotently(tmp_path: Path) -> None:
    """write_sources_yml writes stable enriched source YAML."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "bronze.customer.json").write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "customer_id", "sql_type": "INT", "is_nullable": False},
                {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
            ],
            "primary_keys": [{"constraint_name": "PK_Customer", "columns": ["customer_id"]}],
            "profile": {"watermark": {"column": "loaded_at"}},
        }),
        encoding="utf-8",
    )
    (tables_dir / "bronze.order.json").write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {"name": "order_id", "sql_type": "INT", "is_nullable": False},
                {"name": "loaded_at", "sql_type": "DATETIME2", "is_nullable": False},
            ],
            "primary_keys": [{"constraint_name": "PK_Order", "columns": ["order_id"]}],
            "profile": {"watermark": {"column": "loaded_at"}},
        }),
        encoding="utf-8",
    )
    (tmp_path / "dbt" / "models" / "staging").mkdir(parents=True)

    first = write_sources_yml(tmp_path)
    assert first.path is not None
    sources_path = Path(first.path)
    first_content = sources_path.read_text(encoding="utf-8")

    second = write_sources_yml(tmp_path)

    assert second.path == first.path
    assert "dbt/models/staging/_staging__sources.yml" in first.written_paths
    assert sources_path.read_text(encoding="utf-8") == first_content
    assert sources_path.name == "_staging__sources.yml"
    assert "&id" not in first_content
    assert "*id" not in first_content
    assert "data_type: INT" in first_content
    assert "- not_null" in first_content
    assert "- unique" in first_content
    assert "loaded_at_field: loaded_at" in first_content


def test_write_sources_yml_default_write_allows_legacy_type_fallback(tmp_path: Path) -> None:
    """Plain generate-sources writes source YAML without setup-target contract validation."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "bronze.customer.json").write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
            "columns": [
                {
                    "name": "customer_id",
                    "data_type": "NUMBER(10,0)",
                    "is_nullable": False,
                }
            ],
        }),
        encoding="utf-8",
    )

    result = write_sources_yml(tmp_path)

    assert result.error is None
    assert result.sources is not None
    assert Path(result.path or "").exists()
