"""Tests for generate_sources.py — dbt sources.yml builder.

Tests import shared.generate_sources directly for fast, fixture-based execution.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

from shared.generate_sources import generate_sources, write_sources_yml


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


# ── Core filter logic ─────────────────────────────────────────────────────────


def test_is_source_true_included() -> None:
    """Table with is_source: true appears in included list and sources YAML."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Lookup",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.lookup" in result.included
        assert result.sources is not None
    finally:
        tmp.cleanup()


def test_no_writer_found_without_flag_goes_to_unconfirmed() -> None:
    """no_writer_found table without is_source goes to unconfirmed, not included."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Audit",
         "scoping": {"status": "no_writer_found"}},
    ])
    try:
        result = generate_sources(root)
        assert "silver.audit" not in result.included
        assert "silver.audit" in result.unconfirmed
        assert result.sources is None
    finally:
        tmp.cleanup()


def test_seed_table_ignored_not_unconfirmed() -> None:
    """Seed tables are not treated as pending source decisions."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Lookup",
         "scoping": {"status": "no_writer_found"}, "is_seed": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.lookup" not in result.included
        assert "silver.lookup" not in result.unconfirmed
        assert "silver.lookup" not in result.incomplete
        assert result.sources is None
    finally:
        tmp.cleanup()


def test_resolved_table_excluded() -> None:
    """Resolved table (has writer) goes to excluded list."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "DimCustomer",
         "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load"}},
    ])
    try:
        result = generate_sources(root)
        assert "silver.dimcustomer" in result.excluded
        assert "silver.dimcustomer" not in result.included
    finally:
        tmp.cleanup()


def test_resolved_with_is_source_included() -> None:
    """Resolved table marked is_source: true is included (cross-domain scenario)."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "CrossDomain",
         "scoping": {"status": "resolved", "selected_writer": "dbo.usp_other"},
         "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.crossdomain" in result.included
        assert "silver.crossdomain" not in result.excluded
    finally:
        tmp.cleanup()


def test_unscoped_table_goes_to_incomplete() -> None:
    """Table with no scoping goes to incomplete list."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Fresh"},
    ])
    try:
        result = generate_sources(root)
        assert "silver.fresh" in result.incomplete
    finally:
        tmp.cleanup()


def test_unconfirmed_list_populated() -> None:
    """Multiple no_writer_found tables without is_source all land in unconfirmed."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Audit", "scoping": {"status": "no_writer_found"}},
        {"schema": "silver", "name": "Lookup", "scoping": {"status": "no_writer_found"}},
    ])
    try:
        result = generate_sources(root)
        assert set(result.unconfirmed) == {"silver.audit", "silver.lookup"}
        assert result.included == []
    finally:
        tmp.cleanup()


def test_empty_catalog() -> None:
    """Empty catalog returns all empty lists and None sources."""
    tmp, root = _make_project([])
    try:
        result = generate_sources(root)
        assert result.sources is None
        assert result.included == []
        assert result.excluded == []
        assert result.unconfirmed == []
        assert result.incomplete == []
    finally:
        tmp.cleanup()


def test_mixed_tables() -> None:
    """Mix of is_source, resolved, no_writer_found, and unscoped are classified correctly."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Src",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "silver", "name": "Model",
         "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load"}},
        {"schema": "silver", "name": "Pending",
         "scoping": {"status": "no_writer_found"}},
        {"schema": "silver", "name": "Fresh"},
    ])
    try:
        result = generate_sources(root)
        assert result.included == ["silver.src"]
        assert result.excluded == ["silver.model"]
        assert result.unconfirmed == ["silver.pending"]
        assert result.incomplete == ["silver.fresh"]
    finally:
        tmp.cleanup()


# ── --strict flag ─────────────────────────────────────────────────────────────


def test_strict_mode_passes_when_no_incomplete() -> None:
    """--strict does not trigger when all tables are analyzed."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Src",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert result.incomplete == []
    finally:
        tmp.cleanup()


def test_strict_mode_flags_incomplete_scoping() -> None:
    """incomplete list is non-empty for unscoped tables (strict should exit 1)."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Fresh"},
    ])
    try:
        result = generate_sources(root)
        assert "silver.fresh" in result.incomplete
    finally:
        tmp.cleanup()


def test_strict_mode_does_not_flag_unconfirmed() -> None:
    """unconfirmed tables are not in incomplete — strict mode doesn't block them."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Pending", "scoping": {"status": "no_writer_found"}},
    ])
    try:
        result = generate_sources(root)
        assert result.incomplete == []
        assert "silver.pending" in result.unconfirmed
    finally:
        tmp.cleanup()


# ── sources.yml content ───────────────────────────────────────────────────────


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


def test_sources_yml_rejects_duplicate_source_table_names_across_schemas() -> None:
    """The single bronze source namespace cannot represent duplicate table names."""
    tmp, root = _make_project([
        {"schema": "bronze", "name": "Customer",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "archive", "name": "Customer",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert result.sources is None
        assert result.error == "SOURCE_NAME_COLLISION"
        assert result.message is not None
        assert "archive.customer" in result.message
        assert "bronze.customer" in result.message
    finally:
        tmp.cleanup()


def test_excluded_table_with_is_source_not_in_sources() -> None:
    """Table with both excluded: true and is_source: true must NOT appear in sources.yml."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Ghost",
         "scoping": {"status": "no_writer_found"},
         "is_source": True, "excluded": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.ghost" not in result.included
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


def test_write_sources_yml_writes_enriched_yaml_idempotently(tmp_path: Path) -> None:
    """write_sources_yml writes enriched YAML, staging wrappers, and stable repeated output."""
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
    dbt_dir = tmp_path / "dbt"
    (dbt_dir / "models" / "staging").mkdir(parents=True)

    first = write_sources_yml(tmp_path)
    assert first.path is not None
    sources_path = Path(first.path)
    first_content = sources_path.read_text(encoding="utf-8")
    staging_models_path = dbt_dir / "models" / "staging" / "_staging__models.yml"
    customer_wrapper_path = dbt_dir / "models" / "staging" / "stg_bronze__customer.sql"
    order_wrapper_path = dbt_dir / "models" / "staging" / "stg_bronze__order.sql"

    second = write_sources_yml(tmp_path)
    assert second.path == first.path
    assert sources_path.read_text(encoding="utf-8") == first_content
    assert sources_path.name == "_staging__sources.yml"
    assert "&id" not in first_content
    assert "*id" not in first_content
    assert "data_type: INT" in first_content
    assert "- not_null" in first_content
    assert "- unique" in first_content
    assert "loaded_at_field: loaded_at" in first_content
    assert customer_wrapper_path.read_text(encoding="utf-8") == (
        "with\n"
        "\n"
        "source as (\n"
        "\n"
        "    select * from {{ source('bronze', 'Customer') }}\n"
        "\n"
        "),\n"
        "\n"
        "final as (\n"
        "\n"
        "    select * from source\n"
        "\n"
        ")\n"
        "\n"
        "select * from final\n"
    )
    assert order_wrapper_path.read_text(encoding="utf-8") == (
        "with\n"
        "\n"
        "source as (\n"
        "\n"
        "    select * from {{ source('bronze', 'Order') }}\n"
        "\n"
        "),\n"
        "\n"
        "final as (\n"
        "\n"
        "    select * from source\n"
        "\n"
        ")\n"
        "\n"
        "select * from final\n"
    )
    staging_models_content = staging_models_path.read_text(encoding="utf-8")
    assert "name: stg_bronze__customer" in staging_models_content
    assert "name: stg_bronze__order" in staging_models_content


def test_write_sources_yml_does_not_copy_source_tests_to_staging_models(
    tmp_path: Path,
) -> None:
    """Staging wrapper YAML describes columns without duplicating source tests."""
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
                {"name": "email", "sql_type": "NVARCHAR(255)", "is_nullable": True},
            ],
            "primary_keys": [{"constraint_name": "PK_Customer", "columns": ["customer_id"]}],
        }),
        encoding="utf-8",
    )

    result = write_sources_yml(tmp_path)

    assert result.path is not None
    sources = yaml.safe_load(Path(result.path).read_text(encoding="utf-8"))
    staging_models_path = (
        tmp_path / "dbt" / "models" / "staging" / "_staging__models.yml"
    )
    staging_models = yaml.safe_load(staging_models_path.read_text(encoding="utf-8"))
    source_columns = sources["sources"][0]["tables"][0]["columns"]
    staging_columns = staging_models["models"][0]["columns"]
    assert source_columns[0]["tests"] == ["not_null", "unique"]
    assert staging_columns == [
        {"name": "customer_id", "data_type": "INT"},
        {"name": "email", "data_type": "NVARCHAR(255)"},
    ]


def test_write_sources_yml_removes_stale_staging_wrappers(tmp_path: Path) -> None:
    """Source reclassification removes generated wrappers that are no longer declared."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    customer_catalog = tables_dir / "bronze.customer.json"
    order_catalog = tables_dir / "bronze.order.json"
    customer_catalog.write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
        }),
        encoding="utf-8",
    )
    order_catalog.write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
        }),
        encoding="utf-8",
    )
    staging_dir = tmp_path / "dbt" / "models" / "staging"
    staging_dir.mkdir(parents=True)

    write_sources_yml(tmp_path)
    customer_wrapper_path = staging_dir / "stg_bronze__customer.sql"
    order_wrapper_path = staging_dir / "stg_bronze__order.sql"
    assert customer_wrapper_path.exists()
    assert order_wrapper_path.exists()

    order_catalog.write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Order",
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_order"},
        }),
        encoding="utf-8",
    )

    write_sources_yml(tmp_path)
    assert customer_wrapper_path.exists()
    assert not order_wrapper_path.exists()
    assert "name: Order" not in (
        staging_dir / "_staging__sources.yml"
    ).read_text(encoding="utf-8")


def test_write_sources_yml_removes_source_artifacts_when_no_sources_remain(tmp_path: Path) -> None:
    """When the final source is reclassified, generated source YAML and wrappers are removed."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    customer_catalog = tables_dir / "bronze.customer.json"
    customer_catalog.write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "no_writer_found"},
            "is_source": True,
        }),
        encoding="utf-8",
    )
    staging_dir = tmp_path / "dbt" / "models" / "staging"
    staging_dir.mkdir(parents=True)

    first = write_sources_yml(tmp_path)
    assert first.path is not None
    assert (staging_dir / "_staging__sources.yml").exists()
    assert (staging_dir / "_staging__models.yml").exists()
    assert (staging_dir / "stg_bronze__customer.sql").exists()

    customer_catalog.write_text(
        json.dumps({
            "schema": "bronze",
            "name": "Customer",
            "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load_customer"},
        }),
        encoding="utf-8",
    )

    second = write_sources_yml(tmp_path)
    assert second.path is None
    assert not (staging_dir / "_staging__sources.yml").exists()
    assert not (staging_dir / "_staging__models.yml").exists()
    assert not (staging_dir / "stg_bronze__customer.sql").exists()


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


def test_write_sources_yml_creates_file(tmp_path) -> None:
    """write_sources_yml writes the file and returns the path."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.src.json").write_text(
        json.dumps({
            "schema": "silver", "name": "Src",
            "scoping": {"status": "no_writer_found"}, "is_source": True,
        }),
        encoding="utf-8",
    )
    dbt_dir = tmp_path / "dbt"
    (dbt_dir / "models" / "staging").mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "i"],
        cwd=tmp_path, capture_output=True, check=True,
        env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
             "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
             "HOME": str(Path.home())},
    )
    result = write_sources_yml(tmp_path)
    assert result.path is not None
    sources_path = Path(result.path)
    assert sources_path.exists()
    assert sources_path.name == "_staging__sources.yml"
    assert (tmp_path / "dbt" / "models" / "staging" / "stg_bronze__src.sql").exists()
    assert (tmp_path / "dbt" / "models" / "staging" / "_staging__models.yml").exists()
    assert "silver.src" in result.included
