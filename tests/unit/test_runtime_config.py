"""Tests for the typed runtime manifest helpers."""

from __future__ import annotations

import pytest

from shared.runtime_config import (
    dialect_for_technology,
    get_extracted_schemas,
    get_manifest_model,
    get_primary_dialect,
    get_primary_technology,
    get_runtime_role,
    get_sandbox_name,
    set_extraction,
    set_runtime_role,
)
from shared.runtime_config_models import (
    RuntimeConnection,
    RuntimeRole,
    RuntimeSchemas,
)


def test_dialect_for_technology_includes_duckdb() -> None:
    assert dialect_for_technology("duckdb") == "duckdb"


def test_dialect_for_technology_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Unknown technology"):
        dialect_for_technology("sqlite")


def test_set_runtime_role_sets_primary_fields_from_source() -> None:
    manifest = {}
    source = RuntimeRole(
        technology="oracle",
        dialect="oracle",
        connection=RuntimeConnection(service="SRCPDB", schema="bronze"),
    )

    updated = set_runtime_role(manifest, "source", source)

    assert updated["technology"] == "oracle"
    assert updated["dialect"] == "oracle"
    assert updated["runtime"]["source"]["connection"]["service"] == "SRCPDB"
    assert updated["runtime"]["source"]["connection"]["schema"] == "bronze"


def test_runtime_roles_are_independent() -> None:
    manifest = set_runtime_role(
        {},
        "source",
        RuntimeRole(
            technology="oracle",
            dialect="oracle",
            connection=RuntimeConnection(service="SRCPDB", schema="bronze"),
        ),
    )
    manifest = set_runtime_role(
        manifest,
        "target",
        RuntimeRole(
            technology="duckdb",
            dialect="duckdb",
            connection=RuntimeConnection(path=".runtime/duckdb/target.duckdb"),
            schemas=RuntimeSchemas(source="bronze", marts="silver"),
        ),
    )
    manifest = set_runtime_role(
        manifest,
        "sandbox",
        RuntimeRole(
            technology="sql_server",
            dialect="tsql",
            connection=RuntimeConnection(database="MigrationTestSandbox"),
        ),
    )

    assert get_runtime_role(manifest, "source").technology == "oracle"
    assert get_runtime_role(manifest, "target").technology == "duckdb"
    assert get_runtime_role(manifest, "sandbox").technology == "sql_server"
    assert get_runtime_role(manifest, "target").schemas.source == "bronze"


def test_set_extraction_writes_extraction_section() -> None:
    updated = set_extraction({}, ["bronze", "silver"], "2026-04-13T00:00:00Z")

    assert updated["extraction"]["schemas"] == ["bronze", "silver"]
    assert updated["extraction"]["extracted_at"] == "2026-04-13T00:00:00Z"
    assert get_extracted_schemas(updated) == ["bronze", "silver"]


def test_get_sandbox_name_prefers_connection_values() -> None:
    manifest = set_runtime_role(
        {},
        "sandbox",
        RuntimeRole(
            technology="duckdb",
            dialect="duckdb",
            connection=RuntimeConnection(path=".runtime/duckdb/sandbox.duckdb"),
        ),
    )

    assert get_sandbox_name(manifest) == ".runtime/duckdb/sandbox.duckdb"


def test_primary_helpers_prefer_source_role() -> None:
    manifest = {
        "technology": "sql_server",
        "dialect": "tsql",
        "runtime": {
            "source": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"service": "SRCPDB", "schema": "bronze"},
            }
        },
    }

    assert get_primary_technology(manifest) == "oracle"
    assert get_primary_dialect(manifest) == "oracle"


def test_manifest_model_allows_runtime_and_extraction() -> None:
    manifest = {
        "schema_version": "1.1",
        "runtime": {
            "source": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"service": "SRCPDB", "schema": "bronze"},
            },
            "target": {
                "technology": "duckdb",
                "dialect": "duckdb",
                "connection": {"path": ".runtime/duckdb/target.duckdb"},
                "schemas": {"source": "bronze"},
            },
        },
        "extraction": {"schemas": ["bronze"], "extracted_at": "2026-04-13T00:00:00Z"},
    }

    model = get_manifest_model(manifest)

    assert model.runtime is not None
    assert model.runtime.target is not None
    assert model.runtime.target.connection.path == ".runtime/duckdb/target.duckdb"
