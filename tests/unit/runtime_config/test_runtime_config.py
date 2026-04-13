"""Tests for the typed runtime manifest helpers."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.runtime_config import (
    configured_technologies,
    configured_dialects,
    dialect_for_technology,
    get_extracted_schemas,
    get_manifest_model,
    get_primary_dialect,
    get_primary_technology,
    get_runtime_role,
    get_sandbox_name,
    sanitize_manifest,
    set_extraction,
    set_runtime_role,
    validate_supported_dialects,
    validate_supported_technologies,
)
from shared.runtime_config_models import (
    RuntimeConnection,
    RuntimeRole,
    RuntimeSchemas,
)


def test_dialect_for_technology_includes_sql_server() -> None:
    assert dialect_for_technology("sql_server") == "tsql"


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
            technology="sql_server",
            dialect="tsql",
            connection=RuntimeConnection(database="TargetDB"),
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
    assert get_runtime_role(manifest, "target").technology == "sql_server"
    assert get_runtime_role(manifest, "sandbox").technology == "sql_server"
    assert get_runtime_role(manifest, "target").schemas.source == "bronze"


def test_runtime_schemas_reject_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        RuntimeSchemas.model_validate({"source": "bronze", "soruce": "typo"})


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
            technology="oracle",
            dialect="oracle",
            connection=RuntimeConnection(service="SANDBOXPDB"),
        ),
    )

    assert get_sandbox_name(manifest) == "SANDBOXPDB"


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


def test_primary_technology_rejects_unsupported_runtime_role() -> None:
    manifest = {
        "runtime": {
            "source": {
                "technology": "duckdb",
                "dialect": "duckdb",
                "connection": {"path": ".runtime/source.duckdb"},
            }
        }
    }

    assert get_primary_technology(manifest) is None


def test_configured_technologies_includes_all_runtime_roles() -> None:
    manifest = {
        "technology": "sql_server",
        "runtime": {
            "source": {"technology": "sql_server", "dialect": "tsql", "connection": {"database": "SourceDB"}},
            "target": {"technology": "oracle", "dialect": "oracle", "connection": {"service": "TARGETPDB"}},
        },
    }

    assert configured_technologies(manifest) == ["sql_server", "sql_server", "oracle"]


def test_validate_supported_technologies_rejects_mixed_manifest() -> None:
    manifest = {
        "runtime": {
            "source": {"technology": "sql_server", "dialect": "tsql", "connection": {"database": "SourceDB"}},
            "target": {"technology": "duckdb", "dialect": "duckdb", "connection": {"path": ".runtime/target.duckdb"}},
        }
    }

    with pytest.raises(ValueError, match="Unsupported: \\['duckdb'\\]"):
        validate_supported_technologies(manifest)


def test_configured_dialects_includes_top_level_and_runtime_roles() -> None:
    manifest = {
        "dialect": "tsql",
        "runtime": {
            "source": {"technology": "sql_server", "dialect": "tsql", "connection": {"database": "SourceDB"}},
            "target": {"technology": "oracle", "dialect": "oracle", "connection": {"service": "TARGETPDB"}},
        },
    }

    assert configured_dialects(manifest) == ["tsql", "tsql", "oracle"]


def test_validate_supported_dialects_rejects_unsupported_top_level_dialect() -> None:
    manifest = {"technology": "sql_server", "dialect": "duckdb"}

    with pytest.raises(ValueError, match="supported runtime dialect"):
        validate_supported_dialects(manifest)


def test_validate_supported_dialects_rejects_mismatched_runtime_dialect() -> None:
    manifest = {
        "runtime": {
            "source": {
                "technology": "sql_server",
                "dialect": "oracle",
                "connection": {"database": "SourceDB"},
            }
        }
    }

    with pytest.raises(ValueError, match="runtime.source technology and dialect"):
        validate_supported_dialects(manifest)


def test_primary_dialect_uses_technology_mapping_when_top_level_dialect_missing() -> None:
    manifest = {
        "runtime": {
            "target": {
                "technology": "oracle",
                "dialect": "oracle",
                "connection": {"service": "TARGETPDB"},
            }
        },
        "technology": "oracle",
    }

    assert get_primary_dialect(manifest) == "oracle"


def test_set_runtime_role_scrubs_unsupported_existing_runtime_roles() -> None:
    manifest = {
        "runtime": {
            "target": {
                "technology": "duckdb",
                "dialect": "duckdb",
                "connection": {"path": ".runtime/target.duckdb"},
            }
        }
    }

    updated = set_runtime_role(
        manifest,
        "source",
        RuntimeRole(
            technology="sql_server",
            dialect="tsql",
            connection=RuntimeConnection(database="SourceDB"),
        ),
    )

    assert "target" not in updated["runtime"]
    assert updated["runtime"]["source"]["dialect"] == "tsql"


def test_sanitize_manifest_drops_unsupported_top_level_fields() -> None:
    manifest = {"technology": "duckdb", "dialect": "duckdb"}

    assert sanitize_manifest(manifest) == {}


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
                "technology": "sql_server",
                "dialect": "tsql",
                "connection": {"database": "TargetDB"},
                "schemas": {"source": "bronze"},
            },
        },
        "extraction": {"schemas": ["bronze"], "extracted_at": "2026-04-13T00:00:00Z"},
    }

    model = get_manifest_model(manifest)

    assert model.runtime is not None
    assert model.runtime.target is not None
    assert model.runtime.target.connection.database == "TargetDB"
