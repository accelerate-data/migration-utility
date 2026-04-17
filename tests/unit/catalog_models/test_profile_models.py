"""Tests for profile-related catalog models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.catalog_models import TableProfileSection, ViewProfileSection


def _valid_table_profile() -> dict[str, object]:
    return {
        "status": "ok",
        "classification": {
            "resolved_kind": "fact_transaction",
            "source": "llm",
            "rationale": "Append-only sales facts.",
        },
        "primary_key": {
            "columns": ["sale_id"],
            "primary_key_type": "surrogate",
            "source": "catalog",
            "rationale": "Declared primary key.",
        },
        "natural_key": {
            "columns": ["order_number"],
            "source": "catalog+llm",
            "rationale": "Business order identifier.",
        },
        "watermark": {
            "column": "load_date",
            "source": "llm",
            "rationale": "Writer filters by load date.",
        },
        "foreign_keys": [
            {
                "column": "customer_key",
                "fk_type": "standard",
                "references_source_relation": "silver.dimcustomer",
                "references_column": "customer_key",
                "source": "catalog",
                "rationale": "Declared customer FK.",
            }
        ],
        "pii_actions": [
            {
                "column": "customer_email",
                "suggested_action": "mask",
                "entity": "email",
                "source": "catalog",
                "rationale": "Catalog marks the column as email.",
            }
        ],
        "warnings": [{"code": "LOW_CONFIDENCE", "message": "Limited evidence.", "severity": "warning"}],
        "errors": [{"code": "PROFILE_FAILED", "message": "Profile failed.", "severity": "error"}],
    }


def test_table_profile_accepts_valid_nested_sections() -> None:
    section = TableProfileSection.model_validate(_valid_table_profile())

    assert section.classification is not None
    assert section.classification.resolved_kind == "fact_transaction"
    assert section.primary_key is not None
    assert section.primary_key.primary_key_type == "surrogate"
    assert section.natural_key is not None
    assert section.natural_key.columns == ["order_number"]
    assert section.watermark is not None
    assert section.watermark.column == "load_date"
    assert section.foreign_keys[0].fk_type == "standard"
    assert section.foreign_keys[0].references_source_relation == "silver.dimcustomer"
    assert section.pii_actions[0].suggested_action == "mask"
    assert section.pii_actions[0].entity == "email"
    assert section.warnings[0].severity == "warning"
    assert section.errors[0].severity == "error"


def test_table_profile_accepts_current_legacy_payload_values() -> None:
    section = TableProfileSection.model_validate({
        "status": "ok",
        "classification": {
            "resolved_kind": "fact_insert",
            "source": "manual",
            "confidence": "high",
            "rationale": "Existing eval fixture profile.",
        },
        "primary_key": {
            "column": "LegacyKey",
            "columns": [],
            "primary_key_type": "none",
            "source": "catalog",
        },
        "natural_key": ["EmployeeNaturalKey"],
        "watermark": {
            "columns": ["LastSeenDate"],
            "strategy": "timestamp",
            "watermark_type": "date_partitioned",
        },
        "foreign_keys": [
            {
                "columns": ["ProcedureKey"],
                "references_table": "silver.DimProcedure",
                "fk_type": "standard",
            }
        ],
        "pii_actions": [{"column": "EmailAddress", "action": "mask"}],
        "warnings": [{"code": "LOW_CONFIDENCE", "message": "Existing warning.", "severity": "medium"}],
    })

    assert section.classification is not None
    assert section.classification.resolved_kind == "fact_insert"
    assert section.classification.source == "manual"
    assert section.primary_key is not None
    assert section.primary_key.primary_key_type == "none"
    assert section.primary_key.column == "LegacyKey"
    assert section.natural_key is not None
    assert section.natural_key.columns == ["EmployeeNaturalKey"]
    assert section.watermark is not None
    assert section.watermark.columns == ["LastSeenDate"]
    assert section.foreign_keys[0].references_table == "silver.DimProcedure"
    assert section.pii_actions[0].action == "mask"
    assert section.warnings[0].severity == "medium"


@pytest.mark.parametrize(
    ("field_path", "value"),
    [
        (("classification", "resolved_kind"), "invalid_kind"),
        (("classification", "source"), "spreadsheet"),
        (("primary_key", "primary_key_type"), "business"),
        (("primary_key", "source"), "spreadsheet"),
        (("natural_key", "source"), "spreadsheet"),
        (("watermark", "source"), "spreadsheet"),
        (("foreign_keys", 0, "fk_type"), "snowflake"),
        (("foreign_keys", 0, "source"), "spreadsheet"),
        (("pii_actions", 0, "suggested_action"), "encrypt"),
        (("pii_actions", 0, "source"), "spreadsheet"),
    ],
)
def test_table_profile_rejects_invalid_enum_values(field_path: tuple[object, ...], value: str) -> None:
    data = _valid_table_profile()
    target = data
    for part in field_path[:-1]:
        target = target[part]  # type: ignore[index]
    target[field_path[-1]] = value  # type: ignore[index]

    with pytest.raises(ValidationError, match=str(field_path[-1])):
        TableProfileSection.model_validate(data)


def test_table_profile_rejects_unknown_nested_fields() -> None:
    data = _valid_table_profile()
    data["watermark"] = {
        "column": "load_date",
        "source": "llm",
        "rationale": "Writer filters by load date.",
        "confidence": 0.9,
    }

    with pytest.raises(ValidationError, match="extra_forbidden"):
        TableProfileSection.model_validate(data)


def test_view_profile_accepts_valid_contract() -> None:
    section = ViewProfileSection.model_validate({
        "status": "ok",
        "classification": "mart",
        "rationale": "Aggregates sales by month.",
        "source": "llm",
    })

    assert section.classification == "mart"
    assert section.source == "llm"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("classification", "dim_non_scd"),
        ("source", "catalog"),
    ],
)
def test_view_profile_rejects_invalid_enum_values(field: str, value: str) -> None:
    data = {
        "status": "ok",
        "classification": "stg",
        "rationale": "Single-source pass-through.",
        "source": "llm",
    }
    data[field] = value

    with pytest.raises(ValidationError, match=field):
        ViewProfileSection.model_validate(data)
