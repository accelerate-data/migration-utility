"""Tests for profile catalog model support module exports."""

from __future__ import annotations


def test_profile_support_exports_profile_models_and_aliases() -> None:
    from shared.catalog_model_support.profile import (
        ProfileClassification,
        ProfileForeignKey,
        ProfileNaturalKey,
        ProfilePiiAction,
        ProfilePrimaryKey,
        ProfileWatermark,
        TableProfileSection,
        ViewProfileSection,
    )

    table_profile = TableProfileSection.model_validate({
        "status": "ok",
        "classification": {
            "resolved_kind": "fact_insert",
            "source": "manual",
        },
        "primary_key": {"column": "LegacyKey", "columns": []},
        "natural_key": ["BusinessKey"],
        "watermark": {"columns": ["UpdatedAt"]},
        "foreign_keys": [{"columns": ["DimKey"], "references_table": "silver.dim"}],
        "pii_actions": [{"column": "Email", "action": "mask"}],
    })
    view_profile = ViewProfileSection.model_validate({
        "status": "ok",
        "classification": "mart",
        "rationale": "Aggregated view.",
        "source": "llm",
    })

    assert ProfileClassification().__class__.__name__ == "ProfileClassification"
    assert ProfilePrimaryKey(column="id").columns == ["id"]
    assert ProfileNaturalKey(columns=["id"]).columns == ["id"]
    assert ProfileWatermark(columns=["UpdatedAt"]).column == "UpdatedAt"
    assert (
        ProfileForeignKey(
            columns=["DimKey"],
            references_table="silver.dim",
        ).references_source_relation
        == "silver.dim"
    )
    assert ProfilePiiAction(column="Email", action="mask").suggested_action == "mask"
    assert table_profile.natural_key.columns == ["BusinessKey"]
    assert view_profile.classification == "mart"
