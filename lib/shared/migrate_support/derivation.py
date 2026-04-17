"""Materialization and schema-test derivation helpers for migrate."""

from __future__ import annotations

from typing import Any

from shared.name_resolver import model_name_from_table


def _field_value(value: Any, field: str, default: Any = None) -> Any:
    """Read a field from either a dict or a typed catalog model."""
    if isinstance(value, dict):
        return value.get(field, default)
    return getattr(value, field, default)


def _first_value(value: Any) -> Any:
    """Return the first list item, or the value itself for scalar values."""
    return value[0] if isinstance(value, list) and value else value


def derive_materialization(profile: Any) -> str:
    """Derive dbt materialization from profile classification and watermark."""
    classification = _field_value(profile, "classification") or {}
    if classification in ("stg", "mart"):
        return "view"
    if _field_value(classification, "resolved_kind") == "dim_scd2":
        return "snapshot"
    watermark = _field_value(profile, "watermark")
    watermark_column = _field_value(watermark, "column") or _first_value(_field_value(watermark, "columns"))
    if watermark and watermark_column:
        return "incremental"
    return "table"


def derive_schema_tests(profile: Any) -> dict[str, Any]:
    """Build dbt schema test specs from profile answers."""
    tests: dict[str, Any] = {}

    pk = _field_value(profile, "primary_key")
    pk_columns = _field_value(pk, "columns") or [_field_value(pk, "column")]
    pk_columns = [col for col in pk_columns if col]
    if pk and pk_columns:
        tests["entity_integrity"] = [
            {"column": col, "tests": ["unique", "not_null"]}
            for col in pk_columns
        ]

    fks = _field_value(profile, "foreign_keys") or []
    if fks:
        ri_tests = []
        for fk in fks:
            col = _field_value(fk, "column", "") or _first_value(_field_value(fk, "columns", []))
            ref_relation = (
                _field_value(fk, "references_source_relation", "")
                or _field_value(fk, "references_table", "")
            )
            ref_col = _field_value(fk, "references_column", "")
            if col and ref_relation:
                model_ref = f"ref('{model_name_from_table(ref_relation)}')"
                ri_tests.append({
                    "column": col,
                    "to": model_ref,
                    "field": ref_col or col,
                })
        if ri_tests:
            tests["referential_integrity"] = ri_tests

    watermark = _field_value(profile, "watermark")
    watermark_column = _field_value(watermark, "column") or _first_value(_field_value(watermark, "columns"))
    if watermark and watermark_column:
        col = watermark_column
        tests["recency"] = {"column": col}

    pii_actions = _field_value(profile, "pii_actions") or []
    if pii_actions:
        tests["pii"] = [
            {
                "column": _field_value(p, "column", ""),
                "suggested_action": (
                    _field_value(p, "suggested_action")
                    or _field_value(p, "action")
                    or "mask"
                ),
            }
            for p in pii_actions
            if _field_value(p, "column")
        ]

    return tests
