"""Materialization and schema-test derivation helpers for migrate."""

from __future__ import annotations

from typing import Any

from shared.name_resolver import model_name_from_table


def _field_value(value: Any, field: str, default: Any = None) -> Any:
    """Read a field from either a dict or a typed catalog model."""
    if isinstance(value, dict):
        return value.get(field, default)
    return getattr(value, field, default)


def derive_materialization(profile: Any) -> str:
    """Derive dbt materialization from profile classification and watermark."""
    classification = _field_value(profile, "classification") or {}
    if classification in ("stg", "mart"):
        return "view"
    if _field_value(classification, "resolved_kind") == "dim_scd2":
        return "snapshot"
    watermark = _field_value(profile, "watermark")
    if watermark and _field_value(watermark, "column"):
        return "incremental"
    return "table"


def derive_schema_tests(profile: Any) -> dict[str, Any]:
    """Build dbt schema test specs from profile answers."""
    _get = profile.get if isinstance(profile, dict) else lambda k, d=None: getattr(profile, k, d)
    tests: dict[str, Any] = {}

    pk = _get("primary_key")
    if pk and (pk.get("columns") if isinstance(pk, dict) else getattr(pk, "columns", None)):
        cols = pk.get("columns") if isinstance(pk, dict) else pk.columns
        tests["entity_integrity"] = [
            {"column": col, "tests": ["unique", "not_null"]}
            for col in cols
        ]

    fks = _get("foreign_keys") or []
    if fks:
        ri_tests = []
        for fk in fks:
            col = fk.get("column", "") if isinstance(fk, dict) else getattr(fk, "column", "")
            ref_relation = fk.get("references_source_relation", "") if isinstance(fk, dict) else getattr(fk, "references_source_relation", "")
            ref_col = fk.get("references_column", "") if isinstance(fk, dict) else getattr(fk, "references_column", "")
            if col and ref_relation:
                model_ref = f"ref('{model_name_from_table(ref_relation)}')"
                ri_tests.append({
                    "column": col,
                    "to": model_ref,
                    "field": ref_col or col,
                })
        if ri_tests:
            tests["referential_integrity"] = ri_tests

    watermark = _get("watermark")
    if watermark and (watermark.get("column") if isinstance(watermark, dict) else getattr(watermark, "column", None)):
        col = watermark.get("column") if isinstance(watermark, dict) else watermark.column
        tests["recency"] = {"column": col}

    pii_actions = _get("pii_actions") or []
    if pii_actions:
        tests["pii"] = [
            {
                "column": p.get("column", "") if isinstance(p, dict) else getattr(p, "column", ""),
                "suggested_action": p.get("suggested_action", "mask") if isinstance(p, dict) else getattr(p, "suggested_action", "mask"),
            }
            for p in pii_actions
            if (p.get("column") if isinstance(p, dict) else getattr(p, "column", None))
        ]

    return tests
