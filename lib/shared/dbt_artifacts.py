"""Shared helpers for dbt schema YAML artifacts."""

from __future__ import annotations

from typing import Any

import yaml


def schema_with_model_unit_tests(
    existing_text: str | None,
    *,
    model_name: str,
    unit_tests: list[dict[str, Any]],
) -> dict[str, Any]:
    """Return schema YAML with top-level unit tests replaced for one model."""
    existing = yaml.safe_load(existing_text) if existing_text else None
    schema: dict[str, Any] = existing if isinstance(existing, dict) else {"version": 2}
    if "version" not in schema:
        schema["version"] = 2

    models = schema.setdefault("models", [])
    if not isinstance(models, list):
        models = []
        schema["models"] = models

    model_entry = None
    for model in models:
        if isinstance(model, dict) and model.get("name") == model_name:
            model_entry = model
            break
    if model_entry is None:
        model_entry = {"name": model_name}
        models.append(model_entry)
    else:
        model_entry.pop("unit_tests", None)

    existing_unit_tests = schema.get("unit_tests", [])
    if not isinstance(existing_unit_tests, list):
        existing_unit_tests = []

    schema["unit_tests"] = [
        test
        for test in existing_unit_tests
        if not (isinstance(test, dict) and test.get("model") == model_name)
    ]
    schema["unit_tests"].extend(unit_tests)
    return schema


def dump_schema_yaml(data: dict[str, Any]) -> str:
    """Serialize schema YAML using the repo's dbt artifact formatting."""
    return yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
