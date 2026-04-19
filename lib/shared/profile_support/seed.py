"""Seed profile helpers."""

from __future__ import annotations

from typing import Any


def build_seed_profile(rationale: str = "Table is maintained as a dbt seed.") -> dict[str, Any]:
    """Build the canonical profile payload for a dbt seed table."""
    return {
        "classification": {
            "resolved_kind": "seed",
            "source": "catalog",
            "rationale": rationale,
        },
        "warnings": [],
        "errors": [],
    }
