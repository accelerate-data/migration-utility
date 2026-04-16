"""Regression tests for shared.output_models writeback exports."""

from __future__ import annotations

from shared.output_models import WriteSeedOutput


def test_package_level_write_seed_output_export() -> None:
    """WriteSeedOutput is available from the package-level lazy export barrel."""
    model = WriteSeedOutput(written="catalog/tables/silver.lookup.json", is_seed=True, status="ok")
    assert model.is_seed is True
