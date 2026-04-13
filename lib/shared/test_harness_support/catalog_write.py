"""Catalog write helpers for test_harness."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from shared.catalog import load_and_merge_catalog
from shared.catalog_models import TestGenSection
from shared.name_resolver import normalize

from .manifest import _validate_test_spec

logger = logging.getLogger(__name__)


def run_write_test_gen(
    project_root: Path,
    table_fqn: str,
    branches: int,
    unit_tests: int,
    coverage: str,
    warnings: list[dict[str, Any]] | None = None,
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Validate test-gen output and write test_gen section to catalog."""
    norm = normalize(table_fqn)
    spec_path = project_root / "test-specs" / f"{norm}.json"
    if not spec_path.exists():
        raise ValueError(
            f"Test spec file not found: {spec_path}. "
            f"Write test-specs/{norm}.json before calling test-harness write."
        )

    try:
        spec_data = json.loads(spec_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Test spec file is not valid JSON: {spec_path}. Parse error: {exc}"
        ) from exc

    _validate_test_spec(spec_data)

    status = "ok" if branches > 0 else "error"
    test_gen: dict[str, Any] = {
        "status": status,
        "test_spec_path": f"test-specs/{norm}.json",
        "branches": branches,
        "unit_tests": unit_tests,
        "coverage": coverage,
        "warnings": warnings or [],
        "errors": errors or [],
    }
    TestGenSection.model_validate(test_gen)

    result = load_and_merge_catalog(project_root, norm, "test_gen", test_gen)
    logger.info("event=write_test_gen_complete table=%s status=%s", norm, status)
    return result
