"""Review validation helpers for test_harness."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from shared.output_models import TestReviewOutput


def run_validate_review(review_file: Path) -> dict[str, bool]:
    """Validate a test review JSON file via Pydantic."""
    if not review_file.exists():
        raise ValueError(f"Review file not found: {review_file}")

    try:
        review_data = json.loads(review_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Review file is not valid JSON: {review_file}. Parse error: {exc}") from exc

    try:
        TestReviewOutput.model_validate(review_data)
    except ValidationError as exc:
        raise ValueError(f"Review validation failed: {exc}") from exc

    return {"valid": True}
