from __future__ import annotations

import json

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from shared import profile
from tests.unit.profile.helpers import _VALID_VIEW_PROFILE, _make_writable_copy

_cli_runner = CliRunner()


def test_write_view_profile_stg() -> None:
    """Valid stg profile is merged into view catalog."""
    tmp, root = _make_writable_copy()
    try:
        result = profile.run_write(root, "silver.vw_Simple", {**_VALID_VIEW_PROFILE})
        assert result["ok"] is True
        assert "views" in result["catalog_path"]
        written = json.loads((root / "catalog" / "views" / "silver.vw_simple.json").read_text(encoding="utf-8"))
        assert written["profile"]["classification"] == "stg"
        assert written["profile"]["status"] == "ok"
    finally:
        tmp.cleanup()

def test_write_view_profile_mart() -> None:
    """Valid mart profile is merged into view catalog."""
    tmp, root = _make_writable_copy()
    try:
        mart_profile = {**_VALID_VIEW_PROFILE, "classification": "mart"}
        result = profile.run_write(root, "silver.vw_Simple", mart_profile)
        assert result["ok"] is True
        written = json.loads((root / "catalog" / "views" / "silver.vw_simple.json").read_text(encoding="utf-8"))
        assert written["profile"]["classification"] == "mart"
    finally:
        tmp.cleanup()

def test_write_view_profile_bad_classification_raises() -> None:
    """Invalid classification raises model validation error."""
    tmp, root = _make_writable_copy()
    try:
        bad = {**_VALID_VIEW_PROFILE, "classification": "dim_non_scd"}
        with pytest.raises(ValidationError, match="classification"):
            profile.run_write(root, "silver.vw_Simple", bad)
    finally:
        tmp.cleanup()

def test_write_view_profile_missing_field_raises() -> None:
    """Missing required field raises model validation error."""
    tmp, root = _make_writable_copy()
    try:
        bad = {"classification": "stg", "rationale": "Single-source pass-through."}
        with pytest.raises(ValidationError, match="source"):
            profile.run_write(root, "silver.vw_Simple", bad)
    finally:
        tmp.cleanup()

def test_write_view_profile_idempotent() -> None:
    """Writing the same profile twice leaves the catalog consistent."""
    tmp, root = _make_writable_copy()
    try:
        profile.run_write(root, "silver.vw_Simple", {**_VALID_VIEW_PROFILE})
        profile.run_write(root, "silver.vw_Simple", {**_VALID_VIEW_PROFILE})
        written = json.loads((root / "catalog" / "views" / "silver.vw_simple.json").read_text(encoding="utf-8"))
        assert written["profile"]["classification"] == "stg"
    finally:
        tmp.cleanup()
