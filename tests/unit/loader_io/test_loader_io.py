"""Tests for loader_io manifest handling."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.loader_io import read_manifest


def test_read_manifest_defaults_dialect_when_missing(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps({"technology": "sql_server"}),
        encoding="utf-8",
    )

    manifest = read_manifest(tmp_path)

    assert manifest["dialect"] == "tsql"


def test_read_manifest_rejects_unsupported_top_level_technology(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps({"technology": "duckdb", "dialect": "duckdb"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="supported runtime technology"):
        read_manifest(tmp_path)


def test_read_manifest_rejects_unsupported_runtime_technology(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "runtime": {
                    "source": {
                        "technology": "duckdb",
                        "dialect": "duckdb",
                        "connection": {"path": ".runtime/source.duckdb"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="supported runtime technology"):
        read_manifest(tmp_path)


def test_read_manifest_rejects_unsupported_top_level_dialect(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps({"technology": "sql_server", "dialect": "duckdb"}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="supported runtime dialect"):
        read_manifest(tmp_path)


def test_read_manifest_rejects_mismatched_runtime_dialect(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "runtime": {
                    "source": {
                        "technology": "sql_server",
                        "dialect": "oracle",
                        "connection": {"database": "SourceDB"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="runtime.source technology and dialect"):
        read_manifest(tmp_path)
