"""Facade tests for generate_sources.py."""

from __future__ import annotations

import json
from pathlib import Path

import shared.generate_sources as generate_sources_module
from shared.generate_sources import write_sources_yml


def test_generate_sources_facade_exports_public_api() -> None:
    """generate_sources remains the public facade for callers."""
    assert callable(generate_sources_module.generate_sources)
    assert callable(generate_sources_module.write_sources_yml)
    assert callable(generate_sources_module.list_confirmed_source_tables)


def test_write_sources_yml_facade_writes_file_and_returns_path(tmp_path: Path) -> None:
    """write_sources_yml facade writes source YAML and returns the public result."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.src.json").write_text(
        json.dumps({
            "schema": "silver", "name": "Src",
            "scoping": {"status": "no_writer_found"}, "is_source": True,
        }),
        encoding="utf-8",
    )
    (tmp_path / "dbt" / "models" / "staging").mkdir(parents=True)

    result = write_sources_yml(tmp_path)

    assert result.path is not None
    sources_path = Path(result.path)
    assert sources_path.exists()
    assert sources_path.name == "_staging__sources.yml"
    assert "silver.src" in result.included
