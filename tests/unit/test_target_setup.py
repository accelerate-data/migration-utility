"""Tests for target-setup orchestration helpers."""

from __future__ import annotations

import json
from pathlib import Path

from shared.target_setup import (
    generate_target_sources,
    get_target_source_schema,
    write_target_sources_yml,
)


def _make_project(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    (project_root / "catalog" / "tables").mkdir(parents=True)
    (project_root / "dbt" / "models" / "staging").mkdir(parents=True)
    (project_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "technology": "sql_server",
                "dialect": "tsql",
                "runtime": {
                    "source": {
                        "technology": "sql_server",
                        "dialect": "tsql",
                        "connection": {"database": "SourceDB"},
                    },
                    "target": {
                        "technology": "sql_server",
                        "dialect": "tsql",
                        "connection": {"database": "TargetDB"},
                        "schemas": {"source": "bronze"},
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    (project_root / "catalog" / "tables" / "silver.customer.json").write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "Customer",
                "scoping": {"status": "no_writer_found"},
                "is_source": True,
            }
        ),
        encoding="utf-8",
    )
    return project_root


def test_get_target_source_schema_defaults_to_bronze(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server", "dialect": "tsql"}),
        encoding="utf-8",
    )
    assert get_target_source_schema(project_root) == "bronze"


def test_generate_target_sources_uses_target_schema_override(tmp_path: Path) -> None:
    project_root = _make_project(tmp_path)
    result = generate_target_sources(project_root)
    assert result.sources is not None
    assert result.sources["sources"][0]["name"] == "silver"
    assert result.sources["sources"][0]["schema"] == "bronze"


def test_write_target_sources_yml_writes_remapped_schema(tmp_path: Path) -> None:
    project_root = _make_project(tmp_path)
    result = write_target_sources_yml(project_root)
    assert result.path is not None
    contents = Path(result.path).read_text(encoding="utf-8")
    assert "schema: bronze" in contents
    assert "name: silver" in contents
