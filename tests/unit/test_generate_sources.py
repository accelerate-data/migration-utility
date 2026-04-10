"""Tests for generate_sources.py — dbt sources.yml builder.

Tests import shared.generate_sources directly for fast, fixture-based execution.
"""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path

import pytest

from shared.generate_sources import generate_sources, write_sources_yml


def _make_project(tables: list[dict]) -> tuple[tempfile.TemporaryDirectory, Path]:
    """Create a temp project with the given table catalog entries."""
    tmp = tempfile.TemporaryDirectory()
    root = Path(tmp.name)
    tables_dir = root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (root / "manifest.json").write_text(
        json.dumps({"schema_version": "1.0", "technology": "sql_server"}), encoding="utf-8"
    )
    for table in tables:
        schema = table.get("schema", "silver").lower()
        name = table.get("name", "unknown")
        fqn = f"{schema}.{name.lower()}"
        path = tables_dir / f"{fqn}.json"
        path.write_text(json.dumps(table), encoding="utf-8")
    return tmp, root


# ── Core filter logic ─────────────────────────────────────────────────────────


def test_is_source_true_included() -> None:
    """Table with is_source: true appears in included list and sources YAML."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Lookup",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.lookup" in result.included
        assert result.sources is not None
    finally:
        tmp.cleanup()


def test_no_writer_found_without_flag_goes_to_unconfirmed() -> None:
    """no_writer_found table without is_source goes to unconfirmed, not included."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Audit",
         "scoping": {"status": "no_writer_found"}},
    ])
    try:
        result = generate_sources(root)
        assert "silver.audit" not in result.included
        assert "silver.audit" in result.unconfirmed
        assert result.sources is None
    finally:
        tmp.cleanup()


def test_resolved_table_excluded() -> None:
    """Resolved table (has writer) goes to excluded list."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "DimCustomer",
         "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load"}},
    ])
    try:
        result = generate_sources(root)
        assert "silver.dimcustomer" in result.excluded
        assert "silver.dimcustomer" not in result.included
    finally:
        tmp.cleanup()


def test_resolved_with_is_source_included() -> None:
    """Resolved table marked is_source: true is included (cross-domain scenario)."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "CrossDomain",
         "scoping": {"status": "resolved", "selected_writer": "dbo.usp_other"},
         "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.crossdomain" in result.included
        assert "silver.crossdomain" not in result.excluded
    finally:
        tmp.cleanup()


def test_unscoped_table_goes_to_incomplete() -> None:
    """Table with no scoping goes to incomplete list."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Fresh"},
    ])
    try:
        result = generate_sources(root)
        assert "silver.fresh" in result.incomplete
    finally:
        tmp.cleanup()


def test_unconfirmed_list_populated() -> None:
    """Multiple no_writer_found tables without is_source all land in unconfirmed."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Audit", "scoping": {"status": "no_writer_found"}},
        {"schema": "silver", "name": "Lookup", "scoping": {"status": "no_writer_found"}},
    ])
    try:
        result = generate_sources(root)
        assert set(result.unconfirmed) == {"silver.audit", "silver.lookup"}
        assert result.included == []
    finally:
        tmp.cleanup()


def test_empty_catalog() -> None:
    """Empty catalog returns all empty lists and None sources."""
    tmp, root = _make_project([])
    try:
        result = generate_sources(root)
        assert result.sources is None
        assert result.included == []
        assert result.excluded == []
        assert result.unconfirmed == []
        assert result.incomplete == []
    finally:
        tmp.cleanup()


def test_mixed_tables() -> None:
    """Mix of is_source, resolved, no_writer_found, and unscoped are classified correctly."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Src",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "silver", "name": "Model",
         "scoping": {"status": "resolved", "selected_writer": "dbo.usp_load"}},
        {"schema": "silver", "name": "Pending",
         "scoping": {"status": "no_writer_found"}},
        {"schema": "silver", "name": "Fresh"},
    ])
    try:
        result = generate_sources(root)
        assert result.included == ["silver.src"]
        assert result.excluded == ["silver.model"]
        assert result.unconfirmed == ["silver.pending"]
        assert result.incomplete == ["silver.fresh"]
    finally:
        tmp.cleanup()


# ── --strict flag ─────────────────────────────────────────────────────────────


def test_strict_mode_passes_when_no_incomplete() -> None:
    """--strict does not trigger when all tables are analyzed."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Src",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert result.incomplete == []
    finally:
        tmp.cleanup()


def test_strict_mode_flags_incomplete_scoping() -> None:
    """incomplete list is non-empty for unscoped tables (strict should exit 1)."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Fresh"},
    ])
    try:
        result = generate_sources(root)
        assert "silver.fresh" in result.incomplete
    finally:
        tmp.cleanup()


def test_strict_mode_does_not_flag_unconfirmed() -> None:
    """unconfirmed tables are not in incomplete — strict mode doesn't block them."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Pending", "scoping": {"status": "no_writer_found"}},
    ])
    try:
        result = generate_sources(root)
        assert result.incomplete == []
        assert "silver.pending" in result.unconfirmed
    finally:
        tmp.cleanup()


# ── sources.yml content ───────────────────────────────────────────────────────


def test_sources_yml_groups_by_schema() -> None:
    """Multiple is_source tables from same schema are grouped together."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "TableA",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "silver", "name": "TableB",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "bronze", "name": "TableC",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert result.sources is not None
        schemas = {s["name"] for s in result.sources["sources"]}
        assert schemas == {"silver", "bronze"}
    finally:
        tmp.cleanup()


def test_excluded_table_with_is_source_not_in_sources() -> None:
    """Table with both excluded: true and is_source: true must NOT appear in sources.yml."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Ghost",
         "scoping": {"status": "no_writer_found"},
         "is_source": True, "excluded": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.ghost" not in result.included
    finally:
        tmp.cleanup()


def test_write_sources_yml_creates_file(tmp_path) -> None:
    """write_sources_yml writes the file and returns the path."""
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.src.json").write_text(
        json.dumps({
            "schema": "silver", "name": "Src",
            "scoping": {"status": "no_writer_found"}, "is_source": True,
        }),
        encoding="utf-8",
    )
    dbt_dir = tmp_path / "dbt"
    (dbt_dir / "models" / "staging").mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=tmp_path, capture_output=True, check=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "i"],
        cwd=tmp_path, capture_output=True, check=True,
        env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
             "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
             "HOME": str(Path.home())},
    )
    result = write_sources_yml(tmp_path)
    assert result.path is not None
    sources_path = Path(result.path)
    assert sources_path.exists()
    assert "silver.src" in result.included
