"""Tests for source catalog candidate classification."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared.generate_sources import generate_sources
from shared.output_models.generate_sources import GenerateSourcesOutput
from shared.generate_sources_support.candidates import (
    SourceCandidates,
    collect_source_candidates,
    validate_source_namespace,
)


def test_candidates_support_module_exports_classification_helpers() -> None:
    assert SourceCandidates
    assert callable(collect_source_candidates)
    assert callable(validate_source_namespace)


def test_collect_source_candidates_handles_missing_catalog_dir(tmp_path: Path) -> None:
    candidates = collect_source_candidates(tmp_path / "missing")

    assert candidates == SourceCandidates()


def test_collect_source_candidates_classifies_catalog_tables(tmp_path: Path) -> None:
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.source.json").write_text(
        '{"schema": "silver", "name": "Source", "is_source": true}',
        encoding="utf-8",
    )
    (tables_dir / "silver.model.json").write_text(
        '{"schema": "silver", "name": "Model", "scoping": {"status": "resolved"}}',
        encoding="utf-8",
    )
    (tables_dir / "silver.pending.json").write_text(
        '{"schema": "silver", "name": "Pending", "scoping": {"status": "no_writer_found"}}',
        encoding="utf-8",
    )
    (tables_dir / "silver.fresh.json").write_text(
        '{"schema": "silver", "name": "Fresh"}',
        encoding="utf-8",
    )

    candidates = collect_source_candidates(tables_dir)

    assert candidates.included == ["silver.source"]
    assert candidates.excluded == ["silver.model"]
    assert candidates.unconfirmed == ["silver.pending"]
    assert candidates.incomplete == ["silver.fresh"]


def test_validate_source_namespace_rejects_duplicate_table_names() -> None:
    candidates = SourceCandidates(
        source_tables=[
            {"schema": "bronze", "name": "Customer"},
            {"schema": "archive", "name": "Customer"},
        ],
        included=["bronze.customer", "archive.customer"],
    )

    result = validate_source_namespace(candidates)

    assert isinstance(result, GenerateSourcesOutput)
    assert result.error == "SOURCE_NAME_COLLISION"
    assert result.message is not None
    assert "bronze.customer" in result.message
    assert "archive.customer" in result.message


def test_collect_source_candidates_skips_invalid_json(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    tables_dir = tmp_path / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "broken.json").write_text("{", encoding="utf-8")

    candidates = collect_source_candidates(tables_dir)

    assert candidates == SourceCandidates()
    assert "reason=parse_error" in caplog.text


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


def test_seed_table_ignored_not_unconfirmed() -> None:
    """Seed tables are not treated as pending source decisions."""
    tmp, root = _make_project([
        {"schema": "silver", "name": "Lookup",
         "scoping": {"status": "no_writer_found"}, "is_seed": True},
    ])
    try:
        result = generate_sources(root)
        assert "silver.lookup" not in result.included
        assert "silver.lookup" not in result.unconfirmed
        assert "silver.lookup" not in result.incomplete
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


def test_sources_yml_rejects_duplicate_source_table_names_across_schemas() -> None:
    """The single bronze source namespace cannot represent duplicate table names."""
    tmp, root = _make_project([
        {"schema": "bronze", "name": "Customer",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
        {"schema": "archive", "name": "Customer",
         "scoping": {"status": "no_writer_found"}, "is_source": True},
    ])
    try:
        result = generate_sources(root)
        assert result.sources is None
        assert result.error == "SOURCE_NAME_COLLISION"
        assert result.message is not None
        assert "archive.customer" in result.message
        assert "bronze.customer" in result.message
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
