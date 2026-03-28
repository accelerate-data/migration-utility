"""Tests for scope.py — AST-only writer detection and confidence scoring.

Each test loads a fixture SQL file, creates a temporary DDL directory
(with the fixture content as procedures.sql), runs scope_writers(), and
asserts the expected output.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from shared.name_resolver import normalize
from shared.scope import scope_writers

FIXTURES = Path(__file__).parent / "fixtures" / "scope"

TARGET_TABLE = "silver.FactSales"
TARGET_FQN = normalize(TARGET_TABLE)  # "silver.factsales"


def _load_catalog_procs(fixture_file: Path) -> dict:
    """Load a fixture SQL file into a DdlCatalog and return catalog.procedures."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        proc_file = Path(tmp) / "procedures.sql"
        proc_file.write_text(fixture_file.read_text(encoding="utf-8"), encoding="utf-8")
        catalog = load_directory(Path(tmp))
        return dict(catalog.procedures)


# ---------------------------------------------------------------------------
# Positive: direct INSERT → confirmed writer
# ---------------------------------------------------------------------------


def test_direct_insert_confirmed() -> None:
    procs = _load_catalog_procs(FIXTURES / "direct_insert.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) == 1
    writer = result.writers[0]
    assert writer.procedure_name == "dbo.usp_direct_insert"
    assert writer.write_type == "direct"
    assert "INSERT" in writer.write_operations
    assert writer.call_path == ["dbo.usp_direct_insert"]
    assert writer.status == "confirmed"
    assert abs(writer.confidence - 0.90) < 0.001


# ---------------------------------------------------------------------------
# Positive: direct MERGE → write_operations contains "MERGE"
# ---------------------------------------------------------------------------


def test_direct_merge_write_operations() -> None:
    procs = _load_catalog_procs(FIXTURES / "direct_merge.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) >= 1
    writer = next(w for w in result.writers if w.write_type == "direct")
    assert "MERGE" in writer.write_operations


# ---------------------------------------------------------------------------
# Positive: indirect one-hop — direct writer detected
# ---------------------------------------------------------------------------


def test_indirect_one_hop_direct_writer_found() -> None:
    """The leaf writer proc with direct INSERT is detected.
    The caller proc uses EXEC (invisible to AST) so it will either
    appear as indirect (if call graph resolves) or be excluded."""
    procs = _load_catalog_procs(FIXTURES / "indirect_one_hop.sql")
    result = scope_writers(procs, TARGET_FQN)

    writer = next(
        (w for w in result.writers if "writer" in w.procedure_name and w.write_type == "direct"),
        None,
    )
    assert writer is not None, (
        f"Expected usp_writer as direct writer; writers={[w.procedure_name for w in result.writers]}"
    )


# ---------------------------------------------------------------------------
# Positive: indirect two-hop — leaf writer detected
# ---------------------------------------------------------------------------


def test_indirect_two_hop_leaf_writer_found() -> None:
    procs = _load_catalog_procs(FIXTURES / "indirect_two_hop.sql")
    result = scope_writers(procs, TARGET_FQN)

    leaf = next(
        (w for w in result.writers if "leaf" in w.procedure_name or "writer" in w.procedure_name),
        None,
    )
    assert leaf is not None, (
        f"Expected leaf writer; writers={[w.procedure_name for w in result.writers]}"
    )
    assert leaf.write_type == "direct"


# ---------------------------------------------------------------------------
# Positive: cross-database reference → ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE
# ---------------------------------------------------------------------------


def test_cross_db_error() -> None:
    procs = _load_catalog_procs(FIXTURES / "cross_db.sql")
    result = scope_writers(procs, TARGET_FQN)

    error_codes = [e.code for e in result.errors]
    assert "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE" in error_codes


# ---------------------------------------------------------------------------
# Positive: cyclic call graph → terminates without crash
# ---------------------------------------------------------------------------


def test_cycle_no_infinite_loop() -> None:
    procs = _load_catalog_procs(FIXTURES / "cycle.sql")
    result = scope_writers(procs, TARGET_FQN)

    direct = [w for w in result.writers if w.write_type == "direct"]
    assert direct == []


# ---------------------------------------------------------------------------
# Positive: unparseable proc (dynamic SQL) → PARSE_FAILED error
# ---------------------------------------------------------------------------


def test_parse_failed_reported() -> None:
    """A proc with no AST (failed to parse) → PARSE_FAILED in errors, not a writer."""
    from shared.loader import DdlEntry

    # Simulate a proc that load_directory couldn't parse (ast=None, parse_error set)
    procs = {
        "dbo.usp_unparseable": DdlEntry(
            raw_ddl="CREATE PROCEDURE dbo.usp_unparseable AS BEGIN TRY SELECT 1 END TRY BEGIN CATCH SELECT 2 END CATCH",
            ast=None,
            parse_error="sqlglot could not parse DDL block (fell back to Command)",
        ),
    }
    result = scope_writers(procs, TARGET_FQN)

    error_codes = [e.code for e in result.errors]
    assert "PARSE_FAILED" in error_codes
    assert len(result.writers) == 0


# ---------------------------------------------------------------------------
# Positive: dynamic SQL only → PARSE_FAILED, not a writer
# ---------------------------------------------------------------------------


def test_dynamic_only_parse_failed() -> None:
    procs = _load_catalog_procs(FIXTURES / "dynamic_only.sql")
    result = scope_writers(procs, TARGET_FQN)

    error_codes = [e.code for e in result.errors]
    assert "PARSE_FAILED" in error_codes or len(result.writers) == 0


# ---------------------------------------------------------------------------
# Negative: proc writes to unrelated table → not a writer
# ---------------------------------------------------------------------------


def test_no_writer_for_unrelated_table() -> None:
    procs = _load_catalog_procs(FIXTURES / "unrelated_table.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) == 0
    assert len(result.errors) == 0


# ---------------------------------------------------------------------------
# Negative: target table in SQL comment → not a writer (AST ignores comments)
# ---------------------------------------------------------------------------


def test_table_in_comment_not_detected() -> None:
    procs = _load_catalog_procs(FIXTURES / "table_in_comment.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) == 0


# ---------------------------------------------------------------------------
# Negative: target table in string literal → not a writer
# ---------------------------------------------------------------------------


def test_table_in_string_literal_not_detected() -> None:
    procs = _load_catalog_procs(FIXTURES / "table_in_string.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) == 0
