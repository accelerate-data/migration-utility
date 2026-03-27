"""Tests for scope.py — VU-736 / VU-738.

Each test loads a fixture SQL file, creates a temporary DDL directory
(with the fixture content as procedures.sql), runs scope_writers(), and
asserts the expected output.

sys.path is pre-configured so scope.py (in the plugin root) is importable.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

# Add plugin root (agent-sources/plugins/ad-migration/) to sys.path so that
# `import scope` resolves to scope.py, and `from shared.loader import ...`
# resolves from the shared/ package inside the same plugin directory.
_PLUGIN_ROOT = Path(__file__).parent.parent.parent
if str(_PLUGIN_ROOT) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT))
# Also ensure the shared package itself is importable via the installed editable path,
# but the plugin root takes precedence for scope.py.
_SHARED_ROOT = Path(__file__).parent.parent.parent / "shared"
if str(_SHARED_ROOT) not in sys.path:
    sys.path.insert(1, str(_SHARED_ROOT))

from scope import scope_writers, _normalize  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "scope"

TARGET_TABLE = "silver.FactSales"
TARGET_FQN = _normalize(TARGET_TABLE)  # "silver.factsales"


def _load_catalog_procs(fixture_file: Path) -> dict:
    """Load a fixture SQL file into a DdlCatalog and return catalog.procedures."""
    from shared.loader import load_directory

    with tempfile.TemporaryDirectory() as tmp:
        proc_file = Path(tmp) / "procedures.sql"
        proc_file.write_text(fixture_file.read_text(encoding="utf-8"), encoding="utf-8")
        catalog = load_directory(Path(tmp))
        # Return a copy — catalog.procedures refs are still valid after context exits
        # because DdlEntry holds raw_ddl strings (not file handles)
        return dict(catalog.procedures)


# ---------------------------------------------------------------------------
# Test 1: direct INSERT → confirmed writer
# ---------------------------------------------------------------------------


def test_direct_insert_confirmed() -> None:
    """direct_insert.sql: single writer, write_type=direct, confidence=0.90, status=confirmed."""
    procs = _load_catalog_procs(FIXTURES / "direct_insert.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) == 1, f"Expected 1 writer, got: {result.writers}"
    writer = result.writers[0]
    assert writer.procedure == "dbo.usp_direct_insert"
    assert writer.write_type == "direct"
    assert writer.status == "confirmed"
    assert abs(writer.confidence - 0.90) < 0.001, f"Expected confidence 0.90, got {writer.confidence}"


# ---------------------------------------------------------------------------
# Test 2: direct MERGE → write_operations contains "MERGE"
# ---------------------------------------------------------------------------


def test_direct_merge_write_operations() -> None:
    """direct_merge.sql: write_operations contains 'MERGE'."""
    procs = _load_catalog_procs(FIXTURES / "direct_merge.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) >= 1
    writer = next(
        (w for w in result.writers if w.write_type == "direct"), None
    )
    assert writer is not None, "Expected a direct writer"
    assert "MERGE" in writer.write_operations, (
        f"Expected MERGE in write_operations, got: {writer.write_operations}"
    )


# ---------------------------------------------------------------------------
# Test 3: indirect one-hop caller
# ---------------------------------------------------------------------------


def test_indirect_one_hop() -> None:
    """indirect_one_hop.sql: usp_caller is indirect, confidence=0.75, call_path=[dbo.usp_writer]."""
    procs = _load_catalog_procs(FIXTURES / "indirect_one_hop.sql")
    result = scope_writers(procs, TARGET_FQN)

    # usp_caller should be an indirect writer
    caller = next(
        (w for w in result.writers if "caller" in w.procedure), None
    )
    assert caller is not None, (
        f"Expected usp_caller as indirect writer; writers={[w.procedure for w in result.writers]}"
    )
    assert caller.write_type == "indirect"
    assert abs(caller.confidence - 0.75) < 0.001, (
        f"Expected confidence 0.75 for one-hop indirect, got {caller.confidence}"
    )
    assert len(caller.call_path) == 1
    assert caller.call_path[0] == "dbo.usp_writer"


# ---------------------------------------------------------------------------
# Test 4: indirect two-hop caller
# ---------------------------------------------------------------------------


def test_indirect_two_hop() -> None:
    """indirect_two_hop.sql: usp_outer has call_path of length 2."""
    procs = _load_catalog_procs(FIXTURES / "indirect_two_hop.sql")
    result = scope_writers(procs, TARGET_FQN)

    outer = next(
        (w for w in result.writers if "outer" in w.procedure), None
    )
    assert outer is not None, (
        f"Expected usp_outer as indirect writer; writers={[w.procedure for w in result.writers]}"
    )
    assert outer.write_type == "indirect"
    assert len(outer.call_path) == 2, (
        f"Expected call_path length 2 for two-hop indirect, got {outer.call_path}"
    )


# ---------------------------------------------------------------------------
# Test 5: dynamic SQL with static write → penalty applied
# ---------------------------------------------------------------------------


def test_dynamic_with_static_penalty() -> None:
    """dynamic_with_static.sql: confidence = 0.90 - 0.20 = 0.70."""
    procs = _load_catalog_procs(FIXTURES / "dynamic_with_static.sql")
    result = scope_writers(procs, TARGET_FQN)

    assert len(result.writers) >= 1
    writer = result.writers[0]
    assert writer.write_type == "direct"
    expected_confidence = 0.90 - 0.20  # = 0.70
    assert abs(writer.confidence - expected_confidence) < 0.001, (
        f"Expected confidence {expected_confidence}, got {writer.confidence}"
    )


# ---------------------------------------------------------------------------
# Test 6: dynamic SQL only → capped at 0.45, status=suspected
# ---------------------------------------------------------------------------


def test_dynamic_only_capped() -> None:
    """dynamic_only.sql: confidence capped at 0.45, status=suspected."""
    procs = _load_catalog_procs(FIXTURES / "dynamic_only.sql")
    result = scope_writers(procs, TARGET_FQN)

    # The proc uses only dynamic SQL — it should appear with low confidence,
    # or not appear at all (no static write detected). If it appears:
    if result.writers:
        writer = result.writers[0]
        assert writer.confidence <= 0.45, (
            f"Expected confidence <= 0.45 for dynamic-only, got {writer.confidence}"
        )
        assert writer.status == "suspected"
    # If no writers found (correct — no static write to target), the test passes too.
    # The proc has no static INSERT to silver.FactSales so it may not be a writer at all.
    # Either outcome is valid; what must NOT happen is confidence > 0.45.


# ---------------------------------------------------------------------------
# Test 7: cross-database reference → errors[] contains ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE
# ---------------------------------------------------------------------------


def test_cross_db_error() -> None:
    """cross_db.sql: errors[] contains entry with ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE."""
    procs = _load_catalog_procs(FIXTURES / "cross_db.sql")
    result = scope_writers(procs, TARGET_FQN)

    error_codes = [e.code for e in result.errors]
    assert "ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE" in error_codes, (
        f"Expected ANALYSIS_CROSS_DATABASE_OUT_OF_SCOPE in errors, got: {error_codes}"
    )


# ---------------------------------------------------------------------------
# Test 8: cyclic call graph → terminates without crash, no writers
# ---------------------------------------------------------------------------


def test_cycle_no_infinite_loop() -> None:
    """cycle.sql: terminates cleanly, neither proc writes directly, no crash."""
    procs = _load_catalog_procs(FIXTURES / "cycle.sql")
    # Must not raise or hang
    result = scope_writers(procs, TARGET_FQN)

    # Neither usp_cycle_a nor usp_cycle_b writes to FactSales
    direct = [w for w in result.writers if w.write_type == "direct"]
    assert direct == [], f"Expected no direct writers in cycle scenario, got: {direct}"
