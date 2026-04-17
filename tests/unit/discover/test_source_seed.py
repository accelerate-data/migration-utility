from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import (
    CatalogFileMissingError,
)
from tests.unit.discover.discover_test_helpers import (
    _make_table_cat,
)

def test_write_source_sets_flag() -> None:
    """run_write_source sets is_source: true on a no_writer_found table."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(root, "silver.lookup", {"status": "no_writer_found"})
        result = discover.run_write_source(root, "silver.lookup", True)
        assert result.is_source is True
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert written["is_source"] is True

def test_write_source_resolved_table() -> None:
    """run_write_source accepts resolved tables (cross-domain source scenario)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root, "silver.crossdomain",
            {"status": "resolved", "selected_writer": "dbo.usp_other"},
        )
        discover.run_write_source(root, "silver.crossdomain", True)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert written["is_source"] is True

def test_write_source_false_resets_flag() -> None:
    """run_write_source with value=False writes is_source: false (always present)."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root, "silver.audit", {"status": "no_writer_found"}, {"is_source": True}
        )
        result = discover.run_write_source(root, "silver.audit", False)
        assert result.is_source is False
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert written["is_source"] is False

def test_write_source_clears_seed_flag() -> None:
    """run_write_source clears is_seed when marking a table as source."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root,
            "silver.lookup",
            {"status": "no_writer_found"},
            {"is_seed": True, "is_source": False},
        )
        result = discover.run_write_source(root, "silver.lookup", True)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result.is_source is True
        assert written["is_source"] is True
        assert written["is_seed"] is False

def test_write_seed_sets_seed_and_clears_source_with_profile() -> None:
    """run_write_seed marks a table as seed and persists seed profile semantics."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root,
            "silver.lookup",
            {"status": "no_writer_found"},
            {"is_source": True},
        )
        result = discover.run_write_seed(root, "silver.lookup", True)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result.is_seed is True
        assert written["is_seed"] is True
        assert written["is_source"] is False
        assert written["profile"]["status"] == "ok"
        assert written["profile"]["classification"]["resolved_kind"] == "seed"
        assert written["profile"]["classification"]["source"] == "catalog"

def test_write_seed_rejects_resolved_migration_table() -> None:
    """run_write_seed only marks writerless catalog tables as seeds."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _make_table_cat(
            root,
            "silver.dimcustomer",
            {"status": "resolved", "selected_writer": "silver.usp_load_dimcustomer"},
            {"is_source": False},
        )
        with pytest.raises(ValueError, match="no_writer_found"):
            discover.run_write_seed(root, "silver.dimcustomer", True)

def test_write_seed_false_resets_flag_without_clearing_profile() -> None:
    """run_write_seed with value=False writes is_seed false and leaves other state alone."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        cat_path = _make_table_cat(
            root,
            "silver.lookup",
            {"status": "no_writer_found"},
            {
                "is_seed": True,
                "is_source": False,
                "profile": {
                    "status": "ok",
                    "classification": {"resolved_kind": "seed", "source": "catalog"},
                },
            },
        )
        result = discover.run_write_seed(root, "silver.lookup", False)
        written = json.loads(cat_path.read_text(encoding="utf-8"))
        assert result.is_seed is False
        assert written["is_seed"] is False
        assert written["profile"]["classification"]["resolved_kind"] == "seed"

def test_write_source_missing_catalog_raises() -> None:
    """run_write_source raises CatalogFileMissingError when catalog file absent."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "catalog" / "tables").mkdir(parents=True)
        with pytest.raises(CatalogFileMissingError):
            discover.run_write_source(root, "silver.nonexistent", True)

def test_write_source_unanalyzed_guard_raises() -> None:
    """run_write_source raises ValueError when table has not been analyzed yet."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        tables_dir = root / "catalog" / "tables"
        tables_dir.mkdir(parents=True)
        (tables_dir / "silver.fresh.json").write_text(
            json.dumps({"schema": "silver", "name": "Fresh"}), encoding="utf-8"
        )
        with pytest.raises(ValueError, match="not been analyzed"):
            discover.run_write_source(root, "silver.fresh", True)
