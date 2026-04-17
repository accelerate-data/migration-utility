from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared import discover
from shared.loader import (
    CatalogFileMissingError,
    CatalogLoadError,
    CatalogNotFoundError,
    DdlParseError,
    ObjectNotFoundError,
)
from tests.unit.discover.discover_test_helpers import (
    _CATALOG_FIXTURES,
    _FLAT_FIXTURES,
    _LISTING_OBJECTS_EVAL_FIXTURES,
    _SOURCE_TABLE_GUARD_FIXTURES,
    _UNPARSEABLE_FIXTURES,
    _make_proc_cat,
    _make_project_with_corrupt_catalog,
    _make_project_with_proc_view_refs,
    _make_project_with_view_catalog,
    _make_table_cat,
)

class TestWriteTableSlice:

    def test_write_table_slice_happy_path(self) -> None:
        """run_write_table_slice writes slice text into proc catalog table_slices."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_proc_cat(root, "dbo.usp_multi")
            result = discover.run_write_table_slice(root, "dbo.usp_multi", "dim.target", "MERGE INTO dim.target ...")
            assert result.status == "ok"
            proc_path = root / "catalog" / "procedures" / "dbo.usp_multi.json"
            written = json.loads(proc_path.read_text(encoding="utf-8"))
            assert written["table_slices"]["dim.target"] == "MERGE INTO dim.target ..."

    def test_write_table_slice_accumulates(self) -> None:
        """run_write_table_slice accumulates slices for distinct tables under the same proc."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_proc_cat(root, "dbo.usp_multi")
            discover.run_write_table_slice(root, "dbo.usp_multi", "dim.table_a", "INSERT INTO dim.table_a ...")
            discover.run_write_table_slice(root, "dbo.usp_multi", "dim.table_b", "INSERT INTO dim.table_b ...")
            proc_path = root / "catalog" / "procedures" / "dbo.usp_multi.json"
            written = json.loads(proc_path.read_text(encoding="utf-8"))
            assert "dim.table_a" in written["table_slices"]
            assert "dim.table_b" in written["table_slices"]

    def test_write_table_slice_overwrites_existing(self) -> None:
        """run_write_table_slice overwrites an existing slice for the same (proc, table) pair."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_proc_cat(root, "dbo.usp_multi")
            discover.run_write_table_slice(root, "dbo.usp_multi", "dim.target", "SELECT 1")
            discover.run_write_table_slice(root, "dbo.usp_multi", "dim.target", "SELECT 2")
            proc_path = root / "catalog" / "procedures" / "dbo.usp_multi.json"
            written = json.loads(proc_path.read_text(encoding="utf-8"))
            assert written["table_slices"]["dim.target"] == "SELECT 2"

    def test_write_table_slice_missing_catalog(self) -> None:
        """run_write_table_slice raises CatalogFileMissingError when proc catalog is absent."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "catalog" / "procedures").mkdir(parents=True)
            with pytest.raises(CatalogFileMissingError):
                discover.run_write_table_slice(root, "dbo.usp_nonexistent", "dim.target", "SELECT 1")
