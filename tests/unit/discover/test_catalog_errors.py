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

def test_show_corrupt_catalog_raises_catalog_load_error() -> None:
    """show with corrupt catalog JSON raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_show(root, "dbo.T")

def test_refs_corrupt_table_catalog_raises() -> None:
    """refs with corrupt table catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_refs(root, "dbo.T")

def test_write_statements_corrupt_proc_catalog_raises() -> None:
    """write-statements with corrupt existing proc catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "procedures", "dbo.usp_test")
        with pytest.raises(CatalogLoadError):
            discover.run_write_statements(
                root,
                "dbo.usp_test",
                [{"action": "migrate", "source": "llm", "sql": "SELECT 1", "rationale": "Core transform."}],
            )

def test_list_succeeds_despite_corrupt_catalog() -> None:
    """list does not read catalog JSON, so corrupt catalogs don't affect it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        result = discover.run_list(root, discover.ObjectType.tables)
        assert "dbo.t" in result.objects

def test_write_scoping_corrupt_table_catalog_raises() -> None:
    """write-scoping with corrupt existing table catalog raises CatalogLoadError."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        with pytest.raises(CatalogLoadError):
            discover.run_write_scoping(root, "dbo.T", {"selected_writer": "dbo.usp_load"})
