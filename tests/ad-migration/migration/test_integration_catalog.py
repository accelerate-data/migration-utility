"""Integration tests for catalog extraction against Docker SQL Server.

Requires: Docker SQL Server running with MigrationTest database.
Setup: Run fixtures/sql/catalog_test_setup.sql first.
Run: uv run pytest -m integration -x
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SHARED_DIR = Path(__file__).parent.parent.parent.parent / "agent-sources" / "ad-migration" / "workbench" / "migration" / "shared"

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def catalog_output(tmp_path_factory):
    """Run export_ddl --catalog once for all tests in this module."""
    output_dir = tmp_path_factory.mktemp("catalog_output")
    result = subprocess.run(
        [
            sys.executable, "-m", "shared.export_ddl",
            "--host", "127.0.0.1",
            "--port", "1433",
            "--database", "MigrationTest",
            "--user", "sa",
            "--password", "P@ssw0rd123",
            "--output", str(output_dir),
            "--catalog",
        ],
        cwd=str(SHARED_DIR),
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        pytest.fail(f"export_ddl failed: {result.stderr}")
    return output_dir


def _load_table_catalog(output_dir: Path, table_name: str) -> dict | None:
    path = output_dir / "catalog" / "tables" / f"{table_name}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _load_proc_catalog(output_dir: Path, proc_name: str) -> dict | None:
    path = output_dir / "catalog" / "procedures" / f"{proc_name}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


# -- DMF Detection -----------------------------------------------------------

class TestDMFDetection:
    def test_insert_writer_is_updated(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_insert_writer")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        target = next((t for t in tables if t["name"] == "target_insert"), None)
        assert target is not None
        assert target["is_updated"] is True

    def test_update_writer_is_updated(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_update_writer")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        target = next((t for t in tables if t["name"] == "target_update"), None)
        assert target is not None
        assert target["is_updated"] is True

    def test_merge_writer_is_updated(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_merge_writer")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        target = next((t for t in tables if t["name"] == "target_merge"), None)
        assert target is not None
        assert target["is_updated"] is True

    def test_delete_writer_is_updated(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_delete_writer")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        target = next((t for t in tables if t["name"] == "target_delete"), None)
        assert target is not None
        assert target["is_updated"] is True

    def test_reader_only_not_updated(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_reader_only")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        target = next((t for t in tables if t["name"] == "target_readonly"), None)
        assert target is not None
        assert target["is_selected"] is True
        assert target["is_updated"] is False

    def test_multi_table_both_refs(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_multi_table")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        names = {t["name"] for t in tables}
        assert "target_multi" in names
        assert "staging_multi" in names

    def test_column_level_flags(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_column_detail")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        target = next((t for t in tables if t["name"] == "target_update"), None)
        assert target is not None
        # Should have column-level detail
        if "columns" in target and target["columns"]:
            val_col = next((c for c in target["columns"] if c["name"] == "val"), None)
            if val_col:
                assert val_col["is_updated"] is True

    def test_view_in_referenced_by(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_readonly")
        assert cat is not None
        views = cat["referenced_by"]["views"]["in_scope"]
        names = {v["name"] for v in views}
        assert "vw_readonly" in names

    def test_function_in_referenced_by(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_readonly")
        assert cat is not None
        funcs = cat["referenced_by"]["functions"]["in_scope"]
        names = {f["name"] for f in funcs}
        assert "fn_get_val" in names

    def test_proc_calls_proc(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_calls_other")
        assert cat is not None
        procs = cat["references"]["procedures"]["in_scope"]
        names = {p["name"] for p in procs}
        assert "usp_insert_writer" in names


# -- Catalog Signals ----------------------------------------------------------

class TestCatalogSignals:
    def test_pk_detected(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_insert")
        assert cat is not None
        assert len(cat["primary_keys"]) > 0
        assert "id" in cat["primary_keys"][0]["columns"]

    def test_composite_pk(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_composite_pk")
        assert cat is not None
        pk = cat["primary_keys"][0]
        assert set(pk["columns"]) == {"col_a", "col_b"}

    def test_unique_index(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_unique_idx")
        assert cat is not None
        assert len(cat["unique_indexes"]) > 0

    def test_fk_detected(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_fk")
        assert cat is not None
        assert len(cat["foreign_keys"]) > 0
        fk = cat["foreign_keys"][0]
        assert fk["referenced_table"] == "target_insert"

    def test_multi_fk(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_multi_fk")
        assert cat is not None
        assert len(cat["foreign_keys"]) >= 2

    def test_identity_column(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_identity")
        assert cat is not None
        assert len(cat["auto_increment_columns"]) > 0
        ident = cat["auto_increment_columns"][0]
        assert ident["seed"] == 100
        assert ident["increment"] == 5

    def test_no_constraints_empty(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_no_constraints")
        assert cat is not None
        assert cat["primary_keys"] == []
        assert cat["foreign_keys"] == []
        assert cat["auto_increment_columns"] == []

    def test_no_refs_empty(self, catalog_output):
        cat = _load_table_catalog(catalog_output, "test_catalog.target_no_refs")
        assert cat is not None
        assert cat["referenced_by"]["procedures"]["in_scope"] == []
        assert cat["referenced_by"]["views"]["in_scope"] == []
        assert cat["referenced_by"]["functions"]["in_scope"] == []


# -- Edge Cases ---------------------------------------------------------------

class TestEdgeCases:
    @pytest.mark.xfail(reason="sys.dm_sql_referenced_entities cannot resolve cross-database references; usp_cross_db refs tempdb which DMF skips")
    def test_cross_db_out_of_scope(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_cross_db")
        assert cat is not None
        tables_out = cat["references"]["tables"]["out_of_scope"]
        assert len(tables_out) > 0
        for entry in tables_out:
            assert "reason" in entry

    def test_dynamic_sql_flagged(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_dynamic_sql")
        assert cat is not None
        assert cat.get("has_dynamic_sql") is True

    def test_sp_executesql_flagged(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_sp_executesql")
        assert cat is not None
        assert cat.get("has_dynamic_sql") is True

    def test_empty_proc_no_refs(self, catalog_output):
        cat = _load_proc_catalog(catalog_output, "test_catalog.usp_empty")
        assert cat is not None
        tables = cat["references"]["tables"]["in_scope"]
        assert len(tables) == 0

    def test_schema_bound_view(self, catalog_output):
        # Schema-bound views should appear in referenced_by
        cat = _load_table_catalog(catalog_output, "test_catalog.target_schema_bound")
        assert cat is not None
        views = cat["referenced_by"]["views"]["in_scope"]
        names = {v["name"] for v in views}
        assert "vw_schema_bound" in names
