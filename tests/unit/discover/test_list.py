from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from shared import discover
from tests.unit.discover.discover_test_helpers import (
    _FLAT_FIXTURES,
    _SOURCE_TABLE_GUARD_FIXTURES,
    _UNPARSEABLE_FIXTURES,
    _make_project_with_corrupt_catalog,
    _make_table_cat,
)

def test_list_flat_tables() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables)
    objects = result.objects
    assert "silver.dimproduct" in objects
    assert "bronze.product" in objects
    assert "bronze.customer" in objects
    assert "bronze.sales" in objects
    assert "bronze.salesorder" in objects
    assert "bronze.geography" in objects
    assert "bronze.runcontrol" in objects
    assert "dbo.config" in objects

def test_list_sources_returns_confirmed_sources_only() -> None:
    result = discover.run_list(_SOURCE_TABLE_GUARD_FIXTURES, discover.ObjectType.sources)
    assert result.objects == ["silver.dimsource"]

def test_list_tables_includes_source_tables_unchanged() -> None:
    result = discover.run_list(_SOURCE_TABLE_GUARD_FIXTURES, discover.ObjectType.tables)
    assert result.objects == ["silver.dimsource"]

def test_list_sources_filters_out_non_source_tables() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ddl_dir = root / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE silver.DimSource (SourceKey INT NOT NULL)\nGO\n"
            "CREATE TABLE silver.DimTarget (TargetKey INT NOT NULL)\nGO\n",
            encoding="utf-8",
        )
        _make_table_cat(
            root,
            "silver.dimsource",
            {"status": "no_writer_found"},
            {"is_source": True, "columns": [{"name": "SourceKey", "sql_type": "INT"}]},
        )
        _make_table_cat(
            root,
            "silver.dimtarget",
            {"status": "resolved", "selected_writer": "dbo.usp_load_dimtarget"},
            {"columns": [{"name": "TargetKey", "sql_type": "INT"}]},
        )

        result = discover.run_list(root, discover.ObjectType.sources)

    assert result.objects == ["silver.dimsource"]

def test_list_sources_skips_excluded_source_tables() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ddl_dir = root / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE silver.DimSource (SourceKey INT NOT NULL)\nGO\n"
            "CREATE TABLE silver.DimExcluded (ExcludedKey INT NOT NULL)\nGO\n",
            encoding="utf-8",
        )
        _make_table_cat(
            root,
            "silver.dimsource",
            {"status": "no_writer_found"},
            {"is_source": True, "columns": [{"name": "SourceKey", "sql_type": "INT"}]},
        )
        _make_table_cat(
            root,
            "silver.dimexcluded",
            {"status": "no_writer_found"},
            {
                "is_source": True,
                "excluded": True,
                "columns": [{"name": "ExcludedKey", "sql_type": "INT"}],
            },
        )

        result = discover.run_list(root, discover.ObjectType.sources)

    assert result.objects == ["silver.dimsource"]

def test_list_sources_skips_corrupt_catalog_files() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ddl_dir = root / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE silver.DimSource (SourceKey INT NOT NULL)\nGO\n"
            "CREATE TABLE silver.DimBroken (BrokenKey INT NOT NULL)\nGO\n",
            encoding="utf-8",
        )
        _make_table_cat(
            root,
            "silver.dimsource",
            {"status": "no_writer_found"},
            {"is_source": True, "columns": [{"name": "SourceKey", "sql_type": "INT"}]},
        )
        broken_path = root / "catalog" / "tables" / "silver.dimbroken.json"
        broken_path.parent.mkdir(parents=True, exist_ok=True)
        broken_path.write_text("{broken", encoding="utf-8")

        result = discover.run_list(root, discover.ObjectType.sources)

    assert result.objects == ["silver.dimsource"]

def test_list_seeds_returns_confirmed_seeds_only() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _make_table_cat(
            root,
            "silver.seedlookup",
            {"status": "no_writer_found"},
            {"is_seed": True, "is_source": False},
        )
        _make_table_cat(
            root,
            "silver.dimsource",
            {"status": "no_writer_found"},
            {"is_source": True, "is_seed": False},
        )
        result = discover.run_list(root, discover.ObjectType.seeds)
        assert result.objects == ["silver.seedlookup"]

def test_list_seeds_skips_excluded_seed_tables() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _make_table_cat(
            root,
            "silver.seedlookup",
            {"status": "no_writer_found"},
            {"is_seed": True, "excluded": True},
        )
        result = discover.run_list(root, discover.ObjectType.seeds)
        assert result.objects == []

def test_list_seeds_honors_catalog_dir_override(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        catalog_dir = root / "custom-catalog"
        tables_dir = catalog_dir / "tables"
        tables_dir.mkdir(parents=True)
        monkeypatch.setenv("CATALOG_DIR", str(catalog_dir))
        (tables_dir / "silver.seedlookup.json").write_text(
            json.dumps({
                "schema": "Silver",
                "name": "SeedLookup",
                "is_seed": True,
            }),
            encoding="utf-8",
        )

        result = discover.run_list(root, discover.ObjectType.seeds)

    assert result.objects == ["silver.seedlookup"]

def test_list_seeds_skips_corrupt_catalog_files(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        catalog_dir = root / "custom-catalog"
        tables_dir = catalog_dir / "tables"
        tables_dir.mkdir(parents=True)
        monkeypatch.setenv("CATALOG_DIR", str(catalog_dir))
        (tables_dir / "silver.seedlookup.json").write_text(
            json.dumps({
                "schema": "Silver",
                "name": "SeedLookup",
                "is_seed": True,
            }),
            encoding="utf-8",
        )
        (tables_dir / "silver.seedbroken.json").write_text("{broken", encoding="utf-8")

        with caplog.at_level("WARNING"):
            result = discover.run_list(root, discover.ObjectType.seeds)

    assert result.objects == ["silver.seedlookup"]
    assert any("reason=parse_error" in record.message for record in caplog.records)

def test_list_flat_procedures() -> None:
    result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.procedures)
    objects = result.objects
    assert "dbo.usp_loaddimproduct" in objects
    assert "dbo.usp_logmessage" in objects
    assert "dbo.usp_mergedimproduct" in objects
    assert "dbo.usp_loadwithcte" in objects
    assert "dbo.usp_loadwithmulticte" in objects
    assert "dbo.usp_loadwithcase" in objects
    assert "dbo.usp_loadwithleftjoin" in objects
    assert "dbo.usp_conditionalmerge" in objects
    assert "dbo.usp_trycatchload" in objects
    assert "dbo.usp_correlatedsubquery" in objects

def test_list_flat_missing_optional() -> None:
    """Directory with only tables.sql — views list returns empty without error."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        ddl_dir = p / "ddl"
        ddl_dir.mkdir()
        (ddl_dir / "tables.sql").write_text(
            "CREATE TABLE dbo.SomeTable (Id INT)\nGO\n", encoding="utf-8"
        )
        # Minimal catalog dir to satisfy mandatory check
        (p / "catalog" / "tables").mkdir(parents=True)
        (p / "catalog" / "tables" / "dbo.sometable.json").write_text(
            '{"columns":[],"primary_keys":[],"unique_indexes":[],"foreign_keys":[],'
            '"auto_increment_columns":[],"change_capture":null,"sensitivity_classifications":[],'
            '"referenced_by":{"procedures":{"in_scope":[],"out_of_scope":[]},'
            '"views":{"in_scope":[],"out_of_scope":[]},"functions":{"in_scope":[],"out_of_scope":[]}}}',
            encoding="utf-8",
        )
        result = discover.run_list(p, discover.ObjectType.views)
    assert result.objects == []

def test_list_indexed_same_as_flat() -> None:
    """Indexed dir returns same object names as flat dir."""
    import shutil

    from shared.loader import index_directory

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "indexed"
        index_directory(_FLAT_FIXTURES, out)
        # Copy catalog/ from flat fixtures so indexed dir also has catalog
        shutil.copytree(_FLAT_FIXTURES / "catalog", out / "catalog")

        flat_result = discover.run_list(_FLAT_FIXTURES, discover.ObjectType.tables)
        indexed_result = discover.run_list(out, discover.ObjectType.tables)

    assert flat_result.objects == indexed_result.objects

def test_list_unparseable_stored_with_error() -> None:
    """Unparseable DDL blocks are stored with parse_error, not skipped."""
    from shared.loader import load_directory

    result = load_directory(_UNPARSEABLE_FIXTURES)
    has_error = any(e.parse_error is not None for e in result.procedures.values())
    assert has_error

def test_discover_cli_list_succeeds_with_unparseable() -> None:
    """discover CLI list succeeds even with unparseable blocks (stored with error)."""
    from typer.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(
        discover.app,
        ["list", "--project-root", str(_UNPARSEABLE_FIXTURES), "--type", "procedures"],
    )
    assert result.exit_code == 0

def test_list_succeeds_despite_corrupt_catalog() -> None:
    """list does not read catalog JSON, so corrupt catalogs don't affect it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = _make_project_with_corrupt_catalog(Path(tmp), "tables", "dbo.t")
        result = discover.run_list(root, discover.ObjectType.tables)
        assert "dbo.t" in result.objects
