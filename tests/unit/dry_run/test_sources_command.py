from __future__ import annotations

import json
from pathlib import Path


from shared import generate_sources as gen_src
from tests.unit.dry_run.dry_run_test_helpers import (
    _add_source_table,
    _add_table_to_project,
    _cli_runner,
    _make_bare_project,
    _make_project,
)

def test_generate_sources_only_includes_is_source_true() -> None:
    """Only tables with is_source: true are included in sources."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")

        result = gen_src.generate_sources(root)
        assert "bronze.customerraw" in result.included
        assert "silver.refcurrency" not in result.included
        assert "silver.refcurrency" in result.unconfirmed
        assert "silver.dimcustomer" in result.excluded
        assert result.incomplete == []
        assert result.sources is not None
        schema_names = [s["name"] for s in result.sources["sources"]]
        assert "bronze" in schema_names
        assert "silver" not in schema_names

def test_generate_sources_excludes_resolved_tables() -> None:
    """Tables with resolved status are excluded; unconfirmed writerless go to unconfirmed."""
    tmp, root = _make_project()
    with tmp:
        result = gen_src.generate_sources(root)
        assert "silver.dimcustomer" in result.excluded
        assert "silver.refcurrency" in result.unconfirmed
        assert "silver.refcurrency" not in result.included
        assert result.sources is None

def test_generate_sources_detects_incomplete_scoping() -> None:
    """Tables without scoping section are flagged as incomplete."""
    tmp, root = _make_project()
    with tmp:
        _add_table_to_project(root, "silver.DimDate")
        result = gen_src.generate_sources(root)
        assert "silver.dimdate" in result.incomplete

def test_generate_sources_mixed_statuses() -> None:
    """Mixed resolved, no_writer_found (confirmed and unconfirmed), and incomplete tables."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        _add_source_table(root, "bronze", "OrderRaw")
        _add_table_to_project(root, "silver.DimDate")

        result = gen_src.generate_sources(root)
        assert sorted(result.included) == ["bronze.customerraw", "bronze.orderraw"]
        assert result.excluded == ["silver.dimcustomer"]
        assert result.unconfirmed == ["silver.refcurrency"]
        assert result.incomplete == ["silver.dimdate"]
        schema_names = {s["name"] for s in result.sources["sources"]}
        assert "bronze" in schema_names
        assert "silver" not in schema_names

def test_generate_sources_empty_catalog() -> None:
    """Empty catalog/tables/ produces empty result."""
    tmp, root = _make_bare_project()
    with tmp:
        for f in (root / "catalog" / "tables").glob("*.json"):
            f.unlink()
        result = gen_src.generate_sources(root)
        assert result.sources is None
        assert result.included == []
        assert result.excluded == []
        assert result.incomplete == []

def test_generate_sources_multiple_schemas() -> None:
    """Sources from multiple schemas are emitted under the bronze namespace."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        _add_source_table(root, "staging", "LookupRegion")

        result = gen_src.generate_sources(root)
        schema_names = sorted(s["name"] for s in result.sources["sources"])
        assert schema_names == ["bronze"]
        assert "silver" not in schema_names
        table_names = {table["name"] for table in result.sources["sources"][0]["tables"]}
        assert table_names == {"CustomerRaw", "LookupRegion"}

def test_write_sources_yml() -> None:
    """write_sources_yml creates the YAML file on disk with only is_source: true tables."""
    import yaml

    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = gen_src.write_sources_yml(root)
        assert result.path is not None
        sources_path = Path(result.path)
        assert sources_path.exists()
        content = yaml.safe_load(sources_path.read_text(encoding="utf-8"))
        assert content["version"] == 2
        schema_names = {s["name"] for s in content["sources"]}
        assert "bronze" in schema_names
        assert "silver" not in schema_names

def test_write_sources_yml_no_confirmed_sources() -> None:
    """write_sources_yml returns path=None when no tables have is_source: true."""
    tmp, root = _make_project()
    with tmp:
        result = gen_src.write_sources_yml(root)
        assert result.path is None
        assert result.sources is None

def test_cli_generate_sources() -> None:
    """CLI generate-sources outputs valid JSON with is_source: true table included."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert "bronze.customerraw" in output["included"]
        assert "silver.refcurrency" in output["unconfirmed"]

def test_cli_generate_sources_strict_blocks_on_incomplete() -> None:
    """CLI --strict exits 1 when incomplete scoping exists."""
    tmp, root = _make_project()
    with tmp:
        _add_table_to_project(root, "silver.DimDate")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--strict", "--project-root", str(root)],
        )
        assert result.exit_code == 1
        output = json.loads(result.stdout)
        assert output["error"] == "INCOMPLETE_SCOPING"
        assert "silver.dimdate" in output["incomplete"]

def test_cli_generate_sources_strict_passes_when_complete() -> None:
    """CLI --strict exits 0 when all tables have complete scoping."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--strict", "--project-root", str(root)],
        )
        assert result.exit_code == 0

def test_cli_generate_sources_write() -> None:
    """CLI --write creates sources.yml on disk."""
    tmp, root = _make_project()
    with tmp:
        _add_source_table(root, "bronze", "CustomerRaw")
        result = _cli_runner.invoke(
            gen_src.app,
            ["--write", "--project-root", str(root)],
        )
        assert result.exit_code == 0
        output = json.loads(result.stdout)
        assert output["path"] is not None
        assert Path(output["path"]).exists()
