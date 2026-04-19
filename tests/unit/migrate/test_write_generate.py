from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.loader import CatalogFileMissingError
from shared.migrate import (
    run_write_generate,
)
from shared.output_models.migrate import MigrateWriteGenerateOutput
from tests.unit.migrate.helpers import (
    _seed_generate_fixture,
)


def test_write_generate_ok(tmp_path: Path) -> None:
    """Model file exists, compiled=True, tests_passed=True → status ok, generate written."""
    fqn = "dbo.foo"
    _seed_generate_fixture(tmp_path, fqn)

    result = run_write_generate(
        project_root=tmp_path,
        table_fqn=fqn,
        model_path="models/marts/foo.sql",
        compiled=True,
        tests_passed=True,
        test_count=3,
        schema_yml=True,
    )

    assert isinstance(result, MigrateWriteGenerateOutput)
    assert result.status == "ok"
    assert result.ok is True
    assert result.table == fqn

    cat = json.loads((tmp_path / "catalog" / "tables" / f"{fqn}.json").read_text())
    assert cat["generate"]["status"] == "ok"
    assert cat["generate"]["compiled"] is True
    assert cat["generate"]["tests_passed"] is True
    assert cat["generate"]["test_count"] == 3
    assert cat["generate"]["schema_yml"] is True

def test_write_generate_error_file_missing(tmp_path: Path) -> None:
    """Model file does not exist → status error even with compiled=True."""
    fqn = "dbo.foo"
    _seed_generate_fixture(tmp_path, fqn, create_model=False)

    result = run_write_generate(
        project_root=tmp_path,
        table_fqn=fqn,
        model_path="models/marts/foo.sql",
        compiled=True,
        tests_passed=True,
        test_count=3,
        schema_yml=True,
    )

    assert isinstance(result, MigrateWriteGenerateOutput)
    assert result.status == "error"

def test_write_generate_partial_tests_failed(tmp_path: Path) -> None:
    """Model file exists but tests_passed=False → status partial."""
    fqn = "dbo.foo"
    _seed_generate_fixture(tmp_path, fqn)

    result = run_write_generate(
        project_root=tmp_path,
        table_fqn=fqn,
        model_path="models/marts/foo.sql",
        compiled=True,
        tests_passed=False,
        test_count=3,
        schema_yml=True,
    )

    assert isinstance(result, MigrateWriteGenerateOutput)
    assert result.status == "partial"
    cat = json.loads((tmp_path / "catalog" / "tables" / f"{fqn}.json").read_text())
    assert cat["generate"]["status"] == "partial"

def test_write_generate_missing_catalog(tmp_path: Path) -> None:
    """No catalog file → CatalogFileMissingError."""
    fqn = "dbo.foo"
    # Create dbt project but no catalog
    dbt = tmp_path / "dbt"
    marts = dbt / "models" / "marts"
    marts.mkdir(parents=True, exist_ok=True)
    (dbt / "dbt_project.yml").write_text("name: test\n")
    (marts / "foo.sql").write_text("SELECT 1")

    with pytest.raises(CatalogFileMissingError):
        run_write_generate(
            project_root=tmp_path,
            table_fqn=fqn,
            model_path="models/marts/foo.sql",
            compiled=True,
            tests_passed=True,
            test_count=3,
            schema_yml=True,
        )

def test_write_generate_view_autodetect(tmp_path: Path) -> None:
    """View catalog exists (no table catalog) → writes generate to view catalog."""
    fqn = "dbo.foo"
    _seed_generate_fixture(tmp_path, fqn, kind="views")

    result = run_write_generate(
        project_root=tmp_path,
        table_fqn=fqn,
        model_path="models/marts/foo.sql",
        compiled=True,
        tests_passed=True,
        test_count=2,
        schema_yml=False,
    )

    assert isinstance(result, MigrateWriteGenerateOutput)
    assert result.status == "ok"
    assert "views" in result.catalog_path

    cat = json.loads((tmp_path / "catalog" / "views" / f"{fqn}.json").read_text())
    assert cat["generate"]["status"] == "ok"
    assert cat["generate"]["test_count"] == 2
