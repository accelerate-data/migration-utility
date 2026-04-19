from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.loader_data import CatalogFileMissingError
from shared.output_models.catalog_writer import WriteSeedOutput, WriteSourceOutput
from shared.output_models.dry_run import (
    DryRunOutput,
    ExcludeOutput,
    ObjectReadiness,
    ReadinessDetail,
)
from tests.unit.cli.helpers import _write_manifest

runner = CliRunner()


_EXCLUDE_OUT = ExcludeOutput(
    marked=["silver.AuditLog"],
    not_found=[],
    written_paths=["catalog/tables/silver.auditlog.json"],
)

def test_exclude_table_marks_tables(tmp_path):
    _write_manifest(tmp_path)

    with patch("shared.cli.exclude_table_cmd.run_exclude", return_value=_EXCLUDE_OUT):
        result = runner.invoke(
            app,
            ["exclude-table", "silver.AuditLog", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.auditlog.json" in result.output
    assert "Review and commit the repo changes before continuing" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output

def test_add_source_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        object=ObjectReadiness(object="silver.audittest", ready=True, reason="scope complete"),
    )
    write_out = WriteSourceOutput(written="catalog/tables/silver.audittest.json", is_source=True, status="ok")

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source", return_value=write_out),
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.audittest.json" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output

def test_add_source_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        object=ObjectReadiness(object="silver.audittest", ready=False, reason="scope not complete"),
    )

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source") as mock_write,
    ):
        result = runner.invoke(
            app,
            ["add-source-table", "silver.AuditTest", "--project-root", str(tmp_path)],
        )

    assert result.exit_code == 0
    mock_write.assert_not_called()
    assert "Review and commit the repo changes before continuing" not in result.output

def test_add_source_table_allows_seed_table_flip(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            object="silver.lookup",
            object_type="table",
            ready=False,
            reason="not_applicable",
            code="SEED_TABLE",
            not_applicable=True,
        ),
    )
    write_out = WriteSourceOutput(written="catalog/tables/silver.lookup.json", is_source=True, status="ok")

    with (
        patch("shared.cli.add_source_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_source_table_cmd.run_write_source", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-source-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "source   silver.lookup" in result.output

def test_add_seed_table_marks_valid_tables(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(object="silver.lookup", object_type="table", ready=True, reason="ok"),
    )
    write_out = WriteSeedOutput(written="catalog/tables/silver.lookup.json", is_seed=True, status="ok")

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "seed     silver.lookup" in result.output
    assert "is_seed: true" in result.output
    assert "Updated repo state" in result.output
    assert "catalog/tables/silver.lookup.json" in result.output
    assert "git add" not in result.output
    assert "git commit" not in result.output
    assert "git push" not in result.output

def test_add_seed_table_skips_tables_that_fail_guard(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            object="silver.lookup",
            object_type="table",
            ready=False,
            reason="object_not_found",
            code="OBJECT_NOT_FOUND",
        ),
    )

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed") as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_not_called()
    assert "skipped  silver.lookup" in result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output

def test_add_seed_table_allows_source_table_flip(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=False,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(
            object="silver.lookup",
            object_type="table",
            ready=False,
            reason="not_applicable",
            code="SOURCE_TABLE",
            not_applicable=True,
        ),
    )
    write_out = WriteSeedOutput(written="catalog/tables/silver.lookup.json", is_seed=True, status="ok")

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", return_value=write_out) as mock_write,
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    mock_write.assert_called_once_with(tmp_path, "silver.lookup", value=True)
    assert "seed     silver.lookup" in result.output

def test_add_seed_table_warns_when_catalog_file_is_missing(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(object="silver.lookup", object_type="table", ready=True, reason="ok"),
    )

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch(
            "shared.cli.add_seed_table_cmd.run_write_seed",
            side_effect=CatalogFileMissingError("table", "silver.lookup"),
        ),
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "missing  silver.lookup" in result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output

def test_add_seed_table_warns_when_write_fails_with_value_error(tmp_path):
    _write_manifest(tmp_path)
    ready_out = DryRunOutput(
        stage="scope",
        ready=True,
        project=ReadinessDetail(ready=True, reason="ok"),
        object=ObjectReadiness(object="silver.lookup", object_type="table", ready=True, reason="ok"),
    )

    with (
        patch("shared.cli.add_seed_table_cmd.run_ready", return_value=ready_out),
        patch("shared.cli.add_seed_table_cmd.run_write_seed", side_effect=ValueError("bad state")),
    ):
        result = runner.invoke(app, ["add-seed-table", "silver.lookup", "--project-root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "skipped  silver.lookup -- bad state" in result.output
    assert "Updated repo state" not in result.output
    assert "Review and commit the repo changes before continuing" not in result.output
