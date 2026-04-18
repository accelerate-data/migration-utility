import json
from unittest.mock import patch

from typer.testing import CliRunner

from shared.cli.main import app
from shared.output_models.replicate_source_tables import ReplicateSourceTablesOutput, ReplicateTableResult

runner = CliRunner()


def test_replicate_source_tables_requires_limit(tmp_path):
    result = runner.invoke(
        app,
        ["replicate-source-tables", "--project-root", str(tmp_path), "--yes"],
    )

    assert result.exit_code == 1
    assert "LIMIT_REQUIRED" in result.output


def test_replicate_source_tables_rejects_over_cap_limit(tmp_path):
    result = runner.invoke(
        app,
        ["replicate-source-tables", "--project-root", str(tmp_path), "--limit", "10001", "--yes"],
    )

    assert result.exit_code == 1
    assert "LIMIT_TOO_HIGH" in result.output


def test_replicate_source_tables_dry_run_json_delegates_options(tmp_path):
    output = ReplicateSourceTablesOutput(
        status="ok",
        dry_run=True,
        limit=25,
        tables=[
            ReplicateTableResult(
                fqn="silver.dimcustomer",
                source_schema="silver",
                source_table="DimCustomer",
                target_schema="bronze",
                target_table="DimCustomer",
                columns=["id"],
                predicate="id > 10",
                status="planned",
            )
        ],
    )

    with patch("shared.cli.replicate_source_tables_cmd.run_replicate_source_tables", return_value=output) as mock_run:
        result = runner.invoke(
            app,
            [
                "replicate-source-tables",
                "--project-root",
                str(tmp_path),
                "--limit",
                "25",
                "--select",
                "silver.DimCustomer",
                "--exclude",
                "silver.FactSales",
                "--filter",
                "silver.DimCustomer=id > 10",
                "--dry-run",
                "--json",
            ],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["status"] == "ok"
    assert payload["tables"][0]["predicate"] == "id > 10"
    mock_run.assert_called_once_with(
        tmp_path,
        limit=25,
        select=["silver.DimCustomer"],
        exclude=["silver.FactSales"],
        filters=["silver.DimCustomer=id > 10"],
        dry_run=True,
    )


def test_replicate_source_tables_requires_confirmation_for_destructive_run(tmp_path):
    with patch("shared.cli.replicate_source_tables_cmd.run_replicate_source_tables") as mock_run:
        result = runner.invoke(
            app,
            ["replicate-source-tables", "--project-root", str(tmp_path), "--limit", "10"],
            input="n\n",
        )

    assert result.exit_code == 0, result.output
    assert "Aborted" in result.output
    mock_run.assert_not_called()


def test_replicate_source_tables_exits_one_when_any_table_fails(tmp_path):
    output = ReplicateSourceTablesOutput(
        status="error",
        dry_run=False,
        limit=10,
        tables=[
            ReplicateTableResult(
                fqn="silver.dimcustomer",
                source_schema="silver",
                source_table="DimCustomer",
                target_schema="bronze",
                target_table="DimCustomer",
                status="error",
                error="source failed",
            )
        ],
    )

    with patch("shared.cli.replicate_source_tables_cmd.run_replicate_source_tables", return_value=output):
        result = runner.invoke(
            app,
            ["replicate-source-tables", "--project-root", str(tmp_path), "--limit", "10", "--yes"],
        )

    assert result.exit_code == 1
    assert "silver.dimcustomer" in result.output
    assert "source failed" in result.output
