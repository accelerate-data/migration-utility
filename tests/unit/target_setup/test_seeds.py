"""Tests for target seed export and materialization helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from shared.target_setup_support import seed_commands, seed_export, seed_rendering, seed_specs
from shared.target_setup_support.seeds import (
    DbtSeedResult,
    SeedExportResult,
    SeedTableSpec,
    export_seed_tables,
    materialize_seed_tables,
)


def _write_manifest(project_root: Path, manifest: dict) -> None:
    (project_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )


def _seed_catalog_table(
    project_root: Path,
    name: str,
    *,
    schema: str = "silver",
    is_source: bool = True,
    is_seed: bool = False,
) -> None:
    (project_root / "catalog" / "tables").mkdir(parents=True, exist_ok=True)
    (project_root / "catalog" / "tables" / f"{schema.lower()}.{name.lower()}.json").write_text(
        json.dumps(
            {
                "schema": schema,
                "name": name,
                "scoping": {"status": "no_writer_found"},
                "is_source": is_source,
                "is_seed": is_seed,
                "columns": [
                    {"name": "id", "sql_type": "INT", "is_nullable": False},
                    {"name": "name", "sql_type": "NVARCHAR(50)", "is_nullable": True},
                ],
            }
        ),
        encoding="utf-8",
    )

def _make_sql_server_project(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_manifest(
        project_root,
        {
            "schema_version": "1.0",
            "technology": "sql_server",
            "dialect": "tsql",
            "runtime": {
                "source": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {"host": "localhost", "port": "1433", "database": "SourceDB"},
                },
                "target": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {
                        "host": "localhost",
                        "port": "1433",
                        "database": "TargetDB",
                        "user": "sa",
                        "password_env": "SA_PASSWORD",
                        "driver": "ODBC Driver 18 for SQL Server",
                    },
                    "schemas": {"source": "bronze"},
                },
            },
        },
    )
    return project_root


def _make_oracle_project(tmp_path: Path) -> Path:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_manifest(
        project_root,
        {
            "schema_version": "1.0",
            "technology": "oracle",
            "dialect": "oracle",
            "runtime": {
                "source": {
                    "technology": "oracle",
                    "dialect": "oracle",
                    "connection": {"host": "localhost", "port": "1521", "service": "SRCPDB", "schema": "SH"},
                },
                "target": {
                    "technology": "oracle",
                    "dialect": "oracle",
                    "connection": {
                        "host": "localhost",
                        "port": "1521",
                        "service": "TARGETPDB",
                        "user": "BRONZE",
                        "schema": "BRONZE",
                        "password_env": "ORACLE_TARGET_PASSWORD",
                    },
                    "schemas": {"source": "BRONZE"},
                },
            },
        },
    )
    return project_root

def test_seeds_support_module_exports_seed_helpers() -> None:
    assert DbtSeedResult
    assert SeedExportResult
    assert SeedTableSpec
    assert callable(export_seed_tables)
    assert callable(materialize_seed_tables)


def test_seed_support_is_split_by_responsibility() -> None:
    assert seed_specs.SeedTableSpec is SeedTableSpec
    assert callable(seed_specs.load_seed_table_specs)
    assert callable(seed_rendering.render_seed_csv)
    assert callable(seed_rendering.render_seeds_yml)
    assert seed_export.SeedExportResult is SeedExportResult
    assert callable(seed_export.export_seed_tables)
    assert seed_commands.DbtSeedResult is DbtSeedResult
    assert callable(seed_commands.materialize_seed_tables)


def _write_project(project_root: Path) -> None:
    project_root.mkdir()
    (project_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "runtime": {
                    "source": {
                        "technology": "sql_server",
                        "dialect": "tsql",
                        "connection": {"database": "SourceDB"},
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def test_export_seed_tables_writes_seed_csv_and_yaml(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_project(project_root)
    tables_dir = project_root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.customertype.json").write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "CustomerType",
                "is_seed": True,
                "columns": [
                    {"name": "id", "sql_type": "INT"},
                    {"name": "name", "sql_type": "NVARCHAR(50)"},
                ],
            }
        ),
        encoding="utf-8",
    )
    adapter = MagicMock()
    adapter.read_table_rows.return_value = (["id", "name"], [(1, "Retail")])

    with patch("shared.target_setup_support.seed_export.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = export_seed_tables(project_root)

    assert result.files == ["dbt/seeds/customertype.csv", "dbt/seeds/_seeds.yml"]
    assert result.row_counts == {"silver.customertype": 1}
    assert (project_root / "dbt" / "seeds" / "customertype.csv").read_text(encoding="utf-8") == "id,name\n1,Retail\n"




def test_export_seed_tables_writes_seed_csv_from_source_table(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "CustomerType", is_source=False, is_seed=True)
    adapter = MagicMock()
    adapter.read_table_rows.return_value = (
        ["id", "name"],
        [(1, "Retail"), (2, "Partner, Channel"), (3, None)],
    )

    with patch("shared.target_setup_support.seed_export.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = export_seed_tables(project_root)

    adapter.read_table_rows.assert_called_once_with("silver", "CustomerType", ["id", "name"])
    seed_path = project_root / "dbt" / "seeds" / "customertype.csv"
    seed_yml_path = project_root / "dbt" / "seeds" / "_seeds.yml"
    assert result.files == ["dbt/seeds/customertype.csv", "dbt/seeds/_seeds.yml"]
    assert result.csv_files == ["dbt/seeds/customertype.csv"]
    assert result.written_paths == ["dbt/seeds/customertype.csv", "dbt/seeds/_seeds.yml"]
    assert result.row_counts == {"silver.customertype": 3}
    assert seed_path.read_text(encoding="utf-8") == (
        "id,name\n"
        "1,Retail\n"
        '2,"Partner, Channel"\n'
        "3,\n"
    )
    assert seed_yml_path.read_text(encoding="utf-8") == (
        "version: 2\n"
        "seeds:\n"
        "- name: customertype\n"
        "  columns:\n"
        "  - name: id\n"
        "    data_type: INT\n"
        "  - name: name\n"
        "    data_type: NVARCHAR(50)\n"
    )


def test_export_seed_tables_uses_target_sql_type_in_seed_yml(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    (project_root / "catalog" / "tables").mkdir(parents=True)
    (project_root / "catalog" / "tables" / "silver.customertype.json").write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "CustomerType",
                "is_seed": True,
                "columns": [
                    {
                        "name": "id",
                        "source_sql_type": "NUMBER(10,0)",
                        "canonical_tsql_type": "INT",
                        "sql_type": "INT",
                        "data_type": "NUMBER(10,0)",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    adapter = MagicMock()
    adapter.read_table_rows.return_value = (["id"], [(1,)])

    with patch("shared.target_setup_support.seed_export.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        export_seed_tables(project_root)

    seed_yml_path = project_root / "dbt" / "seeds" / "_seeds.yml"
    assert "data_type: INT" in seed_yml_path.read_text(encoding="utf-8")
    assert "NUMBER(10,0)" not in seed_yml_path.read_text(encoding="utf-8")


def test_export_seed_tables_preserves_oracle_source_schema_case(tmp_path: Path) -> None:
    project_root = _make_oracle_project(tmp_path)
    _seed_catalog_table(
        project_root,
        "BRONZE_CURRENCY",
        schema="MIGRATIONTEST",
        is_source=False,
        is_seed=True,
    )
    adapter = MagicMock()
    adapter.read_table_rows.return_value = (
        ["ID", "NAME"],
        [(1, "USD")],
    )

    with patch("shared.target_setup_support.seed_export.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = export_seed_tables(project_root)

    adapter.read_table_rows.assert_called_once_with(
        "MIGRATIONTEST",
        "BRONZE_CURRENCY",
        ["id", "name"],
    )
    assert result.row_counts == {"migrationtest.bronze_currency": 1}


def test_export_seed_tables_reports_no_written_paths_when_content_unchanged(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "CustomerType", is_source=False, is_seed=True)
    adapter = MagicMock()
    adapter.read_table_rows.return_value = (
        ["id", "name"],
        [(1, "Retail")],
    )

    with patch("shared.target_setup_support.seed_export.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        first = export_seed_tables(project_root)
        second = export_seed_tables(project_root)

    assert first.written_paths == ["dbt/seeds/customertype.csv", "dbt/seeds/_seeds.yml"]
    assert second.files == ["dbt/seeds/customertype.csv", "dbt/seeds/_seeds.yml"]
    assert second.csv_files == ["dbt/seeds/customertype.csv"]
    assert second.written_paths == []


def test_materialize_seed_tables_runs_dbt_seed(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    (project_root / "dbt").mkdir()
    completed = MagicMock(returncode=0, stdout="seeded", stderr="")

    with patch("shared.target_setup_support.seed_commands.subprocess.run", return_value=completed) as mock_run:
        result = materialize_seed_tables(project_root, ["dbt/seeds/customertype.csv"])

    expected_cmd = [
        "dbt",
        "seed",
        "--project-dir",
        str(project_root / "dbt"),
        "--profiles-dir",
        str(project_root / "dbt"),
        "--target",
        "dev",
    ]
    mock_run.assert_called_once()
    assert mock_run.call_args.args[0] == expected_cmd
    assert mock_run.call_args.kwargs["cwd"] == project_root / "dbt"
    assert result.ran is True
    assert result.command == expected_cmd


def test_materialize_seed_tables_skips_when_no_seed_files(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    with patch("shared.target_setup_support.seed_commands.subprocess.run") as mock_run:
        result = materialize_seed_tables(project_root, [])

    mock_run.assert_not_called()
    assert result.ran is False
    assert result.command == []


def test_materialize_seed_tables_skips_when_only_seed_properties_file(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    with patch("shared.target_setup_support.seed_commands.subprocess.run") as mock_run:
        result = materialize_seed_tables(project_root, ["dbt/seeds/_seeds.yml"])

    mock_run.assert_not_called()
    assert result.ran is False
    assert result.command == []


def test_materialize_seed_tables_reports_packaged_runtime_when_dbt_missing(
    tmp_path: Path,
) -> None:
    project_root = _make_sql_server_project(tmp_path)
    (project_root / "dbt").mkdir(exist_ok=True)

    with patch(
        "shared.target_setup_support.seed_commands.subprocess.run",
        side_effect=FileNotFoundError("dbt"),
    ):
        try:
            materialize_seed_tables(project_root, ["dbt/seeds/customertype.csv"])
        except ValueError as exc:
            message = str(exc)
        else:
            raise AssertionError("expected missing dbt ValueError")

    assert "ad-migration doctor drivers" in message
    assert "Homebrew formula resources" in message
    assert "pip install" not in message
    assert "uv pip install" not in message
