"""Facade and orchestration tests for target setup."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.target_setup import run_setup_target, write_target_sources_yml


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


def test_write_target_sources_yml_writes_remapped_schema(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    result = write_target_sources_yml(project_root)
    assert result.path is not None
    contents = Path(result.path).read_text(encoding="utf-8")
    assert "schema: bronze" in contents
    assert "name: bronze" in contents


def test_run_setup_target_validates_generated_staging_scope_after_source_tables(
    tmp_path: Path,
) -> None:
    """setup-target validates only generated staging/source artifacts after creating target tables."""
    project_root = _make_sql_server_project(tmp_path)
    command_order: list[str] = []
    applied = MagicMock(
        physical_schema="bronze",
        desired_tables=["bronze.Customer"],
        created_tables=["bronze.Customer"],
        existing_tables=[],
    )
    sources = MagicMock(
        path=str(project_root / "dbt" / "models" / "staging" / "_staging__sources.yml"),
        written_paths=[
            "dbt/models/staging/_staging__sources.yml",
            "dbt/models/staging/_staging__models.yml",
            "dbt/models/staging/stg_bronze__customer.sql",
        ],
        generated_model_names=["stg_bronze__customer"],
        generated_source_selectors=["source:bronze.Customer"],
    )

    def record_apply(_project_root: Path) -> MagicMock:
        command_order.append("apply")
        return applied

    def record_dbt(_project_root: Path, subcommand: str, selectors: list[str]) -> MagicMock:
        command_order.append(subcommand)
        command = [
            "dbt",
            subcommand,
            "--project-dir",
            str(project_root / "dbt"),
            "--profiles-dir",
            str(project_root / "dbt"),
            "--target",
            "dev",
            "--select",
            *selectors,
        ]
        return MagicMock(ran=True, command=command)

    with (
        patch("shared.target_setup.scaffold_target_project", return_value=["dbt/dbt_project.yml"]),
        patch("shared.target_setup.write_target_sources_yml", return_value=sources),
        patch("shared.target_setup.apply_target_source_tables", side_effect=record_apply),
        patch(
            "shared.target_setup.export_seed_tables",
            return_value=MagicMock(files=[], csv_files=[], row_counts={}, written_paths=[]),
        ),
        patch("shared.target_setup.materialize_seed_tables", return_value=MagicMock(ran=False, command=[])),
        patch("shared.target_setup.run_dbt_validation_command", side_effect=record_dbt) as mock_dbt,
    ):
        result = run_setup_target(project_root)

    dbt_root = project_root / "dbt"
    expected_compile = [
        "dbt",
        "compile",
        "--project-dir",
        str(dbt_root),
        "--profiles-dir",
        str(dbt_root),
        "--target",
        "dev",
        "--select",
        "stg_bronze__customer",
    ]
    expected_build = [
        "dbt",
        "build",
        "--project-dir",
        str(dbt_root),
        "--profiles-dir",
        str(dbt_root),
        "--target",
        "dev",
        "--select",
        "stg_bronze__customer",
        "source:bronze.Customer",
    ]
    assert command_order == ["apply", "compile", "build"]
    assert mock_dbt.call_args_list[0].args == (
        project_root,
        "compile",
        ["stg_bronze__customer"],
    )
    assert mock_dbt.call_args_list[1].args == (
        project_root,
        "build",
        ["stg_bronze__customer", "source:bronze.Customer"],
    )
    assert result.dbt_compile_ran is True
    assert result.dbt_compile_command == expected_compile
    assert result.dbt_build_ran is True
    assert result.dbt_build_command == expected_build


def test_run_setup_target_skips_dbt_validation_when_no_generated_models(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    applied = MagicMock(
        physical_schema="bronze",
        desired_tables=[],
        created_tables=[],
        existing_tables=[],
    )

    with (
        patch("shared.target_setup.scaffold_target_project", return_value=["dbt/dbt_project.yml"]),
        patch(
            "shared.target_setup.write_target_sources_yml",
            return_value=MagicMock(
                path=None,
                written_paths=[],
                generated_model_names=[],
                generated_source_selectors=[],
            ),
        ),
        patch("shared.target_setup.apply_target_source_tables", return_value=applied),
        patch(
            "shared.target_setup.export_seed_tables",
            return_value=MagicMock(files=[], csv_files=[], row_counts={}, written_paths=[]),
        ),
        patch("shared.target_setup.materialize_seed_tables", return_value=MagicMock(ran=False, command=[])),
        patch("shared.target_setup.run_dbt_validation_command", return_value=MagicMock(ran=False, command=[])) as mock_dbt,
    ):
        result = run_setup_target(project_root)

    assert mock_dbt.call_count == 2
    assert mock_dbt.call_args_list[0].args == (project_root, "compile", [])
    assert mock_dbt.call_args_list[1].args == (project_root, "build", [])
    assert result.dbt_compile_ran is False
    assert result.dbt_build_ran is False


def test_run_setup_target_stops_when_staging_sources_generation_fails(tmp_path: Path) -> None:
    """setup-target must not mutate target tables after staging artifact generation fails."""
    project_root = _make_sql_server_project(tmp_path)

    with (
        patch("shared.target_setup.scaffold_target_project", return_value=["dbt/dbt_project.yml"]),
        patch(
            "shared.target_setup.write_target_sources_yml",
            return_value=MagicMock(
                sources=None,
                path=None,
                written_paths=[],
                error="STAGING_CONTRACT_TYPE_MISSING",
                message="Cannot generate staging contract",
            ),
        ),
        patch("shared.target_setup.export_seed_tables") as mock_export_seeds,
        patch("shared.target_setup.materialize_seed_tables") as mock_materialize_seeds,
        patch("shared.target_setup.apply_target_source_tables") as mock_apply,
        patch("shared.target_setup.run_dbt_validation_command") as mock_dbt,
    ):
        with pytest.raises(ValueError, match="Cannot generate staging contract"):
            run_setup_target(project_root)

    mock_export_seeds.assert_not_called()
    mock_materialize_seeds.assert_not_called()
    mock_apply.assert_not_called()
    mock_dbt.assert_not_called()


def test_run_setup_target_applies_delta_after_new_source_added(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)

    first_apply = MagicMock(
        physical_schema="bronze",
        desired_tables=["bronze.Customer"],
        created_tables=["bronze.Customer"],
        existing_tables=[],
    )
    second_apply = MagicMock(
        physical_schema="bronze",
        desired_tables=["bronze.Customer", "bronze.Orders"],
        created_tables=["bronze.Orders"],
        existing_tables=["bronze.Customer"],
    )

    with (
        patch("shared.target_setup.scaffold_target_project", return_value=["dbt/dbt_project.yml"]),
        patch(
            "shared.target_setup.write_target_sources_yml",
            return_value=MagicMock(
                path=str(project_root / "dbt" / "models" / "staging" / "_staging__sources.yml"),
                written_paths=[
                    "dbt/models/staging/_staging__sources.yml",
                    "dbt/models/staging/_staging__models.yml",
                    "dbt/models/staging/stg_bronze__customer.sql",
                ],
            ),
        ),
        patch("shared.target_setup.apply_target_source_tables", side_effect=[first_apply, second_apply]),
        patch(
            "shared.target_setup.export_seed_tables",
            return_value=MagicMock(
                files=["dbt/seeds/customertype.csv", "dbt/seeds/_seeds.yml"],
                csv_files=["dbt/seeds/customertype.csv"],
                row_counts={"silver.customertype": 1},
                written_paths=[],
            ),
        ),
        patch("shared.target_setup.materialize_seed_tables", return_value=MagicMock(ran=False, command=[])),
    ):
        first = run_setup_target(project_root)
        second = run_setup_target(project_root)

    assert first.created_tables == ["bronze.Customer"]
    assert "dbt/models/staging/_staging__models.yml" in first.files
    assert "dbt/models/staging/stg_bronze__customer.sql" in first.files
    assert "dbt/models/staging/_staging__models.yml" in first.written_paths
    assert "dbt/models/staging/stg_bronze__customer.sql" in first.written_paths
    assert "dbt/seeds/customertype.csv" not in first.written_paths
    assert "dbt/seeds/_seeds.yml" not in first.written_paths
    assert first.sources_path not in first.written_paths
    assert second.created_tables == ["bronze.Orders"]
    assert "bronze.Customer" in second.existing_tables
