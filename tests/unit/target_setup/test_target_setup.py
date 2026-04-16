"""Tests for target-setup orchestration helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shared.target_setup import (
    apply_target_source_tables,
    generate_target_sources,
    get_target_source_schema,
    run_setup_target,
    scaffold_target_project,
    write_target_runtime_from_env,
    write_target_sources_yml,
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
) -> None:
    (project_root / "catalog" / "tables").mkdir(parents=True, exist_ok=True)
    (project_root / "catalog" / "tables" / f"{schema.lower()}.{name.lower()}.json").write_text(
        json.dumps(
            {
                "schema": schema,
                "name": name,
                "scoping": {"status": "no_writer_found"},
                "is_source": is_source,
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


def test_get_target_source_schema_defaults_to_bronze(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_manifest(
        project_root,
        {"schema_version": "1.0", "technology": "sql_server", "dialect": "tsql", "runtime": {"target": {"technology": "sql_server", "dialect": "tsql", "connection": {"database": "TargetDB"}}}},
    )
    assert get_target_source_schema(project_root) == "bronze"


def test_write_target_runtime_from_env_maps_sql_server_to_tsql_dialect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
                    "connection": {"database": "SourceDB"},
                },
                "sandbox": {
                    "technology": "sql_server",
                    "dialect": "tsql",
                    "connection": {},
                },
            },
        },
    )
    monkeypatch.setenv("TARGET_MSSQL_DB", "TargetDB")

    role = write_target_runtime_from_env(project_root, "sql_server")

    manifest = json.loads((project_root / "manifest.json").read_text(encoding="utf-8"))
    assert role.technology == "sql_server"
    assert role.dialect == "tsql"
    assert manifest["runtime"]["target"]["technology"] == "sql_server"
    assert manifest["runtime"]["target"]["dialect"] == "tsql"


def test_generate_target_sources_uses_target_schema_override(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    result = generate_target_sources(project_root)
    assert result.sources is not None
    assert result.sources["sources"][0]["name"] == "silver"
    assert result.sources["sources"][0]["schema"] == "bronze"


def test_write_target_sources_yml_writes_remapped_schema(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    result = write_target_sources_yml(project_root)
    assert result.path is not None
    contents = Path(result.path).read_text(encoding="utf-8")
    assert "schema: bronze" in contents
    assert "name: silver" in contents


def test_scaffold_target_project_writes_dbt_files(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    updated = scaffold_target_project(project_root)
    assert "dbt/dbt_project.yml" in updated
    assert "dbt/profiles.yml" in updated
    profiles = (project_root / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    assert 'type: sqlserver' in profiles
    assert 'schema: "bronze"' in profiles


def test_scaffold_target_project_writes_sql_server_profile(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    scaffold_target_project(project_root)
    profiles = (project_root / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    assert 'type: sqlserver' in profiles
    assert "env_var('SA_PASSWORD')" in profiles
    assert 'database: "TargetDB"' in profiles


def test_scaffold_target_project_writes_oracle_profile(tmp_path: Path) -> None:
    project_root = _make_oracle_project(tmp_path)
    scaffold_target_project(project_root)
    profiles = (project_root / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    assert 'type: oracle' in profiles
    assert "env_var('ORACLE_TARGET_PASSWORD')" in profiles
    assert 'service: "TARGETPDB"' in profiles


def test_apply_target_source_tables_creates_missing_tables_via_adapter(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    adapter = MagicMock()
    adapter.list_source_tables.return_value = set()

    with patch("shared.target_setup.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = apply_target_source_tables(project_root)

    adapter.ensure_source_schema.assert_called_once_with("bronze")
    adapter.create_source_table.assert_called_once()
    assert result.created_tables == ["bronze.Customer"]
    assert result.existing_tables == []


def test_apply_target_source_tables_is_idempotent(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    adapter = MagicMock()
    adapter.list_source_tables.return_value = {"customer"}

    with patch("shared.target_setup.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = apply_target_source_tables(project_root)

    adapter.create_source_table.assert_not_called()
    assert result.created_tables == []
    assert result.existing_tables == ["bronze.Customer"]


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
            return_value=MagicMock(path=str(project_root / "dbt" / "models" / "staging" / "sources.yml")),
        ),
        patch("shared.target_setup.apply_target_source_tables", side_effect=[first_apply, second_apply]),
    ):
        first = run_setup_target(project_root)
        second = run_setup_target(project_root)

    assert first.created_tables == ["bronze.Customer"]
    assert second.created_tables == ["bronze.Orders"]
    assert "bronze.Customer" in second.existing_tables


def test_scaffold_target_project_rejects_unknown_technology(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    project_root.mkdir()
    _write_manifest(
        project_root,
        {
            "schema_version": "1.0",
            "technology": "sql_server",
            "dialect": "tsql",
            "runtime": {
                "target": {
                    "technology": "postgres",
                    "dialect": "postgres",
                    "connection": {"database": "TargetDB"},
                }
            },
        },
    )

    with pytest.raises(ValueError, match="Unknown technology: postgres"):
        scaffold_target_project(project_root)
