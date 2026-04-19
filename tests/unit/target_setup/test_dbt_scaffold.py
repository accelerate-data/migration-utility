"""Tests for target dbt scaffold helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.target_setup_support.dbt_scaffold import scaffold_target_project


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

def test_dbt_scaffold_support_module_exports_scaffold_helper() -> None:
    assert callable(scaffold_target_project)


def test_scaffold_target_project_writes_dbt_files(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    updated = scaffold_target_project(project_root)
    assert "dbt/dbt_project.yml" in updated
    assert "dbt/profiles.yml" in updated
    profiles = (project_root / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    assert 'type: sqlserver' in profiles
    assert 'schema: "bronze"' in profiles
    dbt_project = (project_root / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert "models:\n" in dbt_project
    assert "staging:\n      +materialized: view" in dbt_project
    assert "intermediate:\n      +materialized: ephemeral" in dbt_project
    assert "marts:\n      +materialized: table" in dbt_project
    assert (project_root / "dbt" / "models" / "staging").is_dir()
    assert (project_root / "dbt" / "models" / "intermediate").is_dir()
    assert (project_root / "dbt" / "models" / "marts").is_dir()


def test_scaffold_target_project_writes_sql_server_profile(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    scaffold_target_project(project_root)
    profiles = (project_root / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    assert 'type: sqlserver' in profiles
    assert 'driver: "FreeTDS"' in profiles
    assert 'driver: "ODBC Driver 18 for SQL Server"' not in profiles
    assert "env_var('SA_PASSWORD')" in profiles
    assert 'database: "TargetDB"' in profiles


def test_scaffold_target_project_configures_seed_schema_with_profile_schema(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    scaffold_target_project(project_root)
    project_yml = (project_root / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert 'seed-paths: ["seeds"]' in project_yml
    assert '+schema: "bronze"' not in project_yml


def test_scaffold_target_project_writes_oracle_profile(tmp_path: Path) -> None:
    project_root = _make_oracle_project(tmp_path)
    scaffold_target_project(project_root)
    profiles = (project_root / "dbt" / "profiles.yml").read_text(encoding="utf-8")
    assert 'type: oracle' in profiles
    assert "env_var('ORACLE_TARGET_PASSWORD')" in profiles
    assert 'protocol: "tcp"' in profiles
    assert 'database: "TARGETPDB"' in profiles
    assert 'service: "TARGETPDB"' in profiles
    assert 'schema: "BRONZE"' in profiles


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
