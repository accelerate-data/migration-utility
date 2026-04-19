"""Tests for target runtime manifest helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from shared.target_setup_support.runtime import (
    ensure_setup_target_can_rerun,
    get_target_source_schema,
    write_target_runtime_from_env,
)


def test_runtime_support_module_exports_runtime_helpers() -> None:
    assert callable(get_target_source_schema)
    assert callable(write_target_runtime_from_env)


def test_ensure_setup_target_can_rerun_rejects_completed_generated_models(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    catalog_dir = project_root / "catalog" / "tables"
    catalog_dir.mkdir(parents=True)
    (catalog_dir / "silver.customer.json").write_text(
        json.dumps({"generate": {"status": "ok"}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="ad-migration reset all --preserve-catalog"):
        ensure_setup_target_can_rerun(project_root)

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
    assert "driver" not in manifest["runtime"]["target"]["connection"]


def test_setup_target_rerun_guard_rejects_existing_generated_models(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    (project_root / "catalog" / "tables").mkdir(parents=True)
    (project_root / "catalog" / "tables" / "silver.customer.json").write_text(
        json.dumps({
            "schema": "silver",
            "name": "Customer",
            "generate": {"status": "ok", "path": "dbt/models/marts/dim_customer.sql"},
        }),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="ad-migration reset all --preserve-catalog"):
        ensure_setup_target_can_rerun(project_root)
