"""Tests for target source-table materialization helpers."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from shared.target_setup_support.source_tables import (
    TargetApplyResult,
    TargetTableSpec,
    apply_target_source_tables,
    load_target_source_table_specs,
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

def test_source_tables_support_module_exports_materialization_helpers() -> None:
    assert TargetApplyResult
    assert TargetTableSpec
    assert callable(apply_target_source_tables)
    assert callable(load_target_source_table_specs)


def _write_project(project_root: Path) -> None:
    project_root.mkdir()
    (project_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "1.0",
                "runtime": {
                    "target": {
                        "technology": "sql_server",
                        "dialect": "tsql",
                        "connection": {"database": "TargetDB"},
                        "schemas": {"source": "bronze"},
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def test_load_target_source_table_specs_uses_target_sql_type(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_project(project_root)
    tables_dir = project_root / "catalog" / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "silver.customer.json").write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "Customer",
                "is_source": True,
                "columns": [{"name": "id", "sql_type": "INT", "data_type": "NUMBER(10,0)"}],
            }
        ),
        encoding="utf-8",
    )

    specs = load_target_source_table_specs(project_root)

    assert specs[0].fqn == "bronze.Customer"
    assert specs[0].columns[0].source_type == "INT"




def test_apply_target_source_tables_creates_missing_tables_via_adapter(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    adapter = MagicMock()
    adapter.list_source_tables.return_value = set()

    with patch("shared.target_setup_support.source_tables.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = apply_target_source_tables(project_root)

    adapter.ensure_source_schema.assert_called_once_with("bronze")
    adapter.create_source_table.assert_called_once()
    assert result.created_tables == ["bronze.Customer"]
    assert result.existing_tables == []


def test_apply_target_source_tables_uses_target_sql_type(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    (project_root / "catalog" / "tables").mkdir(parents=True)
    (project_root / "catalog" / "tables" / "silver.customer.json").write_text(
        json.dumps(
            {
                "schema": "silver",
                "name": "Customer",
                "is_source": True,
                "columns": [
                    {
                        "name": "id",
                        "source_sql_type": "NUMBER(10,0)",
                        "canonical_tsql_type": "INT",
                        "sql_type": "INT",
                        "data_type": "NUMBER(10,0)",
                        "is_nullable": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    adapter = MagicMock()
    adapter.list_source_tables.return_value = set()

    with patch("shared.target_setup_support.source_tables.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        apply_target_source_tables(project_root)

    created_columns = adapter.create_source_table.call_args.args[2]
    assert created_columns[0].source_type == "INT"


def test_apply_target_source_tables_is_idempotent(tmp_path: Path) -> None:
    project_root = _make_sql_server_project(tmp_path)
    _seed_catalog_table(project_root, "Customer")
    adapter = MagicMock()
    adapter.list_source_tables.return_value = {"customer"}

    with patch("shared.target_setup_support.source_tables.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = apply_target_source_tables(project_root)

    adapter.create_source_table.assert_not_called()
    assert result.created_tables == []
    assert result.existing_tables == ["bronze.Customer"]


def test_apply_target_source_tables_preserves_oracle_target_schema_case(
    tmp_path: Path,
) -> None:
    project_root = _make_oracle_project(tmp_path)
    _seed_catalog_table(project_root, "SILVER_CONFIG", schema="MIGRATIONTEST")
    adapter = MagicMock()
    adapter.list_source_tables.return_value = set()

    with patch("shared.target_setup_support.source_tables.get_dbops") as mock_get_dbops:
        mock_get_dbops.return_value.from_role.return_value = adapter
        result = apply_target_source_tables(project_root)

    adapter.ensure_source_schema.assert_called_once_with("BRONZE")
    adapter.create_source_table.assert_called_once()
    assert adapter.create_source_table.call_args.args[0] == "BRONZE"
    assert result.created_tables == ["BRONZE.SILVER_CONFIG"]
