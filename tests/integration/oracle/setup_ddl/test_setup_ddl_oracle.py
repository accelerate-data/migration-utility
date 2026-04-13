"""Oracle integration coverage for setup_ddl CLI."""

from __future__ import annotations

import json

import pytest

from tests.helpers import run_setup_ddl_cli as _run_cli
from tests.integration.runtime_helpers import ORACLE_MIGRATION_SCHEMA

pytestmark = pytest.mark.oracle


@pytest.mark.usefixtures("oracle_extract_env")
class TestListSchemasOracleIntegration:
    def test_oracle_migrationtest_schema_present(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "schemas" in out
        owners = {entry["owner"] for entry in out["schemas"]}
        assert ORACLE_MIGRATION_SCHEMA in owners
        schema_entry = next(
            e for e in out["schemas"] if e["owner"] == ORACLE_MIGRATION_SCHEMA
        )
        assert schema_entry["tables"] > 0


@pytest.mark.usefixtures("oracle_extract_env")
class TestExtractOracleIntegration:
    def test_migrationtest_produces_ddl_and_catalog(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "ddl").is_dir()
        assert (tmp_path / "catalog").is_dir()
        tables_dir = tmp_path / "catalog" / "tables"
        assert tables_dir.is_dir()
        assert len(list(tables_dir.glob("*.json"))) > 0

    def test_migrationtest_table_has_pk(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        tables_with_pk = [
            f.name for f in tables_dir.glob("*.json") if json.loads(f.read_text()).get("primary_keys")
        ]
        assert len(tables_with_pk) > 0

    def test_migrationtest_table_has_fk(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        tables_with_fk = [
            f.name for f in tables_dir.glob("*.json") if json.loads(f.read_text()).get("foreign_keys")
        ]
        assert len(tables_with_fk) > 0

    def test_migrationtest_change_capture_null(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        for f in tables_dir.glob("*.json"):
            data = json.loads(f.read_text())
            assert data.get("change_capture") is None

    def test_migrationtest_views_sql_created(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        views_sql = tmp_path / "ddl" / "views.sql"
        assert views_sql.exists()
        content = views_sql.read_text(encoding="utf-8")
        assert "CREATE OR REPLACE VIEW" in content

    def test_migrationtest_views_catalog_created(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        views_dir = tmp_path / "catalog" / "views"
        assert views_dir.is_dir()
        view_files = list(views_dir.glob("*.json"))
        assert len(view_files) > 0
        data = json.loads(view_files[0].read_text())
        assert "sql" in data
        assert "CREATE OR REPLACE VIEW" in data["sql"]

    def test_migrationtest_views_ddl_contains_select(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        content = (tmp_path / "ddl" / "views.sql").read_text(encoding="utf-8")
        assert "SELECT" in content.upper()

    def test_migrationtest_views_no_force_editionable(self, tmp_path, oracle_extract_env):
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", ORACLE_MIGRATION_SCHEMA,
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        content = (tmp_path / "ddl" / "views.sql").read_text(encoding="utf-8")
        assert "FORCE" not in content
        assert "EDITIONABLE" not in content
