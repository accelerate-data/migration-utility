"""Oracle integration coverage for setup_ddl CLI."""

from __future__ import annotations

import json
import os

import oracledb
import pytest

from tests.unit.setup_ddl.test_setup_ddl import _run_cli

pytestmark = pytest.mark.oracle


class TestListSchemasOracleIntegration:
    def test_oracle_sh_schema_present(self, tmp_path):
        for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
            if not os.environ.get(var):
                pytest.skip(f"{var} not set")
        try:
            conn = oracledb.connect(
                user=os.environ["ORACLE_USER"],
                password=os.environ["ORACLE_PASSWORD"],
                dsn=os.environ["ORACLE_DSN"],
            )
            conn.close()
        except oracledb.Error as exc:
            pytest.skip(f"Oracle test database not reachable: {exc}")
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli(["list-schemas", "--project-root", str(tmp_path)])
        assert result.returncode == 0, result.stderr
        out = json.loads(result.stdout)
        assert "schemas" in out
        owners = {entry["owner"] for entry in out["schemas"]}
        assert "SH" in owners
        sh_entry = next(e for e in out["schemas"] if e["owner"] == "SH")
        assert sh_entry["tables"] > 0


class TestExtractOracleIntegration:
    def _skip_if_missing(self):
        for var in ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN"):
            if not os.environ.get(var):
                pytest.skip(f"{var} not set")
        try:
            conn = oracledb.connect(
                user=os.environ["ORACLE_USER"],
                password=os.environ["ORACLE_PASSWORD"],
                dsn=os.environ["ORACLE_DSN"],
            )
            conn.close()
        except oracledb.Error as exc:
            pytest.skip(f"Oracle test database not reachable: {exc}")

    def test_sh_produces_ddl_and_catalog(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        assert (tmp_path / "ddl").is_dir()
        assert (tmp_path / "catalog").is_dir()
        tables_dir = tmp_path / "catalog" / "tables"
        assert tables_dir.is_dir()
        assert len(list(tables_dir.glob("*.json"))) > 0

    def test_sh_table_has_pk(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        tables_with_pk = [
            f.name for f in tables_dir.glob("*.json") if json.loads(f.read_text()).get("primary_keys")
        ]
        assert len(tables_with_pk) > 0

    def test_sh_table_has_fk(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        tables_with_fk = [
            f.name for f in tables_dir.glob("*.json") if json.loads(f.read_text()).get("foreign_keys")
        ]
        assert len(tables_with_fk) > 0

    def test_sh_change_capture_null(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        tables_dir = tmp_path / "catalog" / "tables"
        for f in tables_dir.glob("*.json"):
            data = json.loads(f.read_text())
            assert data.get("change_capture") is None

    def test_sh_views_sql_created(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        views_sql = tmp_path / "ddl" / "views.sql"
        assert views_sql.exists()
        content = views_sql.read_text(encoding="utf-8")
        assert "CREATE OR REPLACE VIEW" in content

    def test_sh_views_catalog_created(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
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

    def test_sh_views_ddl_contains_select(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        content = (tmp_path / "ddl" / "views.sql").read_text(encoding="utf-8")
        assert "SELECT" in content.upper()

    def test_sh_views_no_force_editionable(self, tmp_path):
        self._skip_if_missing()
        (tmp_path / "manifest.json").write_text(
            '{"technology": "oracle", "dialect": "oracle"}', encoding="utf-8"
        )
        result = _run_cli([
            "extract",
            "--schemas", "SH",
            "--project-root", str(tmp_path),
        ], timeout=120)
        assert result.returncode == 0, result.stderr
        content = (tmp_path / "ddl" / "views.sql").read_text(encoding="utf-8")
        assert "FORCE" not in content
        assert "EDITIONABLE" not in content
