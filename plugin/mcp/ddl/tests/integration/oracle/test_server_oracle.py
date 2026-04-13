"""Live Oracle integration coverage for ddl_mcp/server.py."""

import json
from pathlib import Path

import pytest

from helpers import git_init, run_setup_ddl
from shared.loader import load_directory

import server as ddl_server

@pytest.mark.oracle
@pytest.mark.usefixtures("oracle_extract_env")
class TestOracleLiveIntegration:
    """Integration tests against DDL extracted from the live Oracle SH schema."""

    def _extract_sh(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps({"technology": "oracle", "dialect": "oracle"}),
            encoding="utf-8",
        )
        result = run_setup_ddl(
            ["extract", "--schemas", "SH", "--project-root", str(tmp_path)]
        )
        assert result.returncode == 0, f"setup-ddl extract failed: {result.stderr}"

    def test_get_table_schema_oracle_column_types(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        entry = catalog.get_table("SH.CUSTOMERS")
        assert entry is not None, "SH.CUSTOMERS not found in extracted DDL"

        cols = {
            c["name"].upper(): c
            for c in ddl_server._parse_columns(entry, dialect="oracle")
        }
        assert any(
            c["type"].startswith("VARCHAR2") for c in cols.values()
        ), "Expected VARCHAR2 columns in SH.CUSTOMERS"
        assert any(
            c["type"] == "NUMBER" or c["type"].startswith("NUMBER(")
            for c in cols.values()
        ), "Expected NUMBER columns in SH.CUSTOMERS"

    def test_list_procedures_returns_sh_procedures(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        sh_procs = [k for k in catalog.procedures if k.startswith("sh.")]
        assert len(sh_procs) > 0, "No SH procedures found in extracted DDL"

    def test_get_procedure_body_returns_oracle_ddl(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        sh_procs = [k for k in catalog.procedures if k.startswith("sh.")]
        assert sh_procs, "No SH procedures found"

        entry = catalog.procedures[sh_procs[0]]
        assert entry.raw_ddl.strip(), "Procedure body is empty"

    def test_list_views_returns_sh_views(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        sh_views = [k for k in catalog.views if k.startswith("sh.")]
        assert len(sh_views) > 0, "No SH views found in extracted DDL"

    def test_get_view_body_returns_oracle_ddl(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_sh(tmp_path)

        catalog = load_directory(tmp_path)
        sh_views = [k for k in catalog.views if k.startswith("sh.")]
        assert sh_views, "No SH views found - cannot test get_view_body"

        entry = catalog.views[sh_views[0]]
        assert entry.raw_ddl.strip(), "View body is empty"
        assert "CREATE OR REPLACE VIEW" in entry.raw_ddl, (
            f"Expected CREATE OR REPLACE VIEW in view DDL, got:\n{entry.raw_ddl[:300]}"
        )
