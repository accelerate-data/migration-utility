"""Live Oracle integration coverage for ddl_mcp/server.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip(
    "mcp.types",
    reason="mcp.types not available — skipping Oracle DDL MCP integration tests outside the mcp/ddl environment",
)

_DDL_MCP_DIR = Path(__file__).resolve().parents[4] / "mcp" / "ddl"
if str(_DDL_MCP_DIR) not in sys.path:
    sys.path.insert(0, str(_DDL_MCP_DIR))

import server as ddl_server
from shared.loader import load_directory
from tests.helpers import git_init, run_setup_ddl_cli
from tests.integration.runtime_helpers import ORACLE_MIGRATION_SCHEMA


@pytest.mark.oracle
@pytest.mark.usefixtures("oracle_extract_env")
class TestOracleLiveIntegration:
    """Integration tests against DDL extracted from the canonical Oracle MigrationTest schema."""

    def _extract_migrationtest(self, tmp_path: Path) -> None:
        (tmp_path / "manifest.json").write_text(
            json.dumps({"technology": "oracle", "dialect": "oracle"}),
            encoding="utf-8",
        )
        result = run_setup_ddl_cli(
            ["extract", "--schemas", ORACLE_MIGRATION_SCHEMA, "--project-root", str(tmp_path)],
            timeout=120,
        )
        assert result.returncode == 0, f"setup-ddl extract failed: {result.stderr}"

    def test_get_table_schema_oracle_column_types(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_migrationtest(tmp_path)

        catalog = load_directory(tmp_path)
        entry = catalog.get_table(f"{ORACLE_MIGRATION_SCHEMA}.CHANNELS")
        assert entry is not None, f"{ORACLE_MIGRATION_SCHEMA}.CHANNELS not found in extracted DDL"

        cols = {
            c["name"].upper(): c
            for c in ddl_server._parse_columns(entry, dialect="oracle")
        }
        assert any(
            c["type"].startswith("VARCHAR2") for c in cols.values()
        ), f"Expected VARCHAR2 columns in {ORACLE_MIGRATION_SCHEMA}.CHANNELS"
        assert any(
            c["type"] == "NUMBER" or c["type"].startswith("NUMBER(")
            for c in cols.values()
        ), f"Expected NUMBER columns in {ORACLE_MIGRATION_SCHEMA}.CHANNELS"

    def test_list_procedures_returns_migrationtest_procedures(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_migrationtest(tmp_path)

        catalog = load_directory(tmp_path)
        schema_procs = [
            k for k in catalog.procedures
            if k.startswith(f"{ORACLE_MIGRATION_SCHEMA.lower()}.")
        ]
        assert len(schema_procs) > 0, f"No {ORACLE_MIGRATION_SCHEMA} procedures found in extracted DDL"

    def test_get_procedure_body_returns_oracle_ddl(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_migrationtest(tmp_path)

        catalog = load_directory(tmp_path)
        schema_procs = [
            k for k in catalog.procedures
            if k.startswith(f"{ORACLE_MIGRATION_SCHEMA.lower()}.")
        ]
        assert schema_procs, f"No {ORACLE_MIGRATION_SCHEMA} procedures found"

        entry = catalog.procedures[schema_procs[0]]
        assert entry.raw_ddl.strip(), "Procedure body is empty"

    def test_list_views_returns_migrationtest_views(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_migrationtest(tmp_path)

        catalog = load_directory(tmp_path)
        schema_views = [
            k for k in catalog.views
            if k.startswith(f"{ORACLE_MIGRATION_SCHEMA.lower()}.")
        ]
        assert len(schema_views) > 0, f"No {ORACLE_MIGRATION_SCHEMA} views found in extracted DDL"

    def test_get_view_body_returns_oracle_ddl(self, tmp_path: Path, oracle_extract_env) -> None:
        git_init(tmp_path)
        self._extract_migrationtest(tmp_path)

        catalog = load_directory(tmp_path)
        schema_views = [
            k for k in catalog.views
            if k.startswith(f"{ORACLE_MIGRATION_SCHEMA.lower()}.")
        ]
        assert schema_views, f"No {ORACLE_MIGRATION_SCHEMA} views found - cannot test get_view_body"

        entry = catalog.views[schema_views[0]]
        assert entry.raw_ddl.strip(), "View body is empty"
        assert "CREATE OR REPLACE VIEW" in entry.raw_ddl, (
            f"Expected CREATE OR REPLACE VIEW in view DDL, got:\n{entry.raw_ddl[:300]}"
        )
